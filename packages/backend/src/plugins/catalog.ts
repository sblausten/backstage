/*
 * Copyright 2020 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  CatalogBuilder,
  ConflictHandlerOptions,
} from '@backstage/plugin-catalog-backend';
import { EntityProvider } from '@backstage/plugin-catalog-node';
import { ScaffolderEntitiesProcessor } from '@backstage/plugin-scaffolder-backend';
import { Router } from 'express';
import { PluginEnvironment } from '../types';
import { GithubEntityProvider } from '@backstage/plugin-catalog-backend-module-github';
import { Entity, stringifyEntityRef } from '@backstage/catalog-model';
import { Duration } from 'luxon';
import { CustomErrorBase } from '@backstage/errors';

export default async function createPlugin(
  env: PluginEnvironment,
  providers?: Array<EntityProvider>,
): Promise<Router> {
  const builder = await CatalogBuilder.create(env);
  const githubEntityProviders = GithubEntityProvider.fromConfig(env.config, {
    logger: env.logger,
    schedule: env.scheduler.createScheduledTaskRunner({
      frequency: { minutes: 5 },
      timeout: { minutes: 3 },
    }),
    scheduler: env.scheduler,
  });

  const conflictHandler = async (options: ConflictHandlerOptions) => {
    async function overwriteEntityStrategy(params: {
      entity: Entity;
      hash: string;
      originalLocationKey: string;
      newLocationKey: string;
    }): Promise<void> {
      options.logger.info('Handling conflict with overwrite strategy');
      const { entity, hash, originalLocationKey, newLocationKey } = params;
      const db = await env.database.getClient();

      const entityRef = stringifyEntityRef(entity);
      const serializedEntity = JSON.stringify(entity);
      const now = db.fn.now();

      await db('refresh_state')
        .update({
          unprocessed_entity: serializedEntity,
          unprocessed_hash: hash,
          location_key: newLocationKey,
          last_discovery_at: now,
          next_update_at: now,
        })
        .where('entity_ref', entityRef)
        .where('location_key', originalLocationKey);
    }

    async function scheduleConflictRetryStrategy() {
      class ConflictRetryError extends CustomErrorBase {}

      options.logger.info('Handling conflict with retry strategy');
      if (options.processingDatabase) {
        const terminationhandler = ({
          originalLocationKey,
          newLocationKey,
        }: ConflictHandlerOptions) => {
          throw new ConflictRetryError(
            `Entity at ${newLocationKey} still conflicting with ${originalLocationKey}. Retrying.`,
          );
        };
        const cleanUp = async (id: string) => {
          try {
            options.logger.info(`Removing scheduled retry for task ${id}`);
            await env.scheduler.unscheduleTask(id);
          } catch (e) {
            if (e.name === 'ConflictError') {
              setTimeout(() => cleanUp(id), 10000);
            } else {
              options.logger.error(e.message);
            }
          }
        };

        const task =
          (abortController: AbortController, taskId: string) => async () => {
            if (abortController.signal.aborted) {
              options.logger.info(
                `Aborted scheduled retry for conflict at ${options.newLocationKey}`,
              );
              cleanUp(taskId);
              return;
            }
            options.logger.info(
              `Retrying import of conflicting entity ${stringifyEntityRef(
                options.entity,
              )} at ${options.newLocationKey}`,
            );
            await options.processingDatabase!.transaction(
              async (tx: { rollback(): Promise<unknown> }) => {
                try {
                  await options.processingDatabase!.addUnprocessedEntities(tx, {
                    entities: [
                      {
                        entity: options.entity,
                        locationKey: options.newLocationKey,
                      },
                    ],
                    sourceEntityRef: stringifyEntityRef(options.entity),
                    conflictHandler: terminationhandler,
                  });
                  options.logger.info(
                    `Successfully imported previously conflicting entity ${stringifyEntityRef(
                      options.entity,
                    )} at ${options.newLocationKey}`,
                  );
                  cleanUp(taskId);
                } catch (e) {
                  if (e.name === 'ConflictRetryError') {
                    options.logger.info(e.message);
                  } else {
                    options.logger.error(e.message);
                  }
                }
              },
            );
          };

        options.logger.info(
          `Scheduling retry for conflicting entity ${stringifyEntityRef(
            options.entity,
          )} at ${options.newLocationKey}`,
        );
        const controller = new AbortController();
        // setTimeout(() => controller.abort(), 3600000); // 60 minutes
        setTimeout(() => controller.abort(), 600000); // 10 minutes

        options.logger.info(
          `Scheduling task conflictRetry-${options.newLocationKey}`,
        );
        const taskId = `conflictRetry-${options.newLocationKey}`;
        return await env.scheduler.scheduleTask({
          id: taskId,
          timeout: Duration.fromMillis(5000),
          frequency: Duration.fromObject({ minutes: 2 }),
          fn: task(controller, taskId),
          scope: 'global',
        });
      }
      return Promise.resolve();
    }

    // return await scheduleConflictRetryStrategy();

    // return await overwriteEntity({
    //   entity: options.entity,
    //   hash: options.hash,
    //   newLocationKey: options.newLocationKey,
    //   originalLocationKey: options.originalLocationKey,
    // });
  };

  builder.addEntityProvider(githubEntityProviders);
  builder.addProcessor(new ScaffolderEntitiesProcessor());
  builder.addEntityProvider(providers ?? []);
  const { processingEngine, router } = await builder
    .setConflictHandler(conflictHandler)
    .build();
  await processingEngine.start();
  return router;
}
