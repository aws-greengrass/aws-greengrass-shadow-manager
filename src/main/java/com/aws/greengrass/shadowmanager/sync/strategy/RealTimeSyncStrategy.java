/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.RetryUtils;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;

import java.util.concurrent.ExecutorService;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

/**
 * Handles syncing of shadows in real time. Whenever the device is connected, this strategy will try to execute the
 * shadow sync requests as quickly as possible.
 */
public class RealTimeSyncStrategy extends BaseSyncStrategy {

    private static final Logger logger = LogManager.getLogger(RealTimeSyncStrategy.class);
    private final ExecutorService syncExecutorService;

    /**
     * Constructor.
     *
     * @param executorService executor service.
     * @param retryer         The retryer object.
     * @param syncQueue       The sync queue from the previous strategy if any.
     */
    public RealTimeSyncStrategy(ExecutorService executorService, Retryer retryer, RequestBlockingQueue syncQueue) {
        super(retryer);
        this.syncExecutorService = executorService;
        if (syncQueue != null) {
            this.syncQueue = syncQueue;
        }
    }

    /**
     * Constructor for testing.
     *
     * @param executorService executor service.
     * @param retryer         The retryer object.
     * @param retryConfig     The retryer configuration.
     */
    public RealTimeSyncStrategy(ExecutorService executorService, Retryer retryer,
                                RetryUtils.RetryConfig retryConfig) {
        super(retryer, retryConfig);
        this.syncExecutorService = executorService;
    }

    @Override
    void doStart(SyncContext context, int syncParallelism) {
        logger.atInfo(SYNC_EVENT_TYPE).log("Start real time syncing");
        for (int i = 0; i < syncParallelism; i++) {
            syncThreads.add(syncExecutorService.submit(this::syncLoop));
        }

    }

    @Override
    void doStop() {
        logger.atInfo(SYNC_EVENT_TYPE).log("Cancel {} real time sync thread(s)", syncThreads.size());
        syncThreads.forEach(t -> t.cancel(true));
        syncThreads.clear();
    }


    /**
     * Take and execute items from the sync queue. This is intended to be run in a separate thread.
     */
    @SuppressWarnings({"PMD.CompareObjectsWithEquals", "PMD.AvoidCatchingGenericException", "PMD.NullAssignment"})
    private void syncLoop() {
        logger.atInfo(SYNC_EVENT_TYPE).log("Start waiting for sync requests");
        try {
            SyncRequest request = syncQueue.take();
            RetryUtils.RetryConfig retryConfig = this.retryConfig;
            String currProcessingThingName = null;
            String currProcessingShadowName = null;
            do {
                try {
                    currProcessingThingName = request.getThingName();
                    currProcessingShadowName = request.getShadowName();
                    logger.atInfo(SYNC_EVENT_TYPE)
                            .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .addKeyValue("Type", request.getClass().getSimpleName())
                            .log("Executing sync request");

                    retryer.run(retryConfig, request, context);
                    request = null;
                    retryConfig = this.retryConfig; // reset the retry config back to default after success

                    logger.atDebug(SYNC_EVENT_TYPE).log("Waiting for next sync request");
                    request = syncQueue.take();
                } catch (RetryableException e) {
                    // this will be rethrown if all retries fail in RetryUtils
                    logger.atDebug(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .log("Retry sync request. Adding back to queue");

                    // put request to back of queue and get the front of queue in a single operation
                    SyncRequest failedRequest = request;

                    // tell queue this is not a new value so it merges correctly with any update that came in
                    request = syncQueue.offerAndTake(request, false);

                    // if queue was empty, we are going to immediately retrying the same request. For this case don't
                    // use the default retry configuration - keep from spamming too quickly
                    if (request == failedRequest) {
                        retryConfig = FAILED_RETRY_CONFIG;
                    }
                } catch (InterruptedException e) {
                    logger.atWarn(SYNC_EVENT_TYPE).log("Interrupted while waiting for sync requests");
                    Thread.currentThread().interrupt();
                } catch (ConflictException | ConflictError e) {
                    logger.atWarn(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .log("Received conflict when processing request. Retrying as a full sync");
                    // add back to queue to merge over any shadow request that came in while it was executing
                    request = syncQueue.offerAndTake(new FullShadowSyncRequest(currProcessingThingName,
                            currProcessingShadowName), true);
                } catch (UnknownShadowException e) {
                    logger.atWarn(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .log("Received unknown shadow when processing request. Retrying as a full sync");
                    // add back to queue to merge over any shadow request that came in while it was executing
                    request = syncQueue.offerAndTake(new FullShadowSyncRequest(request.getThingName(),
                            request.getShadowName()), true);
                } catch (Exception e) {
                    logger.atError(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .log("Skipping sync request");
                    request = syncQueue.take();
                    currProcessingThingName = request.getThingName();
                    currProcessingShadowName = request.getShadowName();
                }
            } while (!Thread.currentThread().isInterrupted());

            // Add the sync request back in the queue if it was not processed.
            if (request != null) {
                syncQueue.offer(request);
            }
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE).log("Interrupted while waiting for sync requests");
        }
        logger.atInfo(SYNC_EVENT_TYPE).log("Stop waiting for sync requests");
    }
}
