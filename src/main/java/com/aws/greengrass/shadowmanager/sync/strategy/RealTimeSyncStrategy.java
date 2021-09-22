/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
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
public class RealTimeSyncStrategy extends BaseSyncStrategy implements SyncStrategy {

    private static final Logger logger = LogManager.getLogger(RealTimeSyncStrategy.class);
    private final ExecutorService syncExecutorService;

    /**
     * Constructor.
     *
     * @param executorService executor service.
     * @param retryer         The retryer object.
     */
    public RealTimeSyncStrategy(ExecutorService executorService, Retryer retryer) {
        super(retryer);
        this.syncExecutorService = executorService;
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

    /**
     * Starts syncing the shadows based on the strategy.
     *
     * @param context         an context object for syncing
     * @param syncParallelism number of threads to use for syncing
     */
    @Override
    public void start(SyncContext context, int syncParallelism) {
        super.startSync(context, syncParallelism);
    }

    @Override
    void doStart(SyncContext context, int syncParallelism) {
        logger.atInfo(SYNC_EVENT_TYPE).log("Start real time syncing");
        for (int i = 0; i < syncParallelism; i++) {
            syncThreads.add(syncExecutorService.submit(this::syncLoop));
        }

    }

    /**
     * Stops the syncing of shadows.
     */
    @Override
    public void stop() {
        super.stopSync();
    }

    @Override
    void doStop() {
        logger.atDebug(SYNC_EVENT_TYPE).log("Cancel {} real time sync thread(s)", syncThreads.size());
        syncThreads.forEach(t -> t.cancel(true));
        syncThreads.clear();
    }

    /**
     * Put a sync request into the queue if syncing is started.
     * <p/>
     * This will block if the queue is full. This is intentional as non-blocking requires either an unbounded queue
     * of requests, or an Executor service which creates threads from an unbounded queue.
     * <p/>
     * We cannot support an unbounded queue as that will lead to memory pressure - instead requests need to be
     * throttled.
     * <p/>
     * Synchronized so that there is at most only one put in progress waiting to be added if queue is full
     *
     * @param request request the request to add.
     */
    @Override
    public void putSyncRequest(SyncRequest request) {
        if (!syncing.get()) {
            logger.atTrace(SYNC_EVENT_TYPE)
                    .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                    .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log("Syncing is stopped. Ignoring sync request");
            return;
        }
        try {
            syncQueue.put(request);

            // the above put call will block. If syncing is stopped while waiting but after the put call succeeds then
            // remove the request we just added
            if (!syncing.get()) {
                syncQueue.remove(request);
            }
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                    .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log("Interrupted while putting sync request into queue");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Clear all the sync requests in the request blocking queue.
     */
    @Override
    public void clearSyncQueue() {
        logger.atTrace(SYNC_EVENT_TYPE).log("Clear all sync requests");
        syncQueue.clear();
    }

    /**
     * Get the remaining capacity in the request blocking sync queue.
     *
     * @return The capacity left in the sync queue.
     */
    @Override
    public int getRemainingCapacity() {
        return syncQueue.remainingCapacity();
    }

    /**
     * Take and execute items from the sync queue. This is intended to be run in a separate thread.
     */
    @SuppressWarnings({"PMD.CompareObjectsWithEquals", "PMD.AvoidCatchingGenericException"})
    private void syncLoop() {
        logger.atInfo(SYNC_EVENT_TYPE).log("Start waiting for sync requests");
        try {
            SyncRequest request = syncQueue.take();
            RetryUtils.RetryConfig retryConfig = this.retryConfig;
            do {
                try {
                    logger.atInfo(SYNC_EVENT_TYPE)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .addKeyValue("Type", request.getClass().getSimpleName())
                            .log("Executing sync request");

                    retryer.run(retryConfig, request, context);
                    retryConfig = this.retryConfig; // reset the retry config back to default after success

                    logger.atDebug(SYNC_EVENT_TYPE).log("Waiting for next sync request");
                    request = syncQueue.take();
                } catch (RetryableException e) {
                    // this will be rethrown if all retries fail in RetryUtils
                    logger.atDebug(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
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
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .log("Received conflict when processing request. Retrying as a full sync");
                    // add back to queue to merge over any shadow request that came in while it was executing
                    request = syncQueue.offerAndTake(new FullShadowSyncRequest(request.getThingName(),
                            request.getShadowName()), true);
                } catch (UnknownShadowException e) {
                    logger.atWarn(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .log("Received unknown shadow when processing request. Retrying as a full sync");
                    // add back to queue to merge over any shadow request that came in while it was executing
                    request = syncQueue.offerAndTake(new FullShadowSyncRequest(request.getThingName(),
                            request.getShadowName()), true);
                } catch (Exception e) {
                    logger.atError(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .log("Skipping sync request");
                    request = syncQueue.take();
                }
            } while (!Thread.currentThread().isInterrupted());
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE).log("Interrupted while waiting for sync requests");
        }
        logger.atInfo(SYNC_EVENT_TYPE).log("Stop waiting for sync requests");
    }
}
