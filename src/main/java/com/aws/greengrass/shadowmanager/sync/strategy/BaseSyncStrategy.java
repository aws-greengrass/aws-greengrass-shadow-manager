/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.RetryUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.LogEvents.SYNC;

public abstract class BaseSyncStrategy implements SyncStrategy {
    private static final Logger logger = LogManager.getLogger(BaseSyncStrategy.class);
    static final String SYNC_EVENT_TYPE = SYNC.code();
    static final int THREAD_END_WAIT_TIME_SECONDS = 10;

    /**
     * Lock used to synchronize start and stop of the sync strategy.
     */
    final Object lifecycleLock = new Object();

    /**
     * The threads running the sync loop.
     */
    final List<Future<?>> syncThreads = new ArrayList<>();

    /**
     * The request blocking queue holding all the sync requests.
     *
     * @implNote The Setter is only used in unit tests. The Getter is used in integration tests.
     */
    @Getter
    final RequestBlockingQueue syncQueue;

    /**
     * Interface for executing sync requests.
     */
    @Getter
    final Retryer retryer;

    /**
     * Context object containing handlers useful for sync requests.
     */
    @Setter(AccessLevel.PACKAGE)
    SyncContext context;

    /**
     * Configuration for retrying a sync request.
     */
    final RetryUtils.RetryConfig retryConfig;

    /**
     * Configuration for retrying sync requests.
     */
    static final RetryUtils.RetryConfig DEFAULT_RETRY_CONFIG =
            RetryUtils.RetryConfig.builder()
                    .maxAttempt(5)
                    .initialRetryInterval(Duration.of(3, ChronoUnit.SECONDS))
                    .maxRetryInterval(Duration.of(1, ChronoUnit.MINUTES))
                    .retryableExceptions(Collections.singletonList(RetryableException.class)).build();

    /**
     * Configuration for retrying a sync request immediately after failing with the {@link #DEFAULT_RETRY_CONFIG}.
     */
    static final RetryUtils.RetryConfig FAILED_RETRY_CONFIG =
            RetryUtils.RetryConfig.builder()
                    .maxAttempt(3)
                    .initialRetryInterval(Duration.of(30, ChronoUnit.SECONDS))
                    .maxRetryInterval(Duration.of(2, ChronoUnit.MINUTES))
                    .retryableExceptions(Collections.singletonList(RetryableException.class))
                    .build();

    /**
     * Indicates whether syncing is running or not.
     */
    AtomicBoolean syncing = new AtomicBoolean(false);

    /**
     * Indicates whether a sync request is executing or not.
     */
    AtomicBoolean executing = new AtomicBoolean(false);

    /**
     * Acquired during execution of sync requests and when stopping syncing.
     */
    Semaphore criticalExecBlock;

    int syncParallelism;


    /**
     * Getter for syncing boolean.
     *
     * @return true if Shadow Manager is syncing.
     */
    public boolean isSyncing() {
        return syncing.get();
    }

    /**
     * Getter for executing boolean.
     *
     * @return true if Shadow Manager is executing.
     */
    public boolean isExecuting() {
        return executing.get();
    }

    protected BaseSyncStrategy(Retryer retryer, RequestBlockingQueue syncQueue) {
        this(retryer, DEFAULT_RETRY_CONFIG, syncQueue);
    }

    /**
     * Constructor for testing.
     *
     * @param retryer     The retryer object.
     * @param retryConfig The config to be used by the retryer.
     * @param syncQueue   A queue to use for sync requests.
     */
    protected BaseSyncStrategy(Retryer retryer, RetryUtils.RetryConfig retryConfig, RequestBlockingQueue syncQueue) {
        this.retryer = (config, request, context) -> {
            try {
                executing.set(true);
                retryer.run(config, request, context);
            } finally {
                executing.set(false);
            }
        };
        this.retryConfig = retryConfig;
        this.syncQueue = syncQueue;

        // initialize some defaults
        this.syncParallelism = 1;
        this.criticalExecBlock = new Semaphore(1);
    }

    /**
     * Starts syncing the shadows based on the strategy.
     *
     * @param context         an context object for syncing
     * @param syncParallelism number of threads to use for syncing
     */
    @Override
    public void start(SyncContext context, int syncParallelism) {
        synchronized (lifecycleLock) {
            this.context = context;
            if (syncing.compareAndSet(false, true)) {
                criticalExecBlock = new Semaphore(syncParallelism);
                this.syncParallelism = syncParallelism;
                doStart(context, syncParallelism);
            } else {
                logger.atDebug(SYNC_EVENT_TYPE).log("Already started syncing");
            }
        }
    }

    /**
     * Stops the syncing of shadows.
     */
    @Override
    public void stop() {
        synchronized (lifecycleLock) {
            if (syncing.compareAndSet(true, false)) {
                logger.atInfo(SYNC_EVENT_TYPE).log("Stop syncing");

                doStop();

                int remaining = syncQueue.size();

                // Not clearing the queue since we need it if the customer updates the sync strategy on the fly. The
                // queue will be transferred to the new sync strategy.
                if (remaining > 0) {
                    logger.atInfo(SYNC_EVENT_TYPE)
                            .log("Stopped syncing with {} pending sync items", remaining);
                }
            } else {
                logger.atDebug(SYNC_EVENT_TYPE)
                        .log("Syncing is already stopped. Ignoring request to stop");
            }
        }
    }

    abstract void doStart(SyncContext context, int syncParallelism);

    protected void doStop() {
        logger.atInfo(SYNC_EVENT_TYPE).log("Cancel {} sync thread(s)", syncThreads.size());
        try {
            // The syncing flag is set to false by this point so once current requests have finished threads will not
            // pick up another

            // We need to cancel threads, but we should wait for any inflight requests to finish
            // Cancelling a thread while the db is in the middle of an operation will cause errors in the connection.
            // FullSyncRequests can generate a local and cloud request and we want both of those to get executed before
            // cancelling the sync threads.

            // There is a semaphore around the critical section in the sync loop - we do not want to cancel while
            // that is held.
            // There is a latch that is decremented when the syncLoop exits - this allows us to know when stopped or
            // cancelled threads have actually exited. Cancelling a future will not let you know when the thread has
            // actually finished.
            if (!criticalExecBlock.tryAcquire(syncParallelism)) {
                logger.atInfo(SYNC_EVENT_TYPE).log("Waiting for in-flight sync requests to finish");
                criticalExecBlock.acquireUninterruptibly(syncParallelism);
                logger.atInfo(SYNC_EVENT_TYPE).log("Finished waiting for sync requests to finish");
            }
            syncThreads.forEach(t -> t.cancel(true));
            try {
                waitForSyncEnd();
            } catch (InterruptedException e) {
                logger.atDebug(SYNC_EVENT_TYPE).log("Interrupted while waiting for threads to finish");
            }
            syncThreads.clear();
        } finally {
            criticalExecBlock.release(syncParallelism);
        }
    }

    /**
     * Wait (briefly) for sync threads to finish to ensure we don't start any more while others are shutting down.
     */
    protected abstract void waitForSyncEnd() throws InterruptedException;

    /**
     * Get the request from the queue.
     * @return a request.
     */
    abstract SyncRequest getRequest() throws InterruptedException;

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
                    .addKeyValue("type", request.getClass())
                    .log("Syncing is stopped. Ignoring sync request");
            return;
        }

        try {
            if (!request.isUpdateNecessary(context)) {
                logger.atDebug()
                        .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                        .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                        .addKeyValue("type", request.getClass())
                        .log("Ignoring sync request since update is not necessary");
                return;
            }
        } catch (SkipSyncRequestException | RetryableException | UnknownShadowException e) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                    .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log("Ignoring sync request since update is not necessary");
            return;
        }

        try {
            logger.atDebug(SYNC_EVENT_TYPE)
                    .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                    .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .addKeyValue("type", request.getClass())
                    .log("Adding new sync request");

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
     * This will work on all the sync requests in the queue until it's empty.
     */
    @SuppressWarnings({"PMD.CompareObjectsWithEquals", "PMD.AvoidCatchingGenericException", "PMD.NullAssignment"})
    protected void syncLoop() {
        SyncRequest request = null;
        try {
            logger.atInfo(SYNC_EVENT_TYPE).log("Start processing sync requests");
            RetryUtils.RetryConfig retryConfig = this.retryConfig;
            request = getRequest();
            String currProcessingThingName = null;
            String currProcessingShadowName = null;

            // Process the sync requests until there is a sync request in the queue and until the thread has not
            // been interrupted.
            while (request != null && !Thread.currentThread().isInterrupted() && syncing.get()) {
                try {
                    currProcessingThingName = request.getThingName();
                    currProcessingShadowName = request.getShadowName();

                    // acquire a permit so that if syncing is stopped while executing, we get to finish the request
                    criticalExecBlock.acquire();
                    try {
                        // if we are currently stopped - don't run the current request
                        if (!syncing.get()) {
                            logger.atTrace(SYNC_EVENT_TYPE)
                                    .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                                    .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                                    .addKeyValue("Type", request.getClass().getSimpleName())
                                    .log("Syncing stopped after acquiring permit, exit");
                            break;
                        }
                        logger.atInfo(SYNC_EVENT_TYPE)
                                .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                                .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                                .addKeyValue("Type", request.getClass().getSimpleName())
                                .log("Executing sync request");
                        retryer.run(retryConfig, request, context);
                    } finally {
                        criticalExecBlock.release();
                    }
                    request = null; // Reset the request here since we have already processed it successfully.

                    retryConfig = this.retryConfig; // reset the retry config back to default after success
                    request = getRequest();
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
                    logger.atWarn(SYNC_EVENT_TYPE).cause(e).addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .log("Received unknown shadow when processing request. Retrying as a full sync");
                    // add back to queue to merge over any shadow request that came in while it was executing
                    request = syncQueue.offerAndTake(
                            new FullShadowSyncRequest(currProcessingThingName, currProcessingShadowName), true);
                } catch (Exception e) {
                    logger.atError(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, currProcessingThingName)
                            .addKeyValue(LOG_SHADOW_NAME_KEY, currProcessingShadowName)
                            .log("Skipping sync request");
                    request = getRequest();
                }
            }

        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE).log("Interrupted while waiting for sync requests");
        } finally {
            // Add the sync request back in the queue if it was not processed.
            if (request != null) {
                logger.atTrace(SYNC_EVENT_TYPE)
                        .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                        .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                        .addKeyValue("Type", request.getClass().getSimpleName())
                        .log("Adding request item back to queue");
                syncQueue.offer(request);
            }
            logger.atInfo(SYNC_EVENT_TYPE).log("Finished processing sync requests");
        }
    }
}
