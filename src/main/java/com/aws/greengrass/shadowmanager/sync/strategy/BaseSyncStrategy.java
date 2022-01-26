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
import com.aws.greengrass.shadowmanager.sync.RequestMerger;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.RetryUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.LogEvents.SYNC;

public abstract class BaseSyncStrategy implements SyncStrategy {
    private static final Logger logger = LogManager.getLogger(BaseSyncStrategy.class);
    static final String SYNC_EVENT_TYPE = SYNC.code();

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
    @Setter
    RequestBlockingQueue syncQueue;

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

    /**
     * Constructor.
     *
     * @param retryer The retryer object.
     */
    public BaseSyncStrategy(Retryer retryer) {
        this.retryer = retryer;
        this.retryConfig = DEFAULT_RETRY_CONFIG;
        RequestMerger requestMerger = new RequestMerger();
        this.syncQueue = new RequestBlockingQueue(requestMerger);
    }

    /**
     * Constructor for testing.
     *
     * @param retryer     The retryer object.
     * @param retryConfig The config to be used by the retryer.
     */
    public BaseSyncStrategy(Retryer retryer, RetryUtils.RetryConfig retryConfig) {
        this.retryer = (config, request, context) -> {
            try {
                executing.set(true);
                retryer.run(config, request, context);
            } finally {
                executing.set(false);
            }
        };
        this.retryConfig = retryConfig;
        RequestMerger requestMerger = new RequestMerger();
        this.syncQueue = new RequestBlockingQueue(requestMerger);
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
                syncing.set(false);

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

    abstract void doStop();

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
}
