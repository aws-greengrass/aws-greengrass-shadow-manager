/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.RequestMerger;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
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

import static com.aws.greengrass.shadowmanager.model.LogEvents.SYNC;

public abstract class BaseSyncStrategy {
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
     * @implNote The Setter is only used in unit tests. The Getter is used in integration tests.
     */
    @Getter
    @Setter(AccessLevel.PACKAGE)
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
     * Indicates whether syncing is running or not.
     */
    AtomicBoolean syncing = new AtomicBoolean(false);

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
        this.retryer = retryer;
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
    void startSync(SyncContext context, int syncParallelism) {
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
    void stopSync() {
        synchronized (lifecycleLock) {
            if (syncing.compareAndSet(true, false)) {
                logger.atInfo(SYNC_EVENT_TYPE).log("Stop real time syncing");
                syncing.set(false);

                doStop();

                int remaining = syncQueue.size();
                syncQueue.clear();

                if (remaining > 0) {
                    logger.atInfo(SYNC_EVENT_TYPE)
                            .log("Stopped real time syncing with {} pending sync items", remaining);
                }
            } else {
                logger.atDebug(SYNC_EVENT_TYPE)
                        .log("Real time Syncing is already stopped. Ignoring request to stop");
            }
        }
    }


    abstract void doStart(SyncContext context, int syncParallelism);

    abstract void doStop();
}
