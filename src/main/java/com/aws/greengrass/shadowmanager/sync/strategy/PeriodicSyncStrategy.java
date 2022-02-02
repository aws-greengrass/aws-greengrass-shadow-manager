/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.RetryUtils;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Handles syncing of shadows on a specific cadence. With this strategy, the Shadow manager will only execute the
 * sync requests on a particular interval. It will cache all the sync requests until the interval has elapsed; after
 * which it will empty the sync queue by executing all the cached sync requests.
 */
public class PeriodicSyncStrategy extends BaseSyncStrategy {
    private static final Logger logger = LogManager.getLogger(PeriodicSyncStrategy.class);
    private final ScheduledExecutorService syncExecutorService;
    private final long interval;
    final ReentrantLock syncThreadLock = new ReentrantLock(); // lock for ensuring thread ends before stopping

    /**
     * Constructor.
     *
     * @param ses       The scheduled executor service object.
     * @param retryer   The retryer object.
     * @param interval  The interval at which to sync the shadows.
     * @param syncQueue The sync queue from the previous strategy if any.
     */
    public PeriodicSyncStrategy(ScheduledExecutorService ses, Retryer retryer, long interval,
                                RequestBlockingQueue syncQueue) {
        super(retryer, syncQueue);
        this.syncExecutorService = ses;
        this.interval = interval;
    }

    /**
     * Constructor for testing.
     *
     * @param ses         executor service.
     * @param retryer     The retryer object.
     * @param interval    The interval at which to sync the shadows.
     * @param retryConfig The retryer configuration.
     */
    public PeriodicSyncStrategy(ScheduledExecutorService ses, Retryer retryer, long interval,
                                RetryUtils.RetryConfig retryConfig) {
        super(retryer, retryConfig);
        this.syncExecutorService = ses;
        this.interval = interval;
    }

    @Override
    void doStart(SyncContext context, int syncParallelism) {
        logger.atInfo(SYNC_EVENT_TYPE).kv("interval", interval).log("Start periodic syncing");
        this.syncParallelism = 1; // ignore sync parallelism as there is only 1 thread running
        this.criticalExecBlock = new Semaphore(1);
        this.syncThreads.add(syncExecutorService
                .scheduleAtFixedRate(this::syncLoop, 0, interval, TimeUnit.SECONDS));
    }

    @Override
    protected void syncLoop() {
        syncThreadLock.lock();
        try {
            super.syncLoop();
        } finally {
            syncThreadLock.unlock();
        }
    }

    @Override
    protected void waitForSyncEnd() throws InterruptedException {
        if (syncThreadLock.tryLock(THREAD_END_WAIT_TIME_SECONDS, TimeUnit.SECONDS)) {
            syncThreadLock.unlock(); // release lock just acquired
        } else {
            logger.atWarn(SYNC_EVENT_TYPE).log("1 thread did not exit after {} seconds",
                    THREAD_END_WAIT_TIME_SECONDS);
        }
    }

    @Override
    SyncRequest getRequest() {
        return syncQueue.poll();
    }
}
