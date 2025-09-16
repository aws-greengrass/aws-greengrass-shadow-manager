/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.sync.RequestQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.RetryUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Handles syncing of shadows in real time. Whenever the device is connected, this strategy will try to execute the
 * shadow sync requests as quickly as possible.
 */
public class RealTimeSyncStrategy extends BaseSyncStrategy {

    private static final Logger logger = LogManager.getLogger(RealTimeSyncStrategy.class);
    private final ExecutorService syncExecutorService;
    /**
     * Track whether a sync thread has exited.
     */
    CountDownLatch syncThreadEnd;

    /**
     * Constructor.
     *
     * @param executorService executor service.
     * @param retryer         The retryer object.
     * @param syncQueue       The sync queue from the previous strategy if any.
     * @param direction       The sync direction
     */
    public RealTimeSyncStrategy(ExecutorService executorService, Retryer retryer, RequestQueue syncQueue,
                                DirectionWrapper direction) {
        super(retryer, syncQueue, direction);
        this.syncExecutorService = executorService;
        this.syncThreadEnd = new CountDownLatch(1);
    }

    /**
     * Constructor for testing.
     *
     * @param executorService executor service.
     * @param retryer         The retryer object.
     * @param retryConfig     The retryer configuration.
     * @param syncQueue       The sync queue from the previous strategy if any.
     * @param direction       The sync direction
     */
    public RealTimeSyncStrategy(ExecutorService executorService, Retryer retryer,
                                RetryUtils.RetryConfig retryConfig, RequestQueue syncQueue,
                                DirectionWrapper direction) {
        super(retryer, retryConfig, syncQueue, direction);
        this.syncExecutorService = executorService;
    }

    @Override
    void doStart(SyncContext context, int syncParallelism) {
        syncThreadEnd = new CountDownLatch(syncParallelism);
        logger.atInfo(SYNC_EVENT_TYPE).log("Start real time syncing");
        for (int i = 0; i < syncParallelism; i++) {
            syncThreads.add(syncExecutorService.submit(this::syncLoop));
        }
    }

    @Override
    protected void syncLoop() {
        try {
            super.syncLoop();
        } finally {
            syncThreadEnd.countDown();
        }
    }

    @Override
    protected void waitForSyncEnd() throws InterruptedException {
        // wait for threads to actually exit but don't block forever
        if (!syncThreadEnd.await(THREAD_END_WAIT_TIME_SECONDS, TimeUnit.SECONDS)) {
            logger.atWarn(SYNC_EVENT_TYPE).log("{} thread(s) did not exit after {} seconds",
                    syncThreadEnd.getCount(), THREAD_END_WAIT_TIME_SECONDS);
        }
    }

    @Override
    SyncRequest getRequest() throws InterruptedException {
        return syncQueue.take();
    }
}
