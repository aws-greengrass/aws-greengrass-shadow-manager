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

import java.util.concurrent.ExecutorService;

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
        super(retryer, syncQueue);
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

    @Override
    void doStart(SyncContext context, int syncParallelism) {
        logger.atInfo(SYNC_EVENT_TYPE).log("Start real time syncing");
        for (int i = 0; i < syncParallelism; i++) {
            syncThreads.add(syncExecutorService.submit(this::syncLoop));
        }

    }

    @Override
    SyncRequest getRequest() throws InterruptedException {
        return syncQueue.take();
    }
}
