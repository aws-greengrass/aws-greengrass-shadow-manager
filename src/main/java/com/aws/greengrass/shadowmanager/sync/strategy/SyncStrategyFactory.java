/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;

import java.util.concurrent.ExecutorService;

public class SyncStrategyFactory {
    /**
     * Interface for executing sync requests.
     */
    private final Retryer retryer;
    private final ExecutorService syncExecutorService;

    public SyncStrategyFactory(Retryer retryer, ExecutorService syncExecutorService) {
        this.retryer = retryer;
        this.syncExecutorService = syncExecutorService;
    }

    /**
     * Gets the sync strategy based on the Strategy object provided.
     *
     * @param syncStrategy The sync strategy.
     */
    @SuppressWarnings("PMD.MissingBreakInSwitch")
    public SyncStrategy getSyncStrategy(Strategy syncStrategy) {
        switch (syncStrategy.getType()) {
            case PERIODIC:
                return new PeriodicSyncStrategy(retryer);
            case REALTIME:
            default:
                return new RealTimeSyncStrategy(syncExecutorService, retryer);
        }
    }
}
