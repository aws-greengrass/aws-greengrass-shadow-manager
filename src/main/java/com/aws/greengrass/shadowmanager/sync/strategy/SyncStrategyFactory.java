/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.strategy.model.Strategy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Class to handle sync strategy clients.
 */
public class SyncStrategyFactory {
    /**
     * Interface for executing sync requests.
     */
    private final Retryer retryer;
    private final ExecutorService syncExecutorService;
    private final ScheduledExecutorService syncScheduledExecutorService;
    private final DirectionWrapper direction;

    /**
     * Constructor for SyncStrategyFactory to maintain sync strategy clients.
     *
     * @param retryer                  The retryer object.
     * @param executorService          The executor service object.
     * @param scheduledExecutorService The scheduled executor service object.
     * @param direction               The sync direction
     */
    public SyncStrategyFactory(Retryer retryer, ExecutorService executorService,
                               ScheduledExecutorService scheduledExecutorService, DirectionWrapper direction) {
        this.retryer = retryer;
        this.syncExecutorService = executorService;
        this.syncScheduledExecutorService = scheduledExecutorService;
        this.direction = direction;
    }

    /**
     * Gets the sync strategy based on the Strategy object provided.
     *
     * @param syncStrategy The sync strategy.
     * @param syncQueue    The sync queue from the previous strategy if any.
     * @return The sync strategy client to handle syncing of shadows.
     */
    @SuppressWarnings("PMD.MissingBreakInSwitch")
    public SyncStrategy createSyncStrategy(Strategy syncStrategy, RequestBlockingQueue syncQueue) {
        switch (syncStrategy.getType()) {
            case PERIODIC:
                return new PeriodicSyncStrategy(syncScheduledExecutorService, retryer, syncStrategy.getDelay(),
                        syncQueue, direction);
            case REALTIME:
            default:
                return new RealTimeSyncStrategy(syncExecutorService, retryer, syncQueue, direction);
        }
    }
}
