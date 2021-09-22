/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;

/**
 * Handles syncing of shadows on a specific cadence. With this strategy, the Shadow manager will only execute the
 * sync requests on a particular interval. It will cache all the sync requests until the interval has elapsed; after
 * which it will empty the sync queue by executing all the cached sync requests.
 */
public class PeriodicSyncStrategy extends BaseSyncStrategy implements SyncStrategy {

    public PeriodicSyncStrategy(Retryer retryer) {
        super(retryer);
    }

    /**
     * Starts syncing the shadows based on the strategy.
     *
     * @param context         an context object for syncing
     * @param syncParallelism number of threads to use for syncing
     */
    @Override
    public void start(SyncContext context, int syncParallelism) {

    }

    @Override
    void doStart(SyncContext context, int syncParallelism) {

    }

    @Override
    void doStop() {

    }

    /**
     * Stops the syncing of shadows.
     */
    @Override
    public void stop() {

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

    }

    /**
     * Clear all the sync requests in the request blocking queue.
     */
    @Override
    public void clearSyncQueue() {

    }

    /**
     * Get the remaining capacity in the request blocking sync queue.
     *
     * @return The capacity left in the sync queue.
     */
    @Override
    public int getRemainingCapacity() {
        return 0;
    }
}
