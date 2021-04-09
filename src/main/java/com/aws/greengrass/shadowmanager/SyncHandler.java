/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.shadowmanager.model.SyncRequest;

import javax.inject.Inject;

/**
 * Class which handles syncing shadows to IoT Shadow Service.
 */
public class SyncHandler  {

    /**
     * Ctr for SyncHandler.
     */
    @Inject
    SyncHandler() {
    }

    /**
     * Performs a full sync on the cloud shadows.
     *
     */
    public void fullSync() {

    }

    /**
     * Starts Sync thread to start processing sync requests. Will run a full sync.
     *
     */
    public void startSync() {

    }

    /**
     * Stops Sync thread. All current messages are removed and a full sync will be necessary.
     *
     */
    public void stopSync() {

    }

    /**
     * Adds sync request to the request queue.
     * TODO: Determine message queue data structure
     *
     */
    public void takeSyncRequest(SyncRequest syncRequest) {
    }

}
