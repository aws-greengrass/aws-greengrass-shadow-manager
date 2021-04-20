/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;

import javax.inject.Inject;

/**
 * Class which handles syncing shadows to IoT Shadow Service.
 * TODO: Remove PMD suppressed warning once sync requests implemented
 */
@SuppressWarnings("PMD")
public class SyncHandler {

    private final ShadowManagerDAO dao;
    private final UpdateThingShadowIPCHandler updateThingShadowIPCHandler;
    private final DeleteThingShadowIPCHandler deleteThingShadowIPCHandler;


    /**
     * Ctr for SyncHandler.
     *
     * @param dao                         Local shadow database management
     * @param updateThingShadowIPCHandler Reference to the UpdateThingShadow IPC Handler
     * @param deleteThingShadowIPCHandler Reference to the DeleteThingShadow IPC Handler
     */
    @Inject
    SyncHandler(ShadowManagerDAO dao,
                UpdateThingShadowIPCHandler updateThingShadowIPCHandler,
                DeleteThingShadowIPCHandler deleteThingShadowIPCHandler) {
        this.dao = dao;
        this.updateThingShadowIPCHandler = updateThingShadowIPCHandler;
        this.deleteThingShadowIPCHandler = deleteThingShadowIPCHandler;
    }

    /**
     * Performs a full sync on all shadows. Clears any existing sync requests and will create full shadow sync requests
     * for all shadows.
     */
    public void fullSyncOnAllShadows() {

    }

    /**
     * Performs a full sync on a specific shadow.
     * TODO: implement message queue data structure to push SyncRequest
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void fullSyncOnShadow(String thingName, String shadowName) {
        FullShadowSyncRequest fullShadowSyncRequest = new FullShadowSyncRequest(thingName,
                shadowName,
                this.dao,
                this.updateThingShadowIPCHandler,
                this.deleteThingShadowIPCHandler);
    }

    /**
     * Starts Sync thread to start processing sync requests. Will run a full sync.
     */
    public void start() {

    }

    /**
     * Stops Sync thread. All current messages are removed and a full sync will be necessary.
     */
    public void stop() {

    }

    /**
     * Pushes an update sync request to the request queue to update shadow in the cloud after a local shadow has
     * been successfully updated.
     * TODO: implement message queue data structure to push SyncRequest
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void pushCloudUpdateSyncRequest(String thingName, String shadowName) {
        CloudUpdateSyncRequest cloudUpdateSyncRequest = new CloudUpdateSyncRequest(thingName, shadowName, this.dao);
    }

    /**
     * Pushes an update sync request to the request queue to update local shadow after a cloud shadow has
     * been successfully updated.
     * TODO: implement message queue data structure to push SyncRequest
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void pushLocalUpdateSyncRequest(String thingName, String shadowName) {
        LocalUpdateSyncRequest localUpdateSyncRequest = new LocalUpdateSyncRequest(thingName, shadowName, this.dao,
                this.updateThingShadowIPCHandler);
    }

    /**
     * Pushes a delete sync request in the request queue to delete a shadow in the cloud after a local shadow has
     * been successfully deleted.
     * TODO: implement message queue data structure to push SyncRequest
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void pushCloudDeleteSyncRequest(String thingName, String shadowName) {
        CloudDeleteSyncRequest cloudDeleteSyncRequest = new CloudDeleteSyncRequest(thingName, shadowName, this.dao);
    }

    /**
     * Pushes a delete sync request in the request queue to delete a local shadow after a cloud shadow has
     * been successfully deleted.
     * TODO: implement message queue data structure to push SyncRequest
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void pushLocalDeleteSyncRequest(String thingName, String shadowName) {
        LocalDeleteSyncRequest localDeleteSyncRequest = new LocalDeleteSyncRequest(thingName, shadowName, this.dao,
                this.deleteThingShadowIPCHandler);
    }
}
