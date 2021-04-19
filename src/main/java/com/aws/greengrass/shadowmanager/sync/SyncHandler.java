/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Class which handles syncing shadows to IoT Shadow Service.
 * TODO: Remove PMD suppressed warning once sync requests implemented
 */
@SuppressWarnings("PMD")
public class SyncHandler {

    private final ShadowManagerDAO dao;
    private final ShadowManager shadowManager;
    private final IotDataPlaneClientFactory clientFactory;


    /**
     * Ctr for SyncHandler for unit testing.
     *
     * @param dao           Local shadow database management.
     * @param shadowManager The Shadow Manager instance to get the Update/Delete handlers.
     * @param clientFactory The IoT data plane client factory to make shadow operations on the cloud.
     */
    public SyncHandler(ShadowManagerDAOImpl dao, IotDataPlaneClientFactory clientFactory, ShadowManager shadowManager) {
        this.dao = dao;
        this.shadowManager = shadowManager;
        this.clientFactory = clientFactory;
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
                this.shadowManager.getUpdateThingShadowRequestHandler(),
                this.shadowManager.getDeleteThingShadowRequestHandler());
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
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument The update shadow request
     */
    public void pushCloudUpdateSyncRequest(String thingName, String shadowName, JsonNode updateDocument) {
        CloudUpdateSyncRequest cloudUpdateSyncRequest = new CloudUpdateSyncRequest(thingName, shadowName,
                updateDocument, this.dao, this.clientFactory);
    }

    /**
     * Pushes an update sync request to the request queue to update local shadow after a cloud shadow has
     * been successfully updated.
     * TODO: implement message queue data structure to push SyncRequest
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument Update document to be applied to local shadow
     */
    public void pushLocalUpdateSyncRequest(String thingName, String shadowName, byte[] updateDocument) {
        LocalUpdateSyncRequest localUpdateSyncRequest = new LocalUpdateSyncRequest(thingName, shadowName,
                updateDocument, this.dao, this.shadowManager.getUpdateThingShadowRequestHandler());
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
        CloudDeleteSyncRequest cloudDeleteSyncRequest = new CloudDeleteSyncRequest(thingName, shadowName, this.dao,
                this.clientFactory);
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
                this.shadowManager.getDeleteThingShadowRequestHandler());
    }
}
