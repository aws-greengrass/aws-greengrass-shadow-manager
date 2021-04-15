/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import lombok.NonNull;

import java.time.Instant;

/**
 * Sync request handling an update from cloud.
 */
public class CloudUpdateSyncRequest extends BaseSyncRequest {

    // TODO: determine update document type
    String updateDocument;

    @NonNull
    UpdateThingShadowIPCHandler updateThingShadowIPCHandler;

    /**
     * Ctr for CloudUpdateSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param updateTime                  The update time of the specific sync shadow update
     * @param updateDocument              The update document from the cloud
     * @param version                     The version of the specific sync shadow update
     * @param dao                         Local shadow database management
     * @param updateThingShadowIPCHandler Reference to the UpdateThingShadow IPC Handler
     */
    public CloudUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  String updateDocument,
                                  Instant updateTime,
                                  int version,
                                  ShadowManagerDAO dao,
                                  UpdateThingShadowIPCHandler updateThingShadowIPCHandler) {
        super(thingName, shadowName, updateTime, version, dao);
        this.updateDocument = updateDocument;
        this.updateThingShadowIPCHandler = updateThingShadowIPCHandler;
    }


    @Override
    public void execute() throws SyncException {

    }
}
