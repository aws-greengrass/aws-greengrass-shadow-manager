/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import lombok.NonNull;

/**
 * Sync request to update locally stored shadow.
 */
public class LocalUpdateSyncRequest extends BaseSyncRequest {

    @NonNull
    UpdateThingShadowRequestHandler updateThingShadowRequestHandler;

    /**
     * Ctr for LocalUpdateSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param dao                         Local shadow database management
     * @param updateThingShadowRequestHandler Reference to the UpdateThingShadow IPC Handler
     */
    public LocalUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  ShadowManagerDAO dao,
                                  UpdateThingShadowRequestHandler updateThingShadowRequestHandler) {
        super(thingName, shadowName, dao);
        this.updateThingShadowRequestHandler = updateThingShadowRequestHandler;
    }

    @Override
    public void execute() throws SyncException {

    }
}
