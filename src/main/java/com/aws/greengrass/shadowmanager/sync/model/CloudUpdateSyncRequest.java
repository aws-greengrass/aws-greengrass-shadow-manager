/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;

/**
 * Sync request to update shadow in the cloud.
 */
public class CloudUpdateSyncRequest extends BaseSyncRequest {

    /**
     * Ctr for CloudUpdateSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param dao                         Local shadow database management
     */
    public CloudUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  ShadowManagerDAO dao) {
        super(thingName, shadowName, dao);
    }

    @Override
    public void execute() throws SyncException {

    }
}
