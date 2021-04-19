/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.sync.ShadowHttpClient;
import lombok.NonNull;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;

/**
 * Sync request to delete shadow in the cloud.
 */
public class CloudDeleteSyncRequest extends BaseSyncRequest {

    @NonNull
    ShadowHttpClient shadowHttpClient;

    /**
     * Ctr for CloudDeleteSyncRequest.
     *
     * @param thingName        The thing name associated with the sync shadow update
     * @param shadowName       The shadow name associated with the sync shadow update
     * @param dao              Local shadow database management
     * @param shadowHttpClient The HTTP client to make shadow operations on the cloud.
     */
    public CloudDeleteSyncRequest(String thingName,
                                  String shadowName,
                                  ShadowManagerDAO dao,
                                  ShadowHttpClient shadowHttpClient) {
        super(thingName, shadowName, dao);
        this.shadowHttpClient = shadowHttpClient;
    }


    /**
     * Executes a cloud shadow delete after a successful local shadow delete.
     *
     * @throws SyncException            if there is any exception while making the HTTP shadow request to the cloud.
     * @throws RetryableException       if the cloud version is not the same as the version of the shadow on the cloud
     *                                  or if the cloud is throttling the request.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     */
    @Override
    public void execute() throws SyncException, RetryableException, SkipSyncRequestException {

    }
}
