/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

/**
 * Sync request to delete shadow in the cloud.
 */
public class CloudDeleteSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(CloudDeleteSyncRequest.class);

    /**
     * Ctr for CloudDeleteSyncRequest.
     *
     * @param thingName     The thing name associated with the sync shadow update
     * @param shadowName    The shadow name associated with the sync shadow update
     */
    public CloudDeleteSyncRequest(String thingName, String shadowName) {
        super(thingName, shadowName);
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
    public void execute(SyncContext context) throws SyncException, RetryableException, SkipSyncRequestException {
        try {
            context.getIotDataPlaneClient().deleteThingShadow(getThingName(), getShadowName());
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException e) {
            throw new SkipSyncRequestException(e);
        }

        Optional<SyncInformation> syncInformation = context.getDao().getShadowSyncInformation(getThingName(),
                getShadowName());
        try {
            context.getDao().updateSyncInformation(SyncInformation.builder()
                    .lastSyncedDocument(null)
                    .cloudVersion(syncInformation.map(SyncInformation::getCloudVersion).orElse(0L))
                    .cloudDeleted(true)
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .build());
        } catch (ShadowManagerDataException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Failed to update sync table after deleting cloud shadow");
        }
    }
}
