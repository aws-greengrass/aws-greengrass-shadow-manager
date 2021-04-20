/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import lombok.NonNull;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
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

    @NonNull
    IotDataPlaneClientFactory clientFactory;

    /**
     * Ctr for CloudDeleteSyncRequest.
     *
     * @param thingName     The thing name associated with the sync shadow update
     * @param shadowName    The shadow name associated with the sync shadow update
     * @param dao           Local shadow database management
     * @param clientFactory The IoT data plane client factory to make shadow operations on the cloud.
     */
    public CloudDeleteSyncRequest(String thingName,
                                  String shadowName,
                                  ShadowManagerDAO dao,
                                  IotDataPlaneClientFactory clientFactory) {
        super(thingName, shadowName, dao);
        this.clientFactory = clientFactory;
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
        try {
            this.clientFactory.getIotDataPlaneClient().deleteThingShadow(DeleteThingShadowRequest
                    .builder()
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .build());
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Could not execute cloud shadow delete request");
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Could not execute cloud shadow delete request");
            throw new SkipSyncRequestException(e);
        }

        Optional<SyncInformation> syncInformation = this.dao.getShadowSyncInformation(getThingName(), getShadowName());
        try {
            this.dao.updateSyncInformation(SyncInformation.builder()
                    .cloudDocument(null)
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
                    .cause(e).log();
        }
    }
}
