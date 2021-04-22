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
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

/**
 * Sync request to update shadow in the cloud.
 */
public class CloudUpdateSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(CloudUpdateSyncRequest.class);

    @NonNull
    JsonNode updateDocument;

    @NonNull
    IotDataPlaneClientFactory clientFactory;

    /**
     * Ctr for CloudUpdateSyncRequest.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument The update request bytes.
     * @param dao            Local shadow database management
     * @param clientFactory  The IoT data plane client factory to make shadow operations on the cloud.
     */
    public CloudUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  JsonNode updateDocument,
                                  ShadowManagerDAO dao,
                                  IotDataPlaneClientFactory clientFactory) {
        super(thingName, shadowName, dao);
        this.updateDocument = updateDocument;
        this.clientFactory = clientFactory;
    }

    /**
     * Executes a cloud shadow update after a successful local shadow update.
     *
     * @throws SyncException            if there is any exception while making the HTTP shadow request to the cloud.
     * @throws RetryableException       if the cloud version is not the same as the version of the shadow on the cloud
     *                                  or if the cloud is throttling the request.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     */
    @SuppressWarnings("PMD.PrematureDeclaration")
    @Override
    public void execute() throws SyncException, RetryableException, SkipSyncRequestException {
        Optional<ShadowDocument> shadowDocument = this.dao.getShadowThing(getThingName(), getShadowName());
        if (!shadowDocument.isPresent()) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Unable to sync shadow since shadow does not exist");
            return;
        }
        long cloudVersion = getAndUpdateCloudVersionInRequest();

        try {
            this.clientFactory.getIotDataPlaneClient().updateThingShadow(UpdateThingShadowRequest.builder()
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .payload(SdkBytes.fromByteArray(JsonUtil.getPayloadBytes(updateDocument)))
                    .build());
        } catch (ConflictException | ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Could not execute cloud shadow delete request");
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException | IOException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Could not execute cloud shadow delete request");
            throw new SkipSyncRequestException(e);
        }

        try {
            this.dao.updateSyncInformation(SyncInformation.builder()
                    .lastSyncedDocument(JsonUtil.getPayloadBytes(shadowDocument.get().toJson(false)))
                    .cloudVersion(cloudVersion + 1)
                    .cloudDeleted(false)
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .build());
        } catch (JsonProcessingException | ShadowManagerDataException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log();
        }
    }

    private long getAndUpdateCloudVersionInRequest() {
        long cloudVersion = 0;
        Optional<SyncInformation> syncInformation = this.dao.getShadowSyncInformation(getThingName(), getShadowName());

        // If the sync information is correct
        if (syncInformation.isPresent()) {
            cloudVersion = syncInformation.get().getCloudVersion();
        }
        ((ObjectNode) updateDocument).set(SHADOW_DOCUMENT_VERSION, new LongNode(cloudVersion));

        return cloudVersion;
    }
}
