/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowState;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientFactory;
import com.aws.greengrass.shadowmanager.util.DataOwner;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.SyncNodeMerger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nullable;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.OBJECT_MAPPER;

/**
 * Sync request handling a full sync request for a particular shadow.
 */
public class FullShadowSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(FullShadowSyncRequest.class);
    @NonNull
    UpdateThingShadowRequestHandler updateThingShadowRequestHandler;

    @NonNull
    DeleteThingShadowRequestHandler deleteThingShadowRequestHandler;

    @NonNull
    IotDataPlaneClientFactory clientFactory;

    /**
     * Ctr for FullShadowSyncRequest.
     *
     * @param thingName                       The thing name associated with the sync shadow update
     * @param shadowName                      The shadow name associated with the sync shadow update
     * @param dao                             Local shadow database management
     * @param deleteThingShadowRequestHandler Reference to the DeleteThingShadow IPC Handler
     * @param updateThingShadowRequestHandler Reference to the UpdateThingShadow IPC Handler
     * @param clientFactory                   The IoT data plane client factory to make shadow operations on the cloud.
     */
    public FullShadowSyncRequest(String thingName,
                                 String shadowName,
                                 ShadowManagerDAO dao,
                                 UpdateThingShadowRequestHandler updateThingShadowRequestHandler,
                                 DeleteThingShadowRequestHandler deleteThingShadowRequestHandler,
                                 IotDataPlaneClientFactory clientFactory) {
        super(thingName, shadowName, dao);
        this.updateThingShadowRequestHandler = updateThingShadowRequestHandler;
        this.deleteThingShadowRequestHandler = deleteThingShadowRequestHandler;
        this.clientFactory = clientFactory;
    }

    /**
     * Executes a full shadow sync.
     *
     * @throws SyncException            if there is any exception while making the HTTP shadow request to the cloud.
     * @throws RetryableException       if the cloud version is not the same as the version of the shadow on the cloud
     *                                  or if the cloud is throttling the request.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     */
    @Override
    public void execute() throws SyncException, RetryableException, SkipSyncRequestException {
        Optional<ShadowDocument> localShadowDocument = this.dao.getShadowThing(getThingName(), getShadowName());
        Optional<SyncInformation> syncInformation = this.dao.getShadowSyncInformation(getThingName(), getShadowName());
        Optional<ShadowDocument> cloudShadowDocument = getCloudShadowDocument();

        if (!cloudShadowDocument.isPresent() && localShadowDocument.isPresent()) {
            deleteLocalShadowDocument();
            syncInformation.ifPresent(information -> this.dao.updateSyncInformation(SyncInformation.builder()
                    .localVersion(localShadowDocument.get().getVersion())
                    .cloudVersion(information.getCloudVersion())
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    // TODO: get the latest from metadata?
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .lastSyncedDocument(null)
                    .build()));
            return;
        }

        //TODO: Need to figure out the actual behavior here. Might need to delete the cloud here right? Also add similar
        //    logic if cloud does not exist and the local exists?
        if (!localShadowDocument.isPresent() && cloudShadowDocument.isPresent()) {
            deleteCloudShadowDocument();
            syncInformation.ifPresent(information -> this.dao.updateSyncInformation(SyncInformation.builder()
                    .localVersion(information.getLocalVersion())
                    .cloudVersion(cloudShadowDocument.get().getVersion())
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    // TODO: get the latest from metadata?
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .lastSyncedDocument(null)
                    .build()));
            return;
        }

        // Get the sync information and check if the versions are same. If the local and cloud versions are same, we
        // don't need to do any sync.
        if (isDocVersionSame(cloudShadowDocument.get(), syncInformation, DataOwner.CLOUD)
                && isDocVersionSame(localShadowDocument.get(), syncInformation, DataOwner.LOCAL)) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv("local-version", localShadowDocument.get().getVersion())
                    .kv("cloud-version", cloudShadowDocument.get().getVersion())
                    .log("Not performing full sync since both local and cloud versions are already in sync");
            return;
        }
        ShadowDocument baseShadowDocument = deserializeLastSyncedShadowDocument(syncInformation);

        // Gets the merged reported node from the local, cloud and base documents. If an existing field has changed in
        // both local and cloud, the local document's value will be selected.
        JsonNode mergedReportedNode = SyncNodeMerger.getMergedNode(getReported(localShadowDocument.get()),
                getReported(cloudShadowDocument.get()), getReported(baseShadowDocument), DataOwner.LOCAL);

        // Gets the merged desired node from the local, cloud and base documents. If an existing field has changed in
        // both local and cloud, the cloud document's value will be selected.
        JsonNode mergedDesiredNode = SyncNodeMerger.getMergedNode(getDesired(localShadowDocument.get()),
                getDesired(cloudShadowDocument.get()), getDesired(baseShadowDocument), DataOwner.CLOUD);
        ShadowState updatedState = new ShadowState(mergedDesiredNode, mergedReportedNode);

        JsonNode updatedStateJson = updatedState.toJson();
        ObjectNode updateDocument = OBJECT_MAPPER.createObjectNode();
        updateDocument.set(SHADOW_DOCUMENT_STATE, updatedStateJson);

        long localDocumentVersion = localShadowDocument.get().getVersion();
        long cloudDocumentVersion = cloudShadowDocument.get().getVersion();

        // If the cloud document version is different from the last sync, that means the local document needed
        // some updates. So we go ahead an update the local shadow document.
        if (!isDocVersionSame(cloudShadowDocument.get(), syncInformation, DataOwner.CLOUD)) {
            updateDocument.set(SHADOW_DOCUMENT_VERSION, new LongNode(localShadowDocument.get().getVersion()));
            SdkBytes payloadBytes = getSdkBytes(updateDocument);
            updateLocalShadowDocument(payloadBytes);
            localDocumentVersion = localDocumentVersion + 1;
        }
        // If the local document version is different from the last sync, that means the cloud document needed
        // some updates. So we go ahead an update the cloud shadow document.
        if (!isDocVersionSame(localShadowDocument.get(), syncInformation, DataOwner.LOCAL)) {
            updateDocument.set(SHADOW_DOCUMENT_VERSION, new LongNode(cloudShadowDocument.get().getVersion()));
            SdkBytes payloadBytes = getSdkBytes(updateDocument);
            updateCloudShadowDocument(payloadBytes);
            cloudDocumentVersion = cloudDocumentVersion + 1;
        }

        if (!isDocVersionSame(localShadowDocument.get(), syncInformation, DataOwner.LOCAL)
                || !isDocVersionSame(cloudShadowDocument.get(), syncInformation, DataOwner.CLOUD)) {
            updateDocument.remove(SHADOW_DOCUMENT_VERSION);
            this.dao.updateSyncInformation(SyncInformation.builder()
                    .localVersion(localDocumentVersion)
                    .cloudVersion(cloudDocumentVersion)
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    // TODO: get the latest from metadata?
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .lastSyncedDocument(getSdkBytes(updateDocument).asByteArray())
                    .build());
        }
    }

    /**
     * Deserialize the last synced shadow document if sync information is present.
     *
     * @param syncInformation the sync informationf for the shadow.
     * @return the Shadow Docment if the sync information is present; Else null.
     */
    private ShadowDocument deserializeLastSyncedShadowDocument(Optional<SyncInformation> syncInformation) {
        if (syncInformation.isPresent()) {
            try {
                return new ShadowDocument(syncInformation.get().getLastSyncedDocument());
            } catch (IOException e) {
                logger.atWarn()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .cause(e).log();
            }
        }
        return null;
    }

    private JsonNode getReported(ShadowDocument shadowDocument) {
        if (shadowDocument == null) {
            return null;
        }
        return shadowDocument.getState() == null ? null : shadowDocument.getState().getReported();
    }

    private JsonNode getDesired(ShadowDocument shadowDocument) {
        if (shadowDocument == null) {
            return null;
        }
        return shadowDocument.getState() == null ? null : shadowDocument.getState().getDesired();
    }

    @NonNull
    private SdkBytes getSdkBytes(ObjectNode updateDocument) throws SkipSyncRequestException {
        SdkBytes payloadBytes;
        try {
            payloadBytes = SdkBytes.fromByteArray(JsonUtil.getPayloadBytes(updateDocument));
        } catch (JsonProcessingException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Unable to serialize update request");
            throw new SkipSyncRequestException(e);
        }
        return payloadBytes;
    }

    private boolean isDocVersionSame(ShadowDocument shadowDocument, @NonNull Optional<SyncInformation> syncInformation,
                                     DataOwner owner) {
        return shadowDocument != null && syncInformation.isPresent()
                && (DataOwner.CLOUD.equals(owner)
                && syncInformation.get().getCloudVersion() == shadowDocument.getVersion()
                || DataOwner.LOCAL.equals(owner)
                && syncInformation.get().getLocalVersion() == shadowDocument.getVersion());
    }

    @Nullable
    private Optional<ShadowDocument> getCloudShadowDocument() throws RetryableException, SkipSyncRequestException {
        try {
            GetThingShadowResponse getThingShadowResponse = this.clientFactory.getIotDataPlaneClient()
                    .getThingShadow(GetThingShadowRequest
                            .builder()
                            .shadowName(getShadowName())
                            .thingName(getThingName())
                            .build());
            if (getThingShadowResponse != null && getThingShadowResponse.payload() != null) {
                return Optional.of(new ShadowDocument(getThingShadowResponse.payload().asByteArray()));
            }
        } catch (ResourceNotFoundException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e)
                    .log("Unable to find cloud shadow");
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow get request");
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException | IOException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow get request");
            throw new SkipSyncRequestException(e);
        }
        return Optional.empty();
    }

    private void updateLocalShadowDocument(SdkBytes payloadBytes) {
        logger.atDebug()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .log("Updating local shadow document");

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localRequest =
                new software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest();
        localRequest.setPayload(payloadBytes.asByteArray());
        localRequest.setShadowName(getShadowName());
        localRequest.setThingName(getThingName());
        updateThingShadowRequestHandler.handleRequest(localRequest, ShadowManager.SERVICE_NAME);
    }

    private void deleteLocalShadowDocument() {
        logger.atDebug()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .log("Deleting local shadow document");

        software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest localRequest =
                new software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest();
        localRequest.setShadowName(getShadowName());
        localRequest.setThingName(getThingName());
        deleteThingShadowRequestHandler.handleRequest(localRequest, ShadowManager.SERVICE_NAME);
    }

    private void updateCloudShadowDocument(SdkBytes updateDocument)
            throws RetryableException, SkipSyncRequestException {
        try {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Updating cloud shadow document");

            this.clientFactory.getIotDataPlaneClient().updateThingShadow(UpdateThingShadowRequest.builder()
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .payload(updateDocument)
                    .build());
        } catch (ConflictException | ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow update request");
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow update request");
            throw new SkipSyncRequestException(e);
        }
    }

    private void deleteCloudShadowDocument()
            throws RetryableException, SkipSyncRequestException {
        try {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Deleting cloud shadow document");

            this.clientFactory.getIotDataPlaneClient().deleteThingShadow(DeleteThingShadowRequest.builder()
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .build());
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow delete request");
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow delete request");
            throw new SkipSyncRequestException(e);
        }
    }
}