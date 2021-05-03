/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowState;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.DataOwner;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.shadowmanager.util.SyncNodeMerger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
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

    private SyncContext context;

    /**
     * Ctr for FullShadowSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     */
    public FullShadowSyncRequest(String thingName, String shadowName) {
        super(thingName, shadowName);
    }

    /**
     * Executes a full shadow sync.
     *
     * @param  context                  the execution context.
     * @throws RetryableException       if the cloud version is not the same as the version of the shadow on the cloud
     *                                  or if the cloud is throttling the request.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     */
    @Override
    public void execute(SyncContext context) throws RetryableException, SkipSyncRequestException {
        Optional<SyncInformation> syncInformation = context.getDao().getShadowSyncInformation(getThingName(),
                getShadowName());

        if (!syncInformation.isPresent()) {
            // This should never happen since we always add a default sync info entry in the DB.
            throw new SkipSyncRequestException("Unable to find sync information");
        }

        this.context = context;

        Optional<ShadowDocument> localShadowDocument = context.getDao().getShadowThing(getThingName(), getShadowName());
        Optional<ShadowDocument> cloudShadowDocument = getCloudShadowDocument();
        // If both the local and cloud document does not exist, then update the sync info and return.
        if (!cloudShadowDocument.isPresent() && !localShadowDocument.isPresent()) {
            logger.atInfo()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.get().getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.get().getCloudVersion())
                    .log("Not performing full sync since both local and cloud versions are already in sync since "
                            + "they don't exist in local and cloud");
            context.getDao().updateSyncInformation(SyncInformation.builder()
                    .localVersion(syncInformation.get().getLocalVersion())
                    .cloudVersion(syncInformation.get().getCloudVersion())
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    // TODO: get the latest from metadata?
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .lastSyncedDocument(null)
                    .build());
            return;
        }

        // If only the cloud document does not exist, check if this is the first time we are syncing this shadow. If we
        // are, go ahead and update the cloud with the local document and update the sync information.
        // If it's not the first time for sync, go ahead and delete the local shadow and update the sync info.
        if (!cloudShadowDocument.isPresent()) {
            if (isFirstSync(syncInformation.get())) {
                handleFirstCloudSync(localShadowDocument.get());
            } else {
                handleLocalDelete(localShadowDocument.get(), syncInformation.get());
            }
            return;
        }

        // If only the local document does not exist, check if this is the first time we are syncing this shadow. If we
        // are, go ahead and update the local with the cloud document and update the sync information.
        // If it's not the first time for sync, go ahead and delete the cloud shadow and update the sync info.
        if (!localShadowDocument.isPresent()) {
            if (isFirstSync(syncInformation.get())) {
                handleFirstLocalSync(cloudShadowDocument.get());
            } else {
                handleCloudDelete(cloudShadowDocument.get(), syncInformation.get());
            }
            return;
        }

        // Get the sync information and check if the versions are same. If the local and cloud versions are same, we
        // don't need to do any sync.
        if (isDocVersionSame(cloudShadowDocument.get(), syncInformation.get(), DataOwner.CLOUD)
                && isDocVersionSame(localShadowDocument.get(), syncInformation.get(), DataOwner.LOCAL)) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, localShadowDocument.get().getVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, cloudShadowDocument.get().getVersion())
                    .log("Not performing full sync since both local and cloud versions are already in sync");
            return;
        }
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, localShadowDocument.get().getVersion())
                .kv(LOG_CLOUD_VERSION_KEY, cloudShadowDocument.get().getVersion())
                .log("Performing full sync");
        ShadowDocument baseShadowDocument = deserializeLastSyncedShadowDocument(syncInformation.get());

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
        if (!isDocVersionSame(cloudShadowDocument.get(), syncInformation.get(), DataOwner.CLOUD)) {
            localDocumentVersion = updateLocalDocumentAndGetUpdatedVersion(updateDocument,
                    Optional.of(localDocumentVersion));
        }
        // If the local document version is different from the last sync, that means the cloud document needed
        // some updates. So we go ahead an update the cloud shadow document.
        if (!isDocVersionSame(localShadowDocument.get(), syncInformation.get(), DataOwner.LOCAL)) {
            cloudDocumentVersion = updateCloudDocumentAndGetUpdatedVersion(updateDocument,
                    Optional.of(cloudDocumentVersion));
        }

        if (!isDocVersionSame(localShadowDocument.get(), syncInformation.get(), DataOwner.LOCAL)
                || !isDocVersionSame(cloudShadowDocument.get(), syncInformation.get(), DataOwner.CLOUD)) {
            updateSyncInformation(updateDocument, localDocumentVersion, cloudDocumentVersion);
        }
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, localShadowDocument.get().getVersion())
                .kv(LOG_CLOUD_VERSION_KEY, cloudShadowDocument.get().getVersion())
                .log("Successfully performed full sync");
    }

    /**
     * Check if this is the first time there is a sync for the thing's shadow by checking the last sync time.
     *
     * @param syncInformation The sync information for the thing's shadow.
     * @return true if this is the first time the shadow is being sync; Else false.
     */
    private boolean isFirstSync(@NonNull SyncInformation syncInformation) {
        return syncInformation.getLastSyncTime() == Instant.EPOCH.getEpochSecond();
    }

    /**
     * Delete the cloud shadow using the IoT Data plane client and then update the sync information.
     *
     * @param syncInformation     The sync information for the thing's shadow.
     * @param cloudShadowDocument The current cloud document.
     * @throws RetryableException       if the delete request to cloud encountered a retryable exception.
     * @throws SkipSyncRequestException if the delete request to cloud encountered a skipable exception.
     */
    private void handleCloudDelete(@NonNull ShadowDocument cloudShadowDocument,
                                   @NonNull SyncInformation syncInformation)
            throws RetryableException, SkipSyncRequestException {
        deleteCloudShadowDocument();
        context.getDao().updateSyncInformation(SyncInformation.builder()
                .localVersion(syncInformation.getLocalVersion())
                .cloudVersion(cloudShadowDocument.getVersion())
                .shadowName(getShadowName())
                .thingName(getThingName())
                // TODO: get the latest from metadata?
                .cloudUpdateTime(Instant.now().getEpochSecond())
                .lastSyncedDocument(null)
                .build());
    }

    /**
     * Create the local shadow using the request handlers and then update the sync information.
     *
     * @param cloudShadowDocument The current cloud document.
     * @throws SkipSyncRequestException if the update request encountered a skipable exception.
     */
    private void handleFirstLocalSync(@NonNull ShadowDocument cloudShadowDocument)
            throws SkipSyncRequestException {
        logger.atInfo()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_CLOUD_VERSION_KEY, cloudShadowDocument.getVersion())
                .log("Syncing local shadow for the first time");

        ObjectNode updateDocument = (ObjectNode) cloudShadowDocument.toJson(false);
        long localDocumentVersion = updateLocalDocumentAndGetUpdatedVersion(updateDocument, Optional.empty());
        updateSyncInformation(updateDocument, localDocumentVersion, cloudShadowDocument.getVersion());
    }

    /**
     * Delete the local shadow using the request handlers and then update the sync information.
     *
     * @param syncInformation     The sync information for the thing's shadow.
     * @param localShadowDocument The current local document.
     * @throws SkipSyncRequestException if the delete request encountered a skipable exception.
     */
    private void handleLocalDelete(@NonNull ShadowDocument localShadowDocument,
                                   @NonNull SyncInformation syncInformation) throws SkipSyncRequestException {
        deleteLocalShadowDocument();
        context.getDao().updateSyncInformation(SyncInformation.builder()
                .localVersion(localShadowDocument.getVersion())
                .cloudVersion(syncInformation.getCloudVersion())
                .shadowName(getShadowName())
                .thingName(getThingName())
                // TODO: get the latest from metadata?
                .cloudUpdateTime(Instant.now().getEpochSecond())
                .lastSyncedDocument(null)
                .build());
    }

    /**
     * Create the cloud shadow using the IoT Data plane client and then update the sync information.
     * @param localShadowDocument The current local document.
     * @throws SkipSyncRequestException if the update request to cloud encountered a skipable exception.
     */
    private void handleFirstCloudSync(@NonNull ShadowDocument localShadowDocument)
            throws SkipSyncRequestException, RetryableException {
        logger.atInfo()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, localShadowDocument.getVersion())
                .log("Syncing cloud shadow for the first time");
        ObjectNode updateDocument = (ObjectNode) localShadowDocument.toJson(false);
        long cloudDocumentVersion = updateCloudDocumentAndGetUpdatedVersion(updateDocument, Optional.empty());
        updateSyncInformation(updateDocument, localShadowDocument.getVersion(), cloudDocumentVersion);
    }

    /**
     * Add the version node to the update request payload and then update the local shadow document using that request.
     *
     * @param updateDocument       The update request payload.
     * @param localDocumentVersion The current local document version.
     * @return the updated local document version.
     * @throws SkipSyncRequestException if the update request encountered a skipable exception.
     */
    private long updateLocalDocumentAndGetUpdatedVersion(ObjectNode updateDocument, Optional<Long> localDocumentVersion)
            throws SkipSyncRequestException {
        localDocumentVersion.ifPresent(version ->
                updateDocument.set(SHADOW_DOCUMENT_VERSION, new LongNode(version)));
        byte[] payloadBytes = getPayloadBytes(updateDocument);
        updateLocalShadowDocument(payloadBytes);
        //TODO: get the version from the response object
        return localDocumentVersion.map(version -> version + 1).orElse(1L);
    }

    /**
     * Update the sync information for the thing's shadow using the update request payload and the current local and
     * cloud version.
     *
     * @param updateDocument       The update request payload.
     * @param localDocumentVersion The current local document version.
     * @param cloudDocumentVersion The current cloud document version.
     * @throws SkipSyncRequestException if the serialization of the update request payload failed.
     */
    private void updateSyncInformation(ObjectNode updateDocument, long localDocumentVersion, long cloudDocumentVersion)
            throws SkipSyncRequestException {
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, localDocumentVersion)
                .kv(LOG_CLOUD_VERSION_KEY, cloudDocumentVersion)
                .log("Updating sync information");

        updateDocument.remove(SHADOW_DOCUMENT_VERSION);
        context.getDao().updateSyncInformation(SyncInformation.builder()
                .localVersion(localDocumentVersion)
                .cloudVersion(cloudDocumentVersion)
                .shadowName(getShadowName())
                .thingName(getThingName())
                // TODO: get the latest from metadata?
                .cloudUpdateTime(Instant.now().getEpochSecond())
                .lastSyncedDocument(getPayloadBytes(updateDocument))
                .build());
    }

    /**
     * Add the version node to the update request payload and then update the cloud shadow document using that request.
     *
     * @param updateDocument       The update request payload.
     * @param cloudDocumentVersion The current cloud document version.
     * @return the updated local document version.
     * @throws RetryableException       if the delete request to cloud encountered a retryable exception or if the
     *                                  serialization of the update request payload failed.
     * @throws SkipSyncRequestException if the delete request to cloud encountered a skipable exception.
     */
    private long updateCloudDocumentAndGetUpdatedVersion(ObjectNode updateDocument, Optional<Long> cloudDocumentVersion)
            throws SkipSyncRequestException, RetryableException {
        cloudDocumentVersion.ifPresent(version ->
                updateDocument.set(SHADOW_DOCUMENT_VERSION, new LongNode(version)));
        byte[] payloadBytes = getPayloadBytes(updateDocument);
        updateCloudShadowDocument(payloadBytes);
        //TODO: get the version from the response object
        return cloudDocumentVersion.map(version -> version + 1).orElse(1L);
    }

    /**
     * Deserialize the last synced shadow document if sync information is present.
     *
     * @param syncInformation the sync informationf for the shadow.
     * @return the Shadow Document if the sync information is present; Else null.
     * @throws SkipSyncRequestException if the serialization of the last synced document failed.
     */
    private ShadowDocument deserializeLastSyncedShadowDocument(@NonNull SyncInformation syncInformation)
            throws SkipSyncRequestException {
        try {
            return new ShadowDocument(syncInformation.getLastSyncedDocument());
        } catch (IOException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not deserialize last synced shadow document");
            //TODO: Should we put the last synced as null? (we should not ever be in this situation though)
            throw new SkipSyncRequestException(e);
        }
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

    /**
     * Gets the SDK bytes object from the Object Node.
     *
     * @param updateDocument The update request payload.
     * @return The SDK bytes object for the update request.
     * @throws SkipSyncRequestException if the serialization of the update request payload failed.
     */
    private byte[] getPayloadBytes(ObjectNode updateDocument) throws SkipSyncRequestException {
        byte[] payloadBytes;
        try {
            payloadBytes = JsonUtil.getPayloadBytes(updateDocument);
        } catch (JsonProcessingException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Unable to serialize update request");
            throw new SkipSyncRequestException(e);
        }
        return payloadBytes;
    }

    /**
     * Check if the current shadow document version is same as the version in the sync information for the shadow.
     *
     * @param shadowDocument  The current shadow document.
     * @param syncInformation The sync information for the shadow.
     * @param owner           The owner of the shadow i.e. LOCAL or CLOUD.
     * @return true if the version was the same; Else false.
     */
    private boolean isDocVersionSame(ShadowDocument shadowDocument, @NonNull SyncInformation syncInformation,
                                     DataOwner owner) {
        return shadowDocument != null
                && (DataOwner.CLOUD.equals(owner) && syncInformation.getCloudVersion() == shadowDocument.getVersion()
                || DataOwner.LOCAL.equals(owner) && syncInformation.getLocalVersion() == shadowDocument.getVersion());
    }

    /**
     * Gets the cloud shadow document using the IoT Data plane client.
     *
     * @return an optional of the cloud shadow document if it existed.
     * @throws RetryableException       if the get request encountered errors which should be retried.
     * @throws SkipSyncRequestException if the get request encountered errors which should be skipped.
     */
    private Optional<ShadowDocument> getCloudShadowDocument() throws RetryableException,
            SkipSyncRequestException {
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .log("Getting cloud shadow document");
        try {
            GetThingShadowResponse getThingShadowResponse = context.getIotDataPlaneClient()
                    .getThingShadow(getThingName(), getShadowName());
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

    /**
     * Update the local shadow document using the update request handler.
     *
     * @param payloadBytes The update request bytes.
     * @throws ConflictError            if the update request for local had a bad version.
     * @throws SkipSyncRequestException if the update request encountered errors which should be skipped.
     */
    private void updateLocalShadowDocument(byte[] payloadBytes) throws ConflictError,
            SkipSyncRequestException {
        logger.atDebug()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .log("Updating local shadow document");

        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest localRequest =
                new software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest();
        localRequest.setPayload(payloadBytes);
        localRequest.setShadowName(getShadowName());
        localRequest.setThingName(getThingName());
        try {
            context.getUpdateHandler().handleRequest(localRequest, ShadowManager.SERVICE_NAME);
        } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError e) {
            throw new SkipSyncRequestException(e);
        }
    }

    /**
     * Delete the local shadow document using the delete request handler.
     *
     * @throws SkipSyncRequestException if the delete request encountered errors which should be skipped.
     */
    private void deleteLocalShadowDocument() throws SkipSyncRequestException {
        logger.atInfo()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .log("Deleting local shadow document");

        software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest localRequest =
                new software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest();
        localRequest.setShadowName(getShadowName());
        localRequest.setThingName(getThingName());
        try {
            context.getDeleteHandler().handleRequest(localRequest, ShadowManager.SERVICE_NAME);
        } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError e) {
            throw new SkipSyncRequestException(e);
        }
    }

    /**
     * Update the cloud document using the IoT Data plane client.
     *
     * @param updateDocument The update request payload.
     * @throws ConflictException        if the update request for cloud had a bad version.
     * @throws RetryableException       if the update request encountered errors which should be retried.
     * @throws SkipSyncRequestException if the update request encountered errors which should be skipped.
     */
    private void updateCloudShadowDocument(byte[] updateDocument)
            throws ConflictException, RetryableException, SkipSyncRequestException {
        try {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Updating cloud shadow document");

            context.getIotDataPlaneClient().updateThingShadow(getThingName(), getShadowName(), updateDocument);
        } catch (ConflictException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Conflict exception occurred while updating cloud document.");
            throw e;
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
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

    /**
     * Delete the cloud document using the IoT Data plane client.
     *
     * @throws RetryableException       if the delete request encountered errors which should be retried.
     * @throws SkipSyncRequestException if the delete request encountered errors which should be skipped.
     */
    private void deleteCloudShadowDocument() throws RetryableException, SkipSyncRequestException {
        try {
            logger.atInfo()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Deleting cloud shadow document");
            context.getIotDataPlaneClient().deleteThingShadow(getThingName(), getShadowName());
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
