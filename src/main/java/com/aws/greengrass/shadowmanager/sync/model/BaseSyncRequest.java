/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.LogEventBuilder;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonMerger;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

/**
 * Base class for all sync requests.
 */
public abstract class BaseSyncRequest extends ShadowRequest implements SyncRequest {
    private static final Logger logger = LogManager.getLogger(BaseSyncRequest.class);

    @Setter(AccessLevel.PROTECTED)
    @Getter(AccessLevel.PROTECTED)
    private SyncContext context;

    /**
     * Ctr for BaseSyncRequest.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    protected BaseSyncRequest(String thingName,
                              String shadowName) {
        super(thingName, shadowName);
    }

    /**
     * Check if this request is necessary or not.
     *
     * @param context context object containing useful objects for requests to use when executing.
     * @return true if an update is necessary; Else false.
     * @throws RetryableException       When error occurs in sync operation indicating a request needs to be retried
     * @throws SkipSyncRequestException When error occurs in sync operation indicating a request needs to be skipped.
     * @throws UnknownShadowException   When shadow not found in the sync table.
     */
    abstract boolean isUpdateNecessary(SyncContext context) throws RetryableException, SkipSyncRequestException,
            UnknownShadowException;

    /**
     * Answer whether the update is already part of the shadow.
     *
     * @param baseDocument the shadow to compare against
     * @param update       the partial update to check
     * @return true if an update to the shadow is needed, otherwise false
     */
    boolean isUpdateNecessary(JsonNode baseDocument, JsonNode update) {
        JsonNode merged = baseDocument.deepCopy();
        JsonMerger.merge(merged.get(SHADOW_DOCUMENT_STATE), update.get(SHADOW_DOCUMENT_STATE));
        return !baseDocument.equals(merged);
    }

    /**
     * Answer whether the update is already part of the shadow.
     *
     * @param baseDocument the shadow to compare against
     * @param update       the partial update to check
     * @return true if an update to the shadow is needed, otherwise false
     * @throws SkipSyncRequestException if an error occurs parsing the shadow documents
     */
    boolean isUpdateNecessary(byte[] baseDocument, JsonNode update) throws SkipSyncRequestException {
        // if the base document is empty, then we need to update
        Optional<JsonNode> existing;
        try {
            existing = JsonUtil.getPayloadJson(baseDocument);
        } catch (IOException e) {
            throw new SkipSyncRequestException(e);
        }

        return existing.map(base -> isUpdateNecessary(base, update)).orElse(true);
    }

    /**
     * Gets the updated version from the payload bytes.
     *
     * @param payload the payload bytes
     * @return an Optional of the updated version.
     */
    protected Optional<Long> getUpdatedVersion(byte[] payload) {
        try {
            ShadowDocument document = new ShadowDocument(payload, false);
            return Optional.of(document.getVersion());
        } catch (InvalidRequestParametersException | IOException e) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e)
                    .log("Unable to get the updated version from the payload");
            return Optional.empty();
        }
    }

    /**
     * Delete the cloud shadow using the IoT Data plane client and then update the sync information.
     *
     * @param syncInformation            The sync information for the thing's shadow.
     * @param cloudShadowDocumentVersion The current cloud document version.
     * @throws RetryableException       if the delete request to cloud encountered a retryable exception.
     * @throws SkipSyncRequestException if the delete request to cloud encountered a skipable exception.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    void handleCloudDelete(@NonNull Long cloudShadowDocumentVersion,
                           @NonNull SyncInformation syncInformation)
            throws RetryableException, SkipSyncRequestException, InterruptedException {
        deleteCloudShadowDocument();
        // Since the local shadow has been deleted, we need get the deleted shadow version from the DAO.
        long localShadowVersion = context.getDao().getDeletedShadowVersion(getThingName(), getShadowName())
                .orElse(syncInformation.getLocalVersion() + 1);
        context.getDao().updateSyncInformation(SyncInformation.builder()
                .localVersion(localShadowVersion)
                // The version number we get in the MQTT message is the version of the cloud shadow that was
                // deleted. But the cloud shadow version after the delete has been incremented by 1. So we have
                // synced that incremented version instead of the deleted cloud shadow version.
                .cloudVersion(cloudShadowDocumentVersion + 1)
                .shadowName(getShadowName())
                .thingName(getThingName())
                .cloudUpdateTime(Instant.now().getEpochSecond())
                .lastSyncedDocument(null)
                .cloudDeleted(true)
                .build());
    }

    /**
     * Delete the cloud document using the IoT Data plane client.
     *
     * @throws RetryableException       if the delete request encountered errors which should be retried.
     * @throws SkipSyncRequestException if the delete request encountered errors which should be skipped.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    private void deleteCloudShadowDocument() throws RetryableException, SkipSyncRequestException, InterruptedException {
        try {
            logger.atInfo()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Deleting cloud shadow document");
            context.getIotDataPlaneClientWrapper().deleteThingShadow(getThingName(), getShadowName());
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException
                | IoTDataPlaneClientCreationException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow delete request");
            throw new RetryableException(e);
        } catch (AbortedException e) {
            LogEventBuilder l = logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName());
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                l.log("Interrupted while deleting cloud shadow");
                throw (InterruptedException) cause;
            }
            l.log("Skipping delete for cloud shadow");
            throw new SkipSyncRequestException(e);
        } catch (SdkServiceException | SdkClientException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow delete request");
            throw new SkipSyncRequestException(e);
        }
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
    long updateCloudDocumentAndGetUpdatedVersion(ObjectNode updateDocument, Optional<Long> cloudDocumentVersion)
            throws SkipSyncRequestException, RetryableException, InterruptedException {
        cloudDocumentVersion.ifPresent(version ->
                updateDocument.set(SHADOW_DOCUMENT_VERSION, new LongNode(version)));
        byte[] payloadBytes = getPayloadBytes(updateDocument);
        Optional<Long> updatedVersion = updateCloudShadowDocument(payloadBytes);
        return updatedVersion.orElse(cloudDocumentVersion.map(version -> version + 1).orElse(1L));
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
     * Update the cloud document using the IoT Data plane client.
     *
     * @param updateDocument The update request payload.
     * @throws ConflictException        if the update request for cloud had a bad version.
     * @throws RetryableException       if the update request encountered errors which should be retried.
     * @throws SkipSyncRequestException if the update request encountered errors which should be skipped.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    private Optional<Long> updateCloudShadowDocument(byte[] updateDocument)
            throws ConflictException, RetryableException, SkipSyncRequestException, InterruptedException {
        UpdateThingShadowResponse response;
        try {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Updating cloud shadow document");

            response = context.getIotDataPlaneClientWrapper().updateThingShadow(getThingName(), getShadowName(),
                    updateDocument);
        } catch (ConflictException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Conflict exception occurred while updating cloud document.");
            throw e;
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException
                | IoTDataPlaneClientCreationException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow update request");
            throw new RetryableException(e);
        } catch (AbortedException e) {
            LogEventBuilder l = logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName());
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                l.log("Interrupted while updating cloud shadow");
                throw (InterruptedException) cause;
            }
            l.log("Skipping update for cloud shadow");
            throw new SkipSyncRequestException(e);
        } catch (SdkServiceException | SdkClientException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow update request");
            throw new SkipSyncRequestException(e);
        }
        return getUpdatedVersion(response.payload().asByteArray());
    }

    /**
     * Update the sync information for the thing's shadow using the update request payload and the current local and
     * cloud version.
     *
     * @param updateDocument       The update request payload.
     * @param localDocumentVersion The current local document version.
     * @param cloudDocumentVersion The current cloud document version.
     * @param cloudUpdateTime      The cloud document latest update time.
     * @throws SkipSyncRequestException if the serialization of the update request payload failed.
     */
    void updateSyncInformation(ObjectNode updateDocument, long localDocumentVersion, long cloudDocumentVersion,
                               long cloudUpdateTime)
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
                .cloudUpdateTime(cloudUpdateTime)
                .lastSyncedDocument(getPayloadBytes(updateDocument))
                .build());
    }

    /**
     * Gets the sync information if it exists or throws a SkipSyncRequestException.
     *
     * @return the sync information for the thing and shadow.
     * @throws SkipSyncRequestException if the sync information does not exist.
     */
    SyncInformation getSyncInformation() throws SkipSyncRequestException {
        Optional<SyncInformation> syncInformation = context.getDao().getShadowSyncInformation(getThingName(),
                getShadowName());

        if (!syncInformation.isPresent()) {
            // This should never happen since we always add a default sync info entry in the DB.
            throw new SkipSyncRequestException("Unable to find sync information");
        }

        return syncInformation.get();
    }

    /**
     * Gets the cloud shadow document using the IoT Data plane client.
     *
     * @return an optional of the cloud shadow document if it existed.
     * @throws RetryableException       if the get request encountered errors which should be retried.
     * @throws SkipSyncRequestException if the get request encountered errors which should be skipped.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    Optional<ShadowDocument> getCloudShadowDocument() throws RetryableException,
            SkipSyncRequestException, InterruptedException {
        logger.atTrace()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .log("Getting cloud shadow document");
        try {
            GetThingShadowResponse getThingShadowResponse = context.getIotDataPlaneClientWrapper()
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
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException
                | IoTDataPlaneClientCreationException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow get request");
            throw new RetryableException(e);
        } catch (AbortedException e) {
            LogEventBuilder l = logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName());
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                l.log("Interrupted while getting cloud shadow");
                throw (InterruptedException) cause;
            }
            l.log("Skipping get for cloud shadow");
            throw new SkipSyncRequestException(e);
        } catch (SdkServiceException | SdkClientException | InvalidRequestParametersException | IOException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not execute cloud shadow get request");
            throw new SkipSyncRequestException(e);
        }
        return Optional.empty();
    }

    /**
     * Delete the local shadow using the request handlers and then update the sync information.
     *
     * @param syncInformation The sync information for the thing's shadow.
     * @throws SkipSyncRequestException if the delete request encountered a skipable exception.
     */
    void handleLocalDelete(@NonNull SyncInformation syncInformation)
            throws SkipSyncRequestException {
        deleteLocalShadowDocument();
        // Since the local shadow has been deleted, we need get the deleted shadow version from the DAO.
        long localShadowVersion = context.getDao().getDeletedShadowVersion(getThingName(), getShadowName())
                .orElse(syncInformation.getLocalVersion() + 1);
        context.getDao().updateSyncInformation(SyncInformation.builder()
                .localVersion(localShadowVersion)
                // If the cloud shadow was deleted, then the last synced version might be 1 higher than the last version
                // that was synced.
                // If the device was offline for a long time and the cloud shadow was deleted multiple times in that
                // period, there is no way to get the correct cloud shadow version. We will eventually get the correct
                // cloud shadow version in the next cloud shadow update.
                .cloudVersion(syncInformation.getCloudVersion() + 1)
                .shadowName(getShadowName())
                .thingName(getThingName())
                .cloudUpdateTime(syncInformation.getCloudUpdateTime())
                .lastSyncedDocument(null)
                .build());
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
     * Gets the latest time the cloud shadow document was updated.
     *
     * @param cloudShadowDocument the cloud shadow document.
     * @return the latest time the cloud shadow was updated if the metadata node exists; Else returns the current time
     */
    long getCloudUpdateTime(ShadowDocument cloudShadowDocument) {
        long cloudUpdateTime = Instant.now().getEpochSecond();
        if (cloudShadowDocument.getMetadata() != null) {
            cloudUpdateTime = cloudShadowDocument.getMetadata().getLatestUpdatedTimestamp();
        }
        return cloudUpdateTime;
    }

    /**
     * Add the version node to the update request payload and then update the local shadow document using that request.
     *
     * @param updateDocument       The update request payload.
     * @param localDocumentVersion The current local document version.
     * @return the updated local document version.
     * @throws SkipSyncRequestException if the update request encountered a skipable exception.
     */
    long updateLocalDocumentAndGetUpdatedVersion(ObjectNode updateDocument, Optional<Long> localDocumentVersion)
            throws SkipSyncRequestException {
        localDocumentVersion.ifPresent(version ->
                updateDocument.set(SHADOW_DOCUMENT_VERSION, new LongNode(version)));
        byte[] payloadBytes = getPayloadBytes(updateDocument);
        Optional<Long> updatedVersion = updateLocalShadowDocument(payloadBytes);
        return updatedVersion.orElse(localDocumentVersion.map(version -> version + 1).orElse(1L));
    }

    /**
     * Update the local shadow document using the update request handler.
     *
     * @param payloadBytes The update request bytes.
     * @throws ConflictError            if the update request for local had a bad version.
     * @throws SkipSyncRequestException if the update request encountered errors which should be skipped.
     */
    private Optional<Long> updateLocalShadowDocument(byte[] payloadBytes) throws ConflictError,
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
        UpdateThingShadowHandlerResponse response;
        try {
            response = getContext().getUpdateHandler().handleRequest(localRequest, ShadowManager.SERVICE_NAME);
        } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError e) {
            throw new SkipSyncRequestException(e);
        }
        return getUpdatedVersion(response.getUpdateThingShadowResponse().getPayload());
    }
}
