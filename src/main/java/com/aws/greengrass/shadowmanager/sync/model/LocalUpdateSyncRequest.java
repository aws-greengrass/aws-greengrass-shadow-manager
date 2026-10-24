/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_UPDATED_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;

/**
 * Sync request to update locally stored shadow.
 */
public class LocalUpdateSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(LocalUpdateSyncRequest.class);

    private final List<byte[]> updateDocuments = new ArrayList<>();

    /**
     * Ctr for LocalUpdateSyncRequest.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument The update document to update the local shadow
     */
    public LocalUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  byte[] updateDocument) {
        super(thingName, shadowName);
        if (updateDocument != null) {
            this.updateDocuments.add(updateDocument);
        }
    }

    /**
     * Gets the cloud updates of this request, in version order.
     *
     * @return the update documents.
     */
    public List<byte[]> getUpdateDocuments() {
        return Collections.unmodifiableList(updateDocuments);
    }

    /**
     * Merge the sync requests together. The cloud updates are kept in version order and applied one by one, because
     * two updates can't always be written as one.
     *
     * @param other the newer request to merge
     * @throws IOException if unable to read the version of an update document.
     */
    public void merge(LocalUpdateSyncRequest other) throws IOException {
        updateDocuments.addAll(other.updateDocuments);
        final Map<byte[], Long> versions = new IdentityHashMap<>();
        for (byte[] document : updateDocuments) {
            final Optional<JsonNode> json = JsonUtil.getPayloadJson(document);
            versions.put(document, json.filter(JsonUtil::hasVersion).map(JsonUtil::getVersion).orElse(Long.MAX_VALUE));
        }
        updateDocuments.sort(Comparator.comparing(versions::get));
    }

    @Override
    public void execute(SyncContext context) throws SkipSyncRequestException, ConflictError,
            UnknownShadowException {
        for (byte[] updateDocument : updateDocuments) {
            execute(context, updateDocument);
        }
    }

    private void execute(SyncContext context, byte[] updateDocument) throws SkipSyncRequestException, ConflictError,
            UnknownShadowException {
        ShadowDocument shadowDocument = parse(updateDocument);

        SyncInformation currentSyncInformation = context.getDao()
                .getShadowSyncInformation(getThingName(), getShadowName())
                .orElseThrow(() -> new UnknownShadowException("Shadow not found in sync table"));

        if (!isUpdateNecessary(context, shadowDocument, currentSyncInformation)) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Local shadow already contains update payload. No sync is necessary");
            return;
        }

        long cloudUpdateVersion = shadowDocument.getVersion();
        long currentCloudVersion = currentSyncInformation.getCloudVersion();
        long currentLocalVersion = currentSyncInformation.getLocalVersion();

        // Expected sequential cloud update, routing update to local shadow
        if (cloudUpdateVersion == currentCloudVersion + 1) {
            try {
                UpdateThingShadowRequest request = new UpdateThingShadowRequest();
                request.setThingName(getThingName());
                request.setShadowName(getShadowName());
                request.setPayload(JsonUtil.getPayloadBytes(shadowDocument.toJson(false)));

                UpdateThingShadowHandlerResponse response =
                        context.getUpdateHandler().handleRequest(request, SHADOW_MANAGER_NAME);

                long updateTime = Instant.now().getEpochSecond();
                long localUpdatedVersion = getUpdatedVersion(response.getUpdateThingShadowResponse().getPayload())
                        .orElse(currentLocalVersion + 1);
                context.getDao().updateSyncInformation(SyncInformation.builder()
                        .thingName(getThingName())
                        .shadowName(getShadowName())
                        .lastSyncedDocument(getCloudDocument(currentSyncInformation.getLastSyncedDocument(),
                                shadowDocument, response.getCurrentDocument()))
                        .cloudUpdateTime(updateTime)
                        .localVersion(localUpdatedVersion)
                        .cloudVersion(cloudUpdateVersion)
                        .lastSyncTime(updateTime)
                        .cloudDeleted(false)
                        .build());
                logger.atDebug()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .kv(LOG_LOCAL_VERSION_KEY, localUpdatedVersion)
                        .kv(LOG_CLOUD_VERSION_KEY, cloudUpdateVersion)
                        .log("Successfully updated local shadow");
            } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError
                     | IOException e) {
                logger.atDebug()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .kv(LOG_LOCAL_VERSION_KEY, currentLocalVersion)
                        .kv(LOG_CLOUD_VERSION_KEY, cloudUpdateVersion)
                        .log("Skipping update for local shadow");
                throw new SkipSyncRequestException(e);
            }

            // edge case where might have missed sync update from cloud
        } else if (cloudUpdateVersion > currentCloudVersion + 1) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, currentSyncInformation.getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, currentSyncInformation.getCloudVersion())
                    .kv(LOG_UPDATED_CLOUD_VERSION_KEY, cloudUpdateVersion)
                    .log("Unable to update local shadow since some update(s) were missed from the cloud");
            throw new ConflictError("Missed update(s) from the cloud");
        }
    }

    /**
     * Gets the cloud document after the update: the last synced document with the update applied. The local document
     * can have changes that aren't in the cloud yet, so it is only used when there is no last synced document.
     */
    private static byte[] getCloudDocument(byte[] lastSyncedDocument, ShadowDocument update, byte[] localDocument)
            throws IOException {
        if (lastSyncedDocument == null) {
            return localDocument;
        }
        final ShadowDocument cloud = new ShadowDocument(lastSyncedDocument, false);
        cloud.update(update.toJson(false));
        final ObjectNode document = JsonUtil.OBJECT_MAPPER.createObjectNode();
        document.set(SHADOW_DOCUMENT_STATE, cloud.getState().toJson());
        return JsonUtil.getPayloadBytes(document);
    }

    private static ShadowDocument parse(byte[] updateDocument) throws SkipSyncRequestException {
        try {
            return new ShadowDocument(updateDocument);
        } catch (IOException | InvalidRequestParametersException e) {
            throw new SkipSyncRequestException(e);
        }
    }

    /**
     * Checks if it is necessary to perform an update to the local shadow.
     *
     * @param context the execution context.
     * @return true if local shadow update is necessary; Else false.
     * @throws SkipSyncRequestException if unable to deserialize cloud shadow update payload,
     */
    @Override
    boolean isUpdateNecessary(SyncContext context) throws SkipSyncRequestException, UnknownShadowException {
        for (byte[] updateDocument : updateDocuments) {
            ShadowDocument shadowDocument = parse(updateDocument);
            //TODO: store this information in a return object to avoid unnecessary calls to DAO.
            SyncInformation currentSyncInformation = context.getDao()
                    .getShadowSyncInformation(getThingName(), getShadowName())
                    .orElseThrow(() -> new UnknownShadowException("Shadow not found in sync table"));
            if (isUpdateNecessary(context, shadowDocument, currentSyncInformation)) {
                return true;
            }
        }
        return false;
    }

    private boolean isUpdateNecessary(SyncContext context, ShadowDocument shadowDocument,
                                      SyncInformation currentSyncInformation) {
        Optional<ShadowDocument> currentLocal = context.getDao().getShadowThing(getThingName(), getShadowName());
        if (currentLocal.isPresent() && !isUpdateNecessary(currentLocal.get().toJson(false),
                shadowDocument.toJson(false))) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Local shadow already contains update payload. No sync is necessary");
            updateSyncInformationVersion(context, shadowDocument, currentSyncInformation);
            return false;
        }

        return true;
    }

    private void updateSyncInformationVersion(SyncContext context, ShadowDocument shadowDocument,
                                              SyncInformation currentSyncInformation) {
        if (currentSyncInformation.getCloudVersion() < shadowDocument.getVersion()) {
            try {
                long updateTime = Instant.now().getEpochSecond();
                context.getDao().updateSyncInformation(SyncInformation.builder()
                        .thingName(getThingName())
                        .shadowName(getShadowName())
                        .lastSyncedDocument(currentSyncInformation.getLastSyncedDocument())
                        .cloudUpdateTime(updateTime)
                        .localVersion(currentSyncInformation.getLocalVersion())
                        .cloudVersion(shadowDocument.getVersion())
                        .lastSyncTime(updateTime)
                        .cloudDeleted(false)
                        .build());
            } catch (ShadowManagerDataException e) {
                logger.atError()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .kv(LOG_LOCAL_VERSION_KEY, currentSyncInformation.getLocalVersion())
                        .kv(LOG_CLOUD_VERSION_KEY, currentSyncInformation.getCloudVersion())
                        .cause(e).log("Failed to update sync table after updating cloud shadow");
            }
        }
    }
}
