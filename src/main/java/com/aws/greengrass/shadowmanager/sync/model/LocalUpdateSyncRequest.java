/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonMerger;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;

/**
 * Sync request to update locally stored shadow.
 */
public class LocalUpdateSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(LocalUpdateSyncRequest.class);

    @Getter
    private byte[] updateDocument;

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
        this.updateDocument = updateDocument;
    }

    /**
     * Merge the sync requests together.
     *
     * @param other the newer request to merge
     * @throws IOException if unable to serialize the update document payload bytes.
     */
    public void merge(LocalUpdateSyncRequest other) throws IOException {
        Optional<JsonNode> oldValueJson = JsonUtil.getPayloadJson(updateDocument);
        Optional<JsonNode> newValueJson = JsonUtil.getPayloadJson(other.getUpdateDocument());
        if (!oldValueJson.isPresent() && newValueJson.isPresent()) {
            updateDocument = other.getUpdateDocument();
            return;
        }
        if (!newValueJson.isPresent()) {
            return;
        }
        JsonMerger.merge(oldValueJson.get(), newValueJson.get());
        updateDocument = JsonUtil.getPayloadBytes(oldValueJson.get());
    }

    @Override
    public void execute(SyncContext context) throws SkipSyncRequestException, ConflictError,
            UnknownShadowException {
        ShadowDocument shadowDocument;
        try {
            shadowDocument = new ShadowDocument(updateDocument);
        } catch (IOException e) {
            throw new SkipSyncRequestException(e);
        }

        Optional<ShadowDocument> currentLocal = context.getDao().getShadowThing(getThingName(), getShadowName());
        if (currentLocal.isPresent() && !isUpdateNecessary(currentLocal.get().toJson(false),
                shadowDocument.toJson(false))) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Local shadow already contains update payload. No sync is necessary");
            return;
        }

        SyncInformation currentSyncInformation = context.getDao()
                .getShadowSyncInformation(getThingName(), getShadowName())
                .orElseThrow(() -> new UnknownShadowException("Shadow not found in sync table"));

        long cloudUpdateVersion = shadowDocument.getVersion();
        long currentCloudVersion = currentSyncInformation.getCloudVersion();
        long currentLocalVersion = currentSyncInformation.getLocalVersion();

        // Expected sequential cloud update, routing update to local shadow
        if (cloudUpdateVersion == currentCloudVersion + 1) {
            try {
                updateRequestWithLocalVersion(currentLocalVersion);

                UpdateThingShadowRequest request = new UpdateThingShadowRequest();
                request.setThingName(getThingName());
                request.setShadowName(getShadowName());
                request.setPayload(JsonUtil.getPayloadBytes(shadowDocument.toJson(false)));

                UpdateThingShadowHandlerResponse response =
                        context.getUpdateHandler().handleRequest(request, SHADOW_MANAGER_NAME);

                byte[] updatedDocument = response.getCurrentDocument();
                long updateTime = Instant.now().getEpochSecond();
                context.getDao().updateSyncInformation(SyncInformation.builder()
                        .thingName(getThingName())
                        .shadowName(getShadowName())
                        .lastSyncedDocument(updatedDocument)
                        .cloudUpdateTime(updateTime)
                        .localVersion(getUpdatedVersion(response.getUpdateThingShadowResponse().getPayload())
                                .orElse(currentLocalVersion + 1))
                        .cloudVersion(cloudUpdateVersion)
                        .lastSyncTime(updateTime)
                        .cloudDeleted(false)
                        .build());
            } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError
                    | IOException e) {
                throw new SkipSyncRequestException(e);
            }

            // edge case where might have missed sync update from cloud
        } else if (cloudUpdateVersion > currentCloudVersion + 1) {
            throw new ConflictError("Missed update(s) from the cloud");
        }
    }

    private void updateRequestWithLocalVersion(long updatedLocalVersion) throws IOException {
        Optional<JsonNode> updateDocumentRequest = JsonUtil.getPayloadJson(updateDocument);
        if (updateDocumentRequest.isPresent()) {
            ((ObjectNode) updateDocumentRequest.get()).set(SHADOW_DOCUMENT_VERSION,
                    new LongNode(updatedLocalVersion));
            this.updateDocument = JsonUtil.getPayloadBytes(updateDocumentRequest.get());
        }
    }

}
