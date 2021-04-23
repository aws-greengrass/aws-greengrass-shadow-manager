/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.FullSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
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

    @NonNull
    UpdateThingShadowRequestHandler updateThingShadowRequestHandler;

    private byte[] updateDocument;

    /**
     * Ctr for LocalUpdateSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param updateDocument              The update document to update the local shadow
     * @param dao                         Local shadow database management
     * @param updateThingShadowRequestHandler Reference to the UpdateThingShadow IPC Handler
     */
    public LocalUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  byte[] updateDocument,
                                  ShadowManagerDAO dao,
                                  UpdateThingShadowRequestHandler updateThingShadowRequestHandler) {
        super(thingName, shadowName, dao);
        this.updateDocument = updateDocument;
        this.updateThingShadowRequestHandler = updateThingShadowRequestHandler;
    }

    /**
     * Main execution thread for syncing cloud update to local shadow.
     */
    @Override
    public void execute() throws FullSyncRequestException, SyncException, SkipSyncRequestException {
        ShadowDocument shadowDocument;
        try {
            shadowDocument = new ShadowDocument(updateDocument);
        } catch (IOException e) {
            logger.atError()
                    .setEventType(LogEvents.LOCAL_UPDATE_SYNC_REQUEST.code())
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .setCause(e)
                    .log("Unable to parse update payload from cloud.");
            throw new SkipSyncRequestException(e);
        }

        SyncInformation currentSyncInformation = dao.getShadowSyncInformation(getThingName(), getShadowName())
                .orElseThrow(() -> new FullSyncRequestException("Missing sync information. A full sync needed "
                        + "to reconcile shadow."));

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
                request.setPayload(updateDocument);

                // TODO: verify service name is authorized
                UpdateThingShadowHandlerResponse updateThingShadowHandlerResponse =
                        updateThingShadowRequestHandler.handleRequest(request, SHADOW_MANAGER_NAME);

                byte[] updatedDocument = updateThingShadowHandlerResponse.getCurrentDocument();
                long updateTime = Instant.now().getEpochSecond();
                dao.updateSyncInformation(SyncInformation.builder()
                        .thingName(getThingName())
                        .shadowName(getShadowName())
                        .lastSyncedDocument(updatedDocument)
                        .cloudUpdateTime(updateTime)
                        .localVersion(currentLocalVersion + 1)
                        .cloudVersion(cloudUpdateVersion)
                        .lastSyncTime(updateTime)
                        .cloudDeleted(false)
                        .build());

            } catch (ConflictError e) {
                logger.atWarn()
                        .setEventType(LogEvents.LOCAL_UPDATE_SYNC_REQUEST.code())
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .setCause(e)
                        .log("Conflict error occurred when syncing local shadow");
                throw new FullSyncRequestException(e);
            } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError
                    | IOException e) {
                logger.atError()
                        .setEventType(LogEvents.LOCAL_UPDATE_SYNC_REQUEST.code())
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .setCause(e)
                        .log("Failed to execute local update sync request");
                throw new SkipSyncRequestException(e);
            }

        // edge case where might have missed sync update from cloud
        } else if (cloudUpdateVersion > currentCloudVersion + 1) {
            throw new FullSyncRequestException("Missed cloud updates, will need a full sync");
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
