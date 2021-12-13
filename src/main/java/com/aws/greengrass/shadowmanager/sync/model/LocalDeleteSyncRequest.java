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
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.DeleteThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ResourceNotFoundError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;

import java.io.IOException;
import java.time.Instant;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_DELETED_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;


/**
 * Sync request to delete a locally stored shadow.
 */
public class LocalDeleteSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(LocalDeleteSyncRequest.class);

    private final byte[] deletePayload;


    /**
     * Ctr for LocalDeleteSyncRequest.
     *
     * @param thingName     The thing name associated with the sync shadow update
     * @param shadowName    The shadow name associated with the sync shadow update
     * @param deletePayload Delete response payload containing the deleted shadow version
     */
    public LocalDeleteSyncRequest(String thingName, String shadowName, byte[] deletePayload) {
         super(thingName,shadowName);
        this.deletePayload = deletePayload;
    }

    @Override
    public void execute(SyncContext context) throws SkipSyncRequestException, UnknownShadowException {
        Long deletedCloudVersion;
        try {
            deletedCloudVersion = JsonUtil.getPayloadJson(deletePayload)
                    .filter(s -> s.has(SHADOW_DOCUMENT_VERSION) && s.get(SHADOW_DOCUMENT_VERSION).isIntegralNumber())
                    .map(s -> s.get(SHADOW_DOCUMENT_VERSION).asLong())
                    .orElseThrow(() -> new SkipSyncRequestException("Invalid delete payload from the cloud"));
        } catch (IOException e) {
            throw new SkipSyncRequestException(e);
        }

        SyncInformation syncInformation = context.getDao().getShadowSyncInformation(getThingName(), getShadowName())
                .orElseThrow(() -> new UnknownShadowException("Shadow not found in sync table"));

        long currentCloudVersion = syncInformation.getCloudVersion();

        if (deletedCloudVersion >= currentCloudVersion) {
            try {
                DeleteThingShadowRequest request = new DeleteThingShadowRequest();
                request.setThingName(getThingName());
                request.setShadowName(getShadowName());
                context.getDeleteHandler().handleRequest(request, SHADOW_MANAGER_NAME);

                long updateTime = Instant.now().getEpochSecond();
                long localShadowVersion = context.getDao().getDeletedShadowVersion(getThingName(), getShadowName())
                        .orElse(syncInformation.getLocalVersion() + 1);
                context.getDao().updateSyncInformation(SyncInformation.builder()
                        .thingName(getThingName())
                        .shadowName(getShadowName())
                        .lastSyncedDocument(null)
                        .cloudUpdateTime(updateTime)
                        .localVersion(localShadowVersion)
                        // The version number we get in the MQTT message is the version of the cloud shadow that was
                        // deleted. But the cloud shadow version after the delete has been incremented by 1. So we have
                        // synced that incremented version instead of the deleted cloud shadow version.
                        .cloudVersion(deletedCloudVersion + 1)
                        .lastSyncTime(updateTime)
                        .cloudDeleted(true)
                        .build());
                logger.atDebug()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .kv(LOG_LOCAL_VERSION_KEY, localShadowVersion)
                        .kv(LOG_CLOUD_VERSION_KEY, deletedCloudVersion + 1)
                        .log("Successfully deleted local shadow");

            } catch (ResourceNotFoundError e) {
                logger.atInfo()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .kv(LOG_LOCAL_VERSION_KEY, syncInformation.getLocalVersion())
                        .kv(LOG_CLOUD_VERSION_KEY, syncInformation.getCloudVersion())
                        .kv(LOG_DELETED_CLOUD_VERSION_KEY, deletedCloudVersion)
                        .setCause(e)
                        .log("Attempted to delete shadow that was already deleted");
            } catch (ShadowManagerDataException | UnauthorizedError | InvalidArgumentsError | ServiceError e) {
                logger.atDebug()
                        .kv(LOG_THING_NAME_KEY, getThingName())
                        .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                        .kv(LOG_LOCAL_VERSION_KEY, syncInformation.getLocalVersion())
                        .kv(LOG_CLOUD_VERSION_KEY, deletedCloudVersion)
                        .log("Skipping delete for local shadow");
                throw new SkipSyncRequestException(e);
            }
        // missed cloud update(s) need to do full sync
        } else {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.getCloudVersion())
                    .kv(LOG_DELETED_CLOUD_VERSION_KEY, deletedCloudVersion)
                    .log("Unable to delete local shadow since some update(s) were missed from the cloud");
            throw new ConflictError("Missed update(s) from the cloud");
        }
    }
}
