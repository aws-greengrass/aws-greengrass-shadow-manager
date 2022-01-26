/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.LogEventBuilder;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
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
     * @throws RetryableException       if the cloud version is not the same as the version of the shadow on the cloud
     *                                  or if the cloud is throttling the request.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    @Override
    public void execute(SyncContext context)
            throws RetryableException, SkipSyncRequestException, UnknownShadowException, InterruptedException {

        Optional<SyncInformation> syncInformation = context.getDao().getShadowSyncInformation(getThingName(),
                getShadowName());
        if (!syncInformation.isPresent()) {
            throw new UnknownShadowException("Shadow not found in sync table");
        }

        if (syncInformation.get().isCloudDeleted()) {
            logger.atInfo()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.get().getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.get().getCloudVersion())
                    .log("Not deleting cloud shadow document since it is already deleted");
            return;
        }

        try {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.get().getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.get().getCloudVersion())
                    .log("Deleting cloud shadow document");

            context.getIotDataPlaneClientWrapper().deleteThingShadow(getThingName(), getShadowName());
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            throw new RetryableException(e);
        } catch (AbortedException e) {
            LogEventBuilder l = logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.get().getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.get().getCloudVersion());
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                l.log("Interrupted while deleting cloud shadow");
                throw (InterruptedException) cause;
            }
            l.log("Skipping delete for cloud shadow");
            throw new SkipSyncRequestException(e);
        } catch (SdkServiceException | SdkClientException e) {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.get().getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.get().getCloudVersion())
                    .log("Skipping delete for cloud shadow");
            throw new SkipSyncRequestException(e);
        }

        long cloudDeletedVersion = syncInformation.map(SyncInformation::getCloudVersion).orElse(0L) + 1;
        long localDeletedVersion = context.getDao().getDeletedShadowVersion(getThingName(), getShadowName())
                .orElse(syncInformation.map(SyncInformation::getLocalVersion).orElse(0L) + 1);
        try {
            // Since the local shadow has been deleted, we need get the deleted shadow version from the DAO.
            context.getDao().updateSyncInformation(SyncInformation.builder()
                    .lastSyncedDocument(null)
                    // After the cloud shadow has been deleted, the version on cloud gets incremented. So we have to
                    // increment the synced cloud shadow version.
                    .cloudVersion(cloudDeletedVersion)
                    .localVersion(localDeletedVersion)
                    .cloudDeleted(true)
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .build());
        } catch (ShadowManagerDataException e) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, localDeletedVersion)
                    .kv(LOG_CLOUD_VERSION_KEY, cloudDeletedVersion)
                    .cause(e).log("Failed to update sync table after deleting cloud shadow");
        }
    }

    /**
     * Returning true for delete since multiple deletes will never happen from the cloud or device.
     *
     * @param context the execution context.
     * @return true.
     */
    @Override
    public boolean isUpdateNecessary(SyncContext context) {
        return true;
    }
}
