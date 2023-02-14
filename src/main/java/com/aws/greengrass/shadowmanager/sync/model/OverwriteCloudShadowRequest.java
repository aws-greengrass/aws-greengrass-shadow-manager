/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;

public class OverwriteCloudShadowRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(OverwriteCloudShadowRequest.class);

    /**
     * Ctr for BaseSyncRequest.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public OverwriteCloudShadowRequest(String thingName, String shadowName) {
        super(thingName, shadowName);
    }

    /**
     * Executes sync request.
     *
     * @param context context object containing useful objects for requests to use when executing.
     * @throws RetryableException       When error occurs in sync operation indicating a request needs to be retried
     * @throws SkipSyncRequestException When error occurs in sync operation indicating a request needs to be skipped.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    @Override
    public void execute(SyncContext context) throws RetryableException, SkipSyncRequestException, InterruptedException {
        super.setContext(context);
        // Synchronizing because shadow sync information is read/written by multiple threads:
        // * during sync request execution (SyncRequest#execute)
        // * during IPC request processing (SyncRequest#isUpdateNecessary(SyncContext)
        //
        // NOTE: checking the sync table during IPC request processing when deciding if a sync request should
        //       be queued is required to prevent excessive full syncs
        //       https://github.com/aws-greengrass/aws-greengrass-shadow-manager/pull/106.
        synchronized (context.getSynchronizeHelper().getThingShadowLock(this)) {
            SyncInformation syncInformation = getSyncInformation();

            Optional<ShadowDocument> localShadowDocument = context.getDao().getShadowThing(
                    getThingName(), getShadowName());

            if (localShadowDocument.isPresent()) {
                if (syncInformation.getLocalVersion() == localShadowDocument.get().getVersion()) {
                    logger.atDebug()
                            .kv(LOG_THING_NAME_KEY, getThingName())
                            .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                            .kv(LOG_LOCAL_VERSION_KEY, syncInformation.getLocalVersion())
                            .log("Not updating cloud shadow "
                                    + "since the local shadow has not changed since the last sync");
                } else {
                    // If the local shadow version is not the same as the last synced local shadow version, go ahead and
                    // update the cloud shadow with the entire local shadow,
                    ObjectNode updateDocument = (ObjectNode) localShadowDocument.get().toJson(false);
                    long cloudDocumentVersion = updateCloudDocumentAndGetUpdatedVersion(
                            updateDocument, Optional.empty());
                    updateSyncInformation(updateDocument, localShadowDocument.get().getVersion(), cloudDocumentVersion,
                            Instant.now().getEpochSecond());
                }
            } else {
                // If the local shadow is not present, then go ahead and delete the cloud shadow.
                handleCloudDelete(syncInformation.getCloudVersion(), syncInformation);
            }
        }
    }

    /**
     * Check if an update is necessary or not.
     *
     * @param context context object containing useful objects for requests to use when executing.
     * @return true.
     */
    @Override
    public boolean isUpdateNecessary(SyncContext context) {
        return true;
    }
}
