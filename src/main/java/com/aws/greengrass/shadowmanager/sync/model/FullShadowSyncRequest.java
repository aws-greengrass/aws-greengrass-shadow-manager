/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowState;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.DataOwner;
import com.aws.greengrass.shadowmanager.util.SyncNodeMerger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_CLOUD_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_LOCAL_VERSION_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.OBJECT_MAPPER;


/**
 * Sync request handling a full sync request for a particular shadow.
 */
public class FullShadowSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(FullShadowSyncRequest.class);

    @Getter
    private List<SyncRequest> mergedRequests;

    /**
     * Ctr for FullShadowSyncRequest.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public FullShadowSyncRequest(String thingName, String shadowName) {
        super(thingName, shadowName);
    }

    public FullShadowSyncRequest(String thingName, String shadowName, SyncRequest... mergedRequests) {
        super(thingName, shadowName);
        if (mergedRequests != null) {
            this.mergedRequests = new ArrayList<>();
            for (SyncRequest req : mergedRequests) {
                if (req instanceof FullShadowSyncRequest) {
                    this.mergedRequests.addAll(((FullShadowSyncRequest) req).getMergedRequests());
                } else {
                    this.mergedRequests.add(req);
                }
            }
        }
    }

    /**
     * Returning true for full sync since we have already figured out that both cloud and local have been updated.
     *
     * @param context the execution context.
     * @return true.
     */
    @Override
    public boolean isUpdateNecessary(SyncContext context) {
        return true;
    }

    private List<SyncRequest> getNecessaryMergedRequests(SyncContext context)
            throws RetryableException, UnknownShadowException, SkipSyncRequestException {
        if (mergedRequests == null) {
            return null;
        }
        List<SyncRequest> necessaryUpdates = new ArrayList<>();
        for (SyncRequest request : mergedRequests) {
            if (request.isUpdateNecessary(context)) {
                necessaryUpdates.add(request);
            }
        }
        return necessaryUpdates;
    }

    /**
     * Executes a full shadow sync.
     *
     * @param context the execution context.
     * @throws RetryableException       if the cloud version is not the same as the version of the shadow on the cloud
     *                                  or if the cloud is throttling the request.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    @Override
    public void execute(SyncContext context) throws RetryableException, SkipSyncRequestException,
            InterruptedException, UnknownShadowException {
        super.setContext(context);

        List<SyncRequest> necessaryMergedUpdates = getNecessaryMergedRequests(context);
        if (necessaryMergedUpdates != null) {
            if (necessaryMergedUpdates.isEmpty()) {
                // TODO log
                return;
            }

            if (necessaryMergedUpdates.size() == 1) {
                SyncRequest request = necessaryMergedUpdates.get(0);
                if (request instanceof CloudUpdateSyncRequest || request instanceof LocalUpdateSyncRequest) {
                    request.execute(context);
                    return;
                }
            }
        }

        SyncInformation syncInformation = getSyncInformation();

        Optional<ShadowDocument> localShadowDocument = context.getDao().getShadowThing(getThingName(), getShadowName());
        Optional<ShadowDocument> cloudShadowDocument = getCloudShadowDocument();
        // If both the local and cloud document does not exist, then update the sync info and return.
        if (!cloudShadowDocument.isPresent() && !localShadowDocument.isPresent()) {
            logger.atInfo()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .kv(LOG_LOCAL_VERSION_KEY, syncInformation.getLocalVersion())
                    .kv(LOG_CLOUD_VERSION_KEY, syncInformation.getCloudVersion())
                    .log("Not performing full sync since both local and cloud versions are already in sync since "
                            + "they don't exist in local and cloud");
            context.getDao().updateSyncInformation(SyncInformation.builder()
                    .localVersion(syncInformation.getLocalVersion())
                    .cloudVersion(syncInformation.getCloudVersion())
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .cloudUpdateTime(syncInformation.getCloudUpdateTime())
                    .lastSyncTime(syncInformation.getLastSyncTime())
                    .lastSyncedDocument(null)
                    .build());
            return;
        }

        long localUpdateTime = Instant.now().getEpochSecond();

        // If only the cloud document does not exist, check if this is the first time we are syncing this shadow or if
        // the local shadow was updated after the last sync. If either of those conditions are true, go ahead and
        // update the cloud with the local document and update the sync information.
        // If not, go ahead and delete the local shadow and update the sync info. That means that the cloud shadow was
        // deleted after the last sync.
        if (!cloudShadowDocument.isPresent()) {
            if (localShadowDocument.get().getMetadata() != null) {
                localUpdateTime = localShadowDocument.get().getMetadata().getLatestUpdatedTimestamp();
            }
            if (isFirstSyncOrShadowUpdatedAfterSync(syncInformation, localUpdateTime)) {
                handleFirstCloudSync(localShadowDocument.get());
            } else {
                handleLocalDelete(syncInformation);
            }
            return;
        }

        long cloudUpdateTime = getCloudUpdateTime(cloudShadowDocument.get());

        // If only the local document does not exist, check if this is the first time we are syncing this shadow or if
        // the cloud shadow was updated after the last sync. If either of those conditions are true, go ahead and
        // update the local with the cloud document and update the sync information.
        // If not, go ahead and delete the cloud shadow and update the sync info. That means that the local shadow was
        // deleted after the last sync.
        if (!localShadowDocument.isPresent()) {
            if (isFirstSyncOrShadowUpdatedAfterSync(syncInformation, cloudUpdateTime)) {
                handleFirstLocalSync(cloudShadowDocument.get(), cloudUpdateTime);
            } else {
                handleCloudDelete(cloudShadowDocument.get().getVersion(), syncInformation);
            }
            return;
        }

        // Get the sync information and check if the versions are same. If the local and cloud versions are same, we
        // don't need to do any sync.
        if (isDocVersionSame(cloudShadowDocument.get(), syncInformation, DataOwner.CLOUD)
                && isDocVersionSame(localShadowDocument.get(), syncInformation, DataOwner.LOCAL)) {
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
            localDocumentVersion = updateLocalDocumentAndGetUpdatedVersion(updateDocument,
                    Optional.of(localDocumentVersion));
        }
        // If the local document version is different from the last sync, that means the cloud document needed
        // some updates. So we go ahead an update the cloud shadow document.
        if (!isDocVersionSame(localShadowDocument.get(), syncInformation, DataOwner.LOCAL)) {
            cloudDocumentVersion = updateCloudDocumentAndGetUpdatedVersion(updateDocument,
                    Optional.of(cloudDocumentVersion));
        }

        if (!isDocVersionSame(localShadowDocument.get(), syncInformation, DataOwner.LOCAL)
                || !isDocVersionSame(cloudShadowDocument.get(), syncInformation, DataOwner.CLOUD)) {
            updateSyncInformation(updateDocument, localDocumentVersion, cloudDocumentVersion, cloudUpdateTime);
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
     * Also check if a shadow was updated after the last sync.
     *
     * @param syncInformation  The sync information for the thing's shadow.
     * @param shadowUpdateTime The time the shadow was updated.
     * @return true if this is the first time the shadow is being sync; Else false.
     */
    private boolean isFirstSyncOrShadowUpdatedAfterSync(@NonNull SyncInformation syncInformation,
                                                        long shadowUpdateTime) {
        return syncInformation.getLastSyncTime() == Instant.EPOCH.getEpochSecond()
                || shadowUpdateTime > syncInformation.getLastSyncTime();
    }

    /**
     * Create the local shadow using the request handlers and then update the sync information.
     *
     * @param cloudShadowDocument The current cloud document.
     * @param cloudUpdateTime     The cloud update timestamp.
     * @throws SkipSyncRequestException if the update request encountered a skipable exception.
     */
    private void handleFirstLocalSync(@NonNull ShadowDocument cloudShadowDocument, long cloudUpdateTime)
            throws SkipSyncRequestException {
        logger.atInfo()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_CLOUD_VERSION_KEY, cloudShadowDocument.getVersion())
                .log("Syncing local shadow for the first time");

        ObjectNode updateDocument = (ObjectNode) cloudShadowDocument.toJson(false);
        long localDocumentVersion = updateLocalDocumentAndGetUpdatedVersion(updateDocument, Optional.empty());
        updateSyncInformation(updateDocument, localDocumentVersion, cloudShadowDocument.getVersion(), cloudUpdateTime);
    }

    /**
     * Create the cloud shadow using the IoT Data plane client and then update the sync information.
     *
     * @param localShadowDocument The current local document.
     * @throws SkipSyncRequestException if the update request to cloud encountered a skipable exception.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    private void handleFirstCloudSync(@NonNull ShadowDocument localShadowDocument)
            throws SkipSyncRequestException, RetryableException, InterruptedException {
        logger.atInfo()
                .kv(LOG_THING_NAME_KEY, getThingName())
                .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                .kv(LOG_LOCAL_VERSION_KEY, localShadowDocument.getVersion())
                .log("Syncing cloud shadow for the first time");
        ObjectNode updateDocument = (ObjectNode) localShadowDocument.toJson(false);
        long cloudDocumentVersion = updateCloudDocumentAndGetUpdatedVersion(updateDocument, Optional.empty());
        updateSyncInformation(updateDocument, localShadowDocument.getVersion(), cloudDocumentVersion,
                Instant.now().getEpochSecond());
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
        } catch (InvalidRequestParametersException | IOException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Could not deserialize last synced shadow document");
            getContext().getDao().updateSyncInformation(SyncInformation.builder()
                    .localVersion(syncInformation.getLocalVersion())
                    .cloudVersion(syncInformation.getCloudVersion())
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .cloudUpdateTime(Instant.now().getEpochSecond())
                    .lastSyncedDocument(null)
                    .build());

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
}
