/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.logging.api.LogEventBuilder;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.model.Constants;
import com.aws.greengrass.shadowmanager.model.LogEvents;
import com.aws.greengrass.shadowmanager.sync.model.BaseSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteCloudShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteLocalShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import javax.inject.Inject;


/**
 * Merge requests that can be combined. Falls back to FullSync if requests cannot be combined in a
 * meaningful way.
 */
public class RequestMerger {
    private static final Logger logger = LogManager.getLogger(RequestMerger.class);

    private final DirectionWrapper direction;

    @Inject
    public RequestMerger(DirectionWrapper direction) {
        this.direction = direction;
    }

    /**
     * Merge two requests into one.
     *
     * @param oldValue a request to merge.
     * @param value    a request to merge.
     * @return a merged request
     */
    @SuppressWarnings({"PMD.EmptyIfStmt"})
    @SuppressFBWarnings("UCF_USELESS_CONTROL_FLOW")
    public SyncRequest merge(SyncRequest oldValue, SyncRequest value) {
        LogEventBuilder logEvent = logger.atDebug(LogEvents.SYNC.code())
                .addKeyValue(Constants.LOG_THING_NAME_KEY, oldValue.getThingName())
                .addKeyValue(Constants.LOG_SHADOW_NAME_KEY, oldValue.getShadowName());

        if (oldValue instanceof FullShadowSyncRequest || oldValue instanceof OverwriteCloudShadowRequest
                || oldValue instanceof OverwriteLocalShadowRequest) {
            return returnRequestBasedOnDirection(oldValue, value, logEvent);
        }
        if (value instanceof FullShadowSyncRequest || value instanceof OverwriteCloudShadowRequest
                || value instanceof OverwriteLocalShadowRequest) {
            return returnRequestBasedOnDirection(value, oldValue, logEvent);
        }

        if (oldValue instanceof CloudUpdateSyncRequest && value instanceof CloudUpdateSyncRequest) {
            logEvent.log("Merge cloud update requests");
            ((CloudUpdateSyncRequest) oldValue).merge((CloudUpdateSyncRequest) value);
            return oldValue;
        } else if (oldValue instanceof LocalUpdateSyncRequest && value instanceof LocalUpdateSyncRequest) {
            try {
                logEvent.log("Merge local update requests");
                ((LocalUpdateSyncRequest) oldValue).merge((LocalUpdateSyncRequest) value);
                return oldValue;
            } catch (IOException e) {
                logger.atWarn(LogEvents.SYNC.code())
                        .addKeyValue(Constants.LOG_THING_NAME_KEY, oldValue.getThingName())
                        .addKeyValue(Constants.LOG_SHADOW_NAME_KEY, oldValue.getShadowName())
                        .cause(e)
                        .log("Unable to merge local update requests");
            }
        } else if ((oldValue instanceof CloudUpdateSyncRequest || oldValue instanceof LocalUpdateSyncRequest)
                && (value instanceof CloudDeleteSyncRequest || value instanceof LocalDeleteSyncRequest)) {
            // update followed by delete, just send the delete - no matter the direction
            logEvent.log("Merge new delete shadow request");
            return value;
        } else if ((oldValue instanceof CloudDeleteSyncRequest || oldValue instanceof LocalDeleteSyncRequest)
                && (value instanceof CloudUpdateSyncRequest || value instanceof LocalUpdateSyncRequest)) {
            // delete followed by update - but we haven't processed delete yet. Prioritizing deletes otherwise it may
            // be impossible to intentionally sync deletes
            logEvent.log("Merge with older delete shadow request. Discarding update and prioritizing delete");
            return oldValue;
        } else if (oldValue instanceof CloudDeleteSyncRequest && value instanceof CloudDeleteSyncRequest
                || oldValue instanceof LocalDeleteSyncRequest && value instanceof LocalDeleteSyncRequest) {
            // this should never happen (multiple local or multiple cloud deletes) but it can safely return either value
            logEvent.log("Merge redundant delete requests");
            return oldValue;
        } else if (oldValue instanceof CloudDeleteSyncRequest && value instanceof LocalDeleteSyncRequest
                || oldValue instanceof LocalDeleteSyncRequest && value instanceof CloudDeleteSyncRequest) {
            logEvent.log("Merge simultaneous deletes for shadow from local and cloud");
        } else if (oldValue instanceof CloudUpdateSyncRequest && value instanceof LocalUpdateSyncRequest
                || oldValue instanceof LocalUpdateSyncRequest && value instanceof CloudUpdateSyncRequest) {
            logger.atDebug(LogEvents.SYNC.code())
                    .addKeyValue(Constants.LOG_THING_NAME_KEY, oldValue.getThingName())
                    .addKeyValue(Constants.LOG_SHADOW_NAME_KEY, oldValue.getShadowName())
                    .log("Received bi-directional updates. Converting to a full shadow sync request");
        }

        return returnRequestBasedOnDirection(value, oldValue, logEvent);
    }

    private BaseSyncRequest returnRequestBasedOnDirection(SyncRequest value, SyncRequest otherValue,
                                                          LogEventBuilder logEvent) {
        switch (direction.get()) {
            case DEVICE_TO_CLOUD:
                logEvent.log("Creating overwrite cloud shadow sync request");
                // Instead of a partial update, an overwrite cloud shadow sync request will force the device to
                // overwrite the cloud shadow
                return new OverwriteCloudShadowRequest(value.getThingName(), value.getShadowName());
            case CLOUD_TO_DEVICE:
                logEvent.log("Creating overwrite local shadow sync request");
                // Instead of a partial update, an overwrite local shadow sync request will force the cloud to
                // overwrite the local shadow
                return new OverwriteLocalShadowRequest(value.getThingName(), value.getShadowName());
            case BETWEEN_DEVICE_AND_CLOUD:
            default:
                logEvent.log("Creating full shadow sync request");
                // Instead of a partial update, a full sync request will force a get of the latest local
                // and remote shadows
                return FullShadowSyncRequest.fromMerge(value.getThingName(), value.getShadowName(),
                        this, value, otherValue);
        }
    }
}
