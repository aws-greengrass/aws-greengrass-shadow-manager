/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.shadowmanager.util.JsonMerger;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;

/**
 * Base class for all sync requests.
 */
public abstract class BaseSyncRequest extends ShadowRequest implements SyncRequest {
    private static final Logger logger = LogManager.getLogger(BaseSyncRequest.class);

    /**
     * Ctr for BaseSyncRequest.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public BaseSyncRequest(String thingName,
                           String shadowName) {
        super(thingName, shadowName);
    }

    /**
     * Answer whether the update is already part of the shadow.
     *
     * @param baseDocument the shadow to compare against
     * @param update       the partial update to check
     * @return true if an update to the shadow is needed, otherwise false
     */
    protected boolean isUpdateNecessary(JsonNode baseDocument, JsonNode update) {
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
    protected boolean isUpdateNecessary(byte[] baseDocument, JsonNode update) throws SkipSyncRequestException {
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
}
