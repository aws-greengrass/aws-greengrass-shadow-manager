/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

import java.io.IOException;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

/**
 * Class for managing operations on the Shadow Document.
 */
@Getter
public class ShadowDocument {
    @JsonProperty(value = SHADOW_DOCUMENT_STATE, required = true)
    private ShadowState state;

    @JsonProperty(SHADOW_DOCUMENT_METADATA)
    private ShadowStateMetadata metadata;

    @JsonProperty(SHADOW_DOCUMENT_VERSION)
    private Long version;

    /**
     * Constructor needed for deserializing from JSON node.
     */
    public ShadowDocument() {
        this(null, null, null);
    }

    /**
     * Constructor to create a new shadow document from a byte array.
     *
     * @param documentBytes the byte array containing the shadow information.
     * @throws IOException if there was an issue while deserializing the shadow byte array.
     */
    public ShadowDocument(byte[] documentBytes) throws IOException {
        if (documentBytes.length == 0) {
            return;
        }
        ShadowDocument shadowDocument = JsonUtil.OBJECT_MAPPER.readValue(documentBytes, ShadowDocument.class);
        setFields(shadowDocument.getState(), shadowDocument.getMetadata(), shadowDocument.getVersion());
    }

    /**
     * Constructor for creating new shadow document.
     *
     * @param state    The state of the shadow document.
     * @param metadata The metadata of the shadow document.
     * @param version  The version of the shadow document.
     */
    public ShadowDocument(final ShadowState state, final ShadowStateMetadata metadata, Long version) {
        setFields(state, metadata, version);
    }

    private void setFields(ShadowState state, ShadowStateMetadata metadata, Long version) {
        this.state = state == null ? new ShadowState() : state;
        this.metadata = metadata == null ? new ShadowStateMetadata() : metadata;
        this.version = version;
    }

    /**
     * Checks whether or not the shadow document is new based on the version.
     *
     * @return true if the version is null; Else false.
     */
    public boolean isNewDocument() {
        return this.version == null;
    }

    /**
     * Calculates the documents node based on the current version of the shadow document and the new updated
     * version of the shadow document sent in the update request.
     *
     * @param updateDocumentRequest The JSON containing the shadow document update request.
     * @return the new updated shadow document.
     */
    public ShadowDocument createNewMergedDocument(JsonNode updateDocumentRequest) {
        ShadowDocument updatedShadowDocument = new ShadowDocument(
                this.getState() == null ? null : this.getState().deepCopy(),
                this.getMetadata() == null ? null : this.getMetadata().deepCopy(),
                this.getVersion() == null ? 0 : this.getVersion() + 1);
        JsonNode updatedStateNode = updateDocumentRequest.get(SHADOW_DOCUMENT_STATE);

        updatedShadowDocument.getState().update(updatedStateNode);
        if (updatedShadowDocument.getMetadata() != null) {
            updatedShadowDocument.getMetadata().update(updatedStateNode, updatedShadowDocument.getState());
        }

        return updatedShadowDocument;
    }

    /**
     * Converts the class to its JSON representation.
     *
     * @return a JSON node containing the shadow document.
     */
    public JsonNode toJson() {
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        result.set(SHADOW_DOCUMENT_STATE, this.state.toJson());
        if (this.metadata != null) {
            result.set(SHADOW_DOCUMENT_METADATA, this.metadata.toJson());
        }
        result.set(SHADOW_DOCUMENT_VERSION, new LongNode(this.version));

        return result;
    }

    /**
     * Calculates the delta node based on the current version of the shadow document's desired and reported state. Also
     * calculates the delta in the metadata node.
     *
     * @return an optional value of the delta node.
     */
    public Optional<Pair<JsonNode, JsonNode>> getDelta() {
        final JsonNode delta = state.getDelta();
        if (delta == null) {
            return Optional.empty();
        }
        //TODO: Add the metadata node here as well and return that.
        final JsonNode deltaMetadata = metadata.getDeltaMetadata(delta);
        return Optional.of(new Pair<>(delta, deltaMetadata));
    }
}
