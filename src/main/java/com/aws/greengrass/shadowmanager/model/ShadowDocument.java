/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.SerializerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

import java.io.IOException;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;

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

    @JsonProperty(SHADOW_DOCUMENT_TIMESTAMP)
    private Long timestamp;

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
     * @throws IOException                       if there was an issue while deserializing the shadow byte array.
     * @throws InvalidRequestParametersException if there was a validation issue while deserializing the shadow doc.
     */
    public ShadowDocument(byte[] documentBytes) throws IOException, InvalidRequestParametersException {
        this(documentBytes, true);
    }

    /**
     * Constructor to create a new shadow document from a byte array.
     *
     * @param documentBytes the byte array containing the shadow information.
     * @param validate      whether to validate the payload or not.
     * @throws IOException                       if there was an issue while deserializing the shadow byte array.
     * @throws InvalidRequestParametersException if there was a validation issue while deserializing the shadow doc.
     */
    public ShadowDocument(byte[] documentBytes, boolean validate)
            throws IOException, InvalidRequestParametersException {
        setFields(documentBytes, validate, null);
    }

    /**
     * Constructor to create a new shadow document from a byte array.
     *
     * @param documentBytes the byte array containing the shadow information.
     * @param version       The shadow document version.
     * @throws IOException if there was an issue while deserializing the shadow byte array.
     */
    public ShadowDocument(byte[] documentBytes, long version) throws IOException {
        setFields(documentBytes, false, version);
    }

    public ShadowDocument(JsonNode node, boolean validate) throws IOException {
        setFields(node, validate, null);
    }

    /**
     * Copy constructor.
     *
     * @param shadowDocument The shadow document to create from.
     */
    public ShadowDocument(ShadowDocument shadowDocument) {
        this(shadowDocument.getState() == null ? null : shadowDocument.getState().deepCopy(),
                shadowDocument.getMetadata() == null ? null : shadowDocument.getMetadata().deepCopy(),
                shadowDocument.getVersion());
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

    private void setFields(byte[] documentBytes, boolean validate, Long versionOverride) throws IOException {
        if (documentBytes == null || documentBytes.length == 0) {
            setFields(null, null, null);
            return;
        }
        setFields(JsonUtil.getPayloadJson(documentBytes).orElse(null), validate, versionOverride);
    }

    private void setFields(JsonNode node, boolean validate, Long versionOverride) {
        if (isNullOrMissing(node)) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidPayloadJsonMessage(""));
        }
        if (validate) {
            JsonUtil.validatePayloadSchema(node);
        }
        setFields(SerializerFactory.getFailSafeJsonObjectMapper().convertValue(node, ShadowDocument.class),
                versionOverride);
    }

    private void setFields(ShadowDocument shadowDocument, Long versionOverride) {
        Long version = versionOverride == null ? shadowDocument.getVersion() : versionOverride;
        setFields(shadowDocument.getState(), shadowDocument.getMetadata(), version);
    }

    private void setFields(ShadowState state, ShadowStateMetadata metadata, Long version) {
        this.state = state == null ? new ShadowState() : state;
        this.metadata = metadata == null ? new ShadowStateMetadata() : metadata;
        this.version = version;
    }

    /**
     * Checks whether the shadow document is new based on the version.
     *
     * @return true if the version is null; Else false.
     */
    public boolean isNewDocument() {
        return this.version == null;
    }

    /**
     * Updates the documents node based on the current version of the shadow document and the requested update.
     *
     * @param updateDocumentRequest The JSON containing the shadow document update request.
     * @return the metadata of the updated state
     */
    public JsonNode update(JsonNode updateDocumentRequest) {
        JsonNode updatedStateNode = updateDocumentRequest.get(SHADOW_DOCUMENT_STATE);

        this.state.update(updatedStateNode);
        JsonNode patchMetadata = this.metadata.update(updatedStateNode, this.state);
        // Incrementing the version here since we are creating a new version of the shadow document.
        this.version = this.version == null ? 1 : this.version + 1;

        return patchMetadata;
    }

    /**
     * Converts the class to its JSON representation.
     *
     * @param withVersion whether or not to add the version node to the JSON.
     * @return a JSON node containing the shadow document.
     */
    public JsonNode toJson(boolean withVersion) {
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        result.set(SHADOW_DOCUMENT_STATE, this.state.toJson());
        if (this.metadata != null) {
            result.set(SHADOW_DOCUMENT_METADATA, this.metadata.toJson());
        }
        if (withVersion) {
            result.set(SHADOW_DOCUMENT_VERSION, new LongNode(this.version));
        }

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
        final JsonNode deltaMetadata = metadata.getDeltaMetadata(delta);
        return Optional.of(new Pair<>(delta, deltaMetadata));
    }
}
