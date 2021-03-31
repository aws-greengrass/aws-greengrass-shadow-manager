package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_CLIENT_TOKEN;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_METADATA;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_CURRENT;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_PREVIOUS;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

public class ResponseMessageBuilder {
    private final ObjectNode json = JsonUtil.createObjectNode();

    public static ResponseMessageBuilder builder() {
        return new ResponseMessageBuilder();
    }

    public ResponseMessageBuilder withClientToken(final Optional<String> token) {
        token.ifPresent(this::withClientToken);
        return this;
    }

    public ResponseMessageBuilder withClientToken(final String token) {
        json.set(SHADOW_DOCUMENT_CLIENT_TOKEN, new TextNode(token));
        return this;
    }

    public ResponseMessageBuilder withTimestamp(final Instant time) {
        json.set(SHADOW_DOCUMENT_TIMESTAMP, new LongNode(time.getEpochSecond()));
        return this;
    }

    public ResponseMessageBuilder withVersion(final long version) {
        json.set(SHADOW_DOCUMENT_VERSION, new LongNode(version));
        return this;
    }

    //public ResponseMessageBuilder withError(final SpectreException exception) {
    //    json.set(ERROR_CODE_FIELD_NAME, exception.getCode().getValue());
    //    json.set(ERROR_MESSAGE_FIELD_NAME, Json.fromJavaString(exception.getMessage()));
    //    return this;
    //}

    public ResponseMessageBuilder withState(final JsonNode state) {
        json.set(SHADOW_DOCUMENT_STATE, state);
        return this;
    }

    public ResponseMessageBuilder withMetadata(final JsonNode metadata) {
        json.set(SHADOW_DOCUMENT_METADATA, metadata);
        return this;
    }

    public ResponseMessageBuilder withPrevious(final JsonNode previousState) {
        json.set(SHADOW_DOCUMENT_STATE_PREVIOUS, previousState);
        return this;
    }

    public ResponseMessageBuilder withCurrent(final JsonNode currentState) {
        json.set(SHADOW_DOCUMENT_STATE_CURRENT, currentState);
        return this;
    }

    /**
     * Return the build JsonNode object.
     *
     * Note this does not make a deep copy of the object - any changes made to the builder
     * after this is called will affected the returned object.
     */
    public ObjectNode build() {
        return json;
    }
}
