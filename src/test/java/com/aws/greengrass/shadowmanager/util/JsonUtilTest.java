/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class JsonUtilTest {

    @Test
    void GIVEN_bad_state_node_WHEN_validatePayloadSchema_THEN_throws_invalid_request_parameters_exception() throws IOException, ProcessingException {
        JsonUtil.setUpdateShadowJsonSchema();
        JsonNode node = JsonUtil.getPayloadJson("{\"version\": 6, \"state\": {\"desired\": [\"Pink Floyd\", \"The Beatles\"]}}".getBytes()).get();
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayloadSchema(node));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getMessage(), is("Invalid JSON\n"
                + "Invalid desired. instance type (array) does not match any allowed primitive type (allowed: [\"null\",\"object\"])"));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
    }

    @Test
    void GIVEN_bad_state_node_WHEN_validate_throws_processingException_THEN_throws_invalid_request_parameters_exception() throws IOException, ProcessingException {
        JsonSchema mockJsonSchema = mock(JsonSchema.class);
        when(mockJsonSchema.validate(any(JsonNode.class))).thenThrow(ProcessingException.class);
        JsonUtil.setUpdateShadowRequestJsonSchema(mockJsonSchema);
        JsonNode node = JsonUtil.getPayloadJson("{\"version\": 6, \"state\": {\"desired\": [\"Pink Floyd\", \"The Beatles\"]}}".getBytes()).get();
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayloadSchema(node));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(500));
        assertThat(thrown.getErrorMessage().getMessage(), is("Internal service failure"));
    }

    @Test
    void GIVEN_no_source_node_and_good_update_node_WHEN_validatePayload_THEN_successfully_validates() throws IOException {
        ShadowDocument source = new ShadowDocument();
        JsonNode updateNode = JsonUtil.getPayloadJson("{\"version\": 1, \"state\": {\"desired\": {\"name\": \"The Beatles\"}}}".getBytes()).get();
        assertDoesNotThrow(() -> JsonUtil.validatePayload(source, updateNode));
    }

    @Test
    void GIVEN_no_source_node_and_bad_update_node_WHEN_validatePayload_THEN_throws_invalid_request_parameters_exception() throws IOException {
        ShadowDocument source = new ShadowDocument();
        JsonNode updateNode = JsonUtil.getPayloadJson("{\"version\": 2, \"state\": {\"desired\": {\"name\": \"The Beatles\"}}}".getBytes()).get();
        InvalidRequestParametersException thrown =  assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayload(source, updateNode));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
        assertThat(thrown.getErrorMessage().getMessage(), is("Invalid version"));
    }
}
