/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static com.aws.greengrass.shadowmanager.util.JsonUtil.getPayloadJson;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.validatePayloadSchema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class JsonUtilTest {
    private static final String NAME_A = "{\"name\": \"A\"}";
    private static final String NAME_B = "{\"name\": \"B\"}";

    @BeforeEach
    void setup() throws IOException {
        JsonUtil.loadSchema();
    }

    @ParameterizedTest
    @ValueSource(strings={
            "{\"version\": 1}",
            "{}",
            "{\"state\": {\"reported\": 1}}",
            "{\"state\": {\"desired\": 1}}",
            "{\"state\": {\"delta\": 1}}",
            "{\"version\": 1, \"state\": {\"foo\": {\"name\": \"The Beatles\"}}}",
            "{\"version\": \"foo\", \"state\": {\"foo\": {\"name\": \"The Beatles\"}}}"
            })
    void GIVEN_bad_payload_WHEN_validate_THEN_throws_invalid_request_exception(String json) {
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class,
                () -> {
                    validatePayloadSchema(getPayloadJson(json.getBytes(StandardCharsets.UTF_8)).get());
                });
        assertThat(thrown.getErrorMessage().getMessage(), containsString("Invalid JSON"));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"state\": null}",
            "{\"state\": {}}",
            "{\"state\": {\"desired\":" + NAME_A + ", \"reported\":" + NAME_B + ", \"delta\":" + NAME_A + "}}",
            "{\"state\": {\"desired\":" + NAME_A + ", \"reported\":" + NAME_A + "}}",
            "{\"state\": {\"desired\":" + NAME_A + "}}",
            "{\"state\": {\"desired\":" + NAME_A + ", \"reported\": null}}",
            "{\"state\": {\"reported\":" + NAME_A + "}}",
            "{\"state\": {\"reported\":" + NAME_A + ", \"desired\": null}}",

            "{\"version\": 1, \"state\": {\"desired\":" + NAME_A + ", \"reported\":" + NAME_B + ", \"delta\":" + NAME_A + "}}",
            "{\"version\": 1, \"state\": {\"desired\":" + NAME_A + ", \"reported\":" + NAME_A + "}}",
            "{\"version\": 1, \"state\": {\"desired\":" + NAME_A + "}}",
            "{\"version\": 1, \"state\": {\"desired\":" + NAME_A + ", \"reported\": null}}",
            "{\"version\": 1, \"state\": {\"reported\":" + NAME_A + "}}",
            "{\"version\": 1, \"state\": {\"reported\":" + NAME_A + ", \"desired\": null}}",
    })
    void GIVEN_valid_request_WHEN_validatePayloadSchema_THEN_does_not_throw(String json) {
        assertDoesNotThrow(() -> {
            validatePayloadSchema(getPayloadJson(json.getBytes(StandardCharsets.UTF_8)).get());
        });
    }

    @Test
    void GIVEN_no_source_node_and_good_update_node_WHEN_validatePayload_THEN_successfully_validates() throws IOException {
        ShadowDocument source = new ShadowDocument();
        JsonNode updateNode = getPayloadJson("{\"version\": 1, \"state\": {\"desired\": {\"name\": \"The Beatles\"}}}".getBytes()).get();
        assertDoesNotThrow(() -> JsonUtil.validatePayload(source, updateNode));
    }

    @Test
    void GIVEN_no_source_node_and_bad_update_node_WHEN_validatePayload_THEN_throws_invalid_request_parameters_exception() throws IOException {
        ShadowDocument source = new ShadowDocument();
        JsonNode updateNode = getPayloadJson("{\"version\": 2, \"state\": {\"desired\": {\"name\": \"The Beatles\"}}}".getBytes()).get();
        InvalidRequestParametersException thrown =  assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayload(source, updateNode));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
        assertThat(thrown.getErrorMessage().getMessage(), is("Invalid version"));
    }

    @Test
    void GIVEN_state_with_6_levels_WHEN_validatePayload_THEN_successfully_validates() throws IOException {
        ShadowDocument source = new ShadowDocument();
        JsonNode desiredNode = getPayloadJson("{\"state\": {\"desired\": {\"1\": {\"2\": {\"3\": {\"4\": {\"5\": {\"6\": \"The Beatles\"}}}}}}}}".getBytes()).get();
        assertDoesNotThrow(() -> JsonUtil.validatePayload(source, desiredNode));
        JsonNode reportedNode = getPayloadJson("{\"state\": {\"reported\": {\"1\": {\"2\": {\"3\": {\"4\": {\"5\": {\"6\": \"The Beatles\"}}}}}}}}".getBytes()).get();
        assertDoesNotThrow(() -> JsonUtil.validatePayload(source, reportedNode));
    }

    @Test
    void GIVEN_state_with_7_levels_WHEN_validatePayload_THEN_throws_invalid_request_parameters_exception() throws IOException {
        ShadowDocument source = new ShadowDocument();
        JsonNode desiredNode = getPayloadJson("{\"state\": {\"desired\": {\"1\": {\"2\": {\"3\": {\"4\": {\"5\": {\"6\": {\"7\": \"The Beatles\"}}}}}}}}}".getBytes()).get();
        InvalidRequestParametersException thrown =  assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayload(source, desiredNode));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
        assertThat(thrown.getErrorMessage().getMessage(), is("JSON contains too many levels of nesting; maximum is 6"));

        JsonNode reportedNode = getPayloadJson("{\"state\": {\"reported\": {\"1\": {\"2\": {\"3\": {\"4\": {\"5\": {\"6\": {\"7\": \"The Beatles\"}}}}}}}}}".getBytes()).get();
        thrown =  assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayload(source, reportedNode));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
        assertThat(thrown.getErrorMessage().getMessage(), is("JSON contains too many levels of nesting; maximum is 6"));

        JsonNode reportedAndDesiredNode = getPayloadJson("{\"state\": {\"desired\": {\"1\": {\"2\": {\"3\": {\"4\": {\"5\": {\"6\": \"The Beatles\"}}}}}},\"reported\": {\"1\": {\"2\": {\"3\": {\"4\": {\"5\": {\"6\": {\"7\": \"The Beatles\"}}}}}}}}}".getBytes()).get();
        thrown =  assertThrows(InvalidRequestParametersException.class, () -> JsonUtil.validatePayload(source, reportedAndDesiredNode));
        assertThat(thrown.getErrorMessage(), is(notNullValue()));
        assertThat(thrown.getErrorMessage().getErrorCode(), is(400));
        assertThat(thrown.getErrorMessage().getMessage(), is("JSON contains too many levels of nesting; maximum is 6"));
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> emptyStateDocuments() {
        return Stream.of(
                Arguments.of("{\"state\": null}", false),
                Arguments.of("{\"state\": {}}", true),
                Arguments.of("{}", true),
                Arguments.of("null", false),
                Arguments.of("{\"state\": {\"field\":1}}", false),
                Arguments.of("{\"field\":1}", false)
        );
    }

    @ParameterizedTest
    @MethodSource("emptyStateDocuments")
    void GIVEN_empty_state_document_WHEN_check_is_empty_state_document_THEN_return_true(String json, boolean emptyDocumentExpected) throws IOException {
        assertEquals(emptyDocumentExpected,
                JsonUtil.isEmptyStateDocument(getPayloadJson(json.getBytes(StandardCharsets.UTF_8)).get()),
                String.format("%s %sexpected to be empty", json, emptyDocumentExpected ? "" : "not "));
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> nullStateDocuments() {
        return Stream.of(
                Arguments.of("{\"state\": null}", true),
                Arguments.of("{\"state\": {}}", false),
                Arguments.of("{}", false),
                Arguments.of("null", true),
                Arguments.of("{\"state\": {\"field\":1}}", false),
                Arguments.of("{\"field\":1}", false)
        );
    }

    @ParameterizedTest
    @MethodSource("nullStateDocuments")
    void GIVEN_null_state_document_WHEN_check_is_null_state_document_THEN_return_true(String json, boolean nullDocumentExpected) throws IOException {
        assertEquals(nullDocumentExpected,
                JsonUtil.isNullStateDocument(getPayloadJson(json.getBytes(StandardCharsets.UTF_8)).get()),
                String.format("%s %sexpected to be null", json, nullDocumentExpected ? "" : "not "));
    }
}
