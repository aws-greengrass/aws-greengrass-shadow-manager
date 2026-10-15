/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for issue #212. After any {@link ShadowDocument#update(JsonNode)}, the state and metadata
 * trees must stay in step: a non-null {@code state.desired} (or {@code state.reported}) must be matched by a
 * non-null metadata counterpart, and {@link ShadowDocument#getDelta()} must be safe to call. Updates containing
 * empty objects (directly, or left behind after all children of a node are removed) used to strip the
 * corresponding metadata nodes, leaving the metadata tree null while the state tree was not, and a later
 * {@code getDelta()} then threw a {@code NullPointerException}.
 */
@ExtendWith({GGExtension.class})
class ShadowDocumentMetadataInvariantTest {
    private static final String FRESH_DOCUMENT = "{\"state\":{},\"version\":1}";

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> updateCases() {
        return Stream.of(
                Arguments.of("empty object as only desired node",
                        FRESH_DOCUMENT,
                        "{\"state\":{\"desired\":{\"node\":{}}}}"),
                Arguments.of("empty object beside a populated sibling",
                        FRESH_DOCUMENT,
                        "{\"state\":{\"desired\":{\"empty\":{},\"populated\":{\"on\":true}}}}"),
                Arguments.of("all children of a node removed by a null patch",
                        "{\"state\":{\"desired\":{\"node\":{\"a\":1,\"b\":2}}},"
                                + "\"metadata\":{\"desired\":{\"node\":{\"a\":{\"timestamp\":1},"
                                + "\"b\":{\"timestamp\":1}}}},\"version\":5}",
                        "{\"state\":{\"desired\":{\"node\":{\"a\":null,\"b\":null}}}}"),
                Arguments.of("empty object nested two levels down",
                        FRESH_DOCUMENT,
                        "{\"state\":{\"desired\":{\"outer\":{\"inner\":{}}}}}"),
                Arguments.of("empty object on the reported side",
                        FRESH_DOCUMENT,
                        "{\"state\":{\"reported\":{\"node\":{}},\"desired\":{\"x\":1}}}"),
                Arguments.of("populated desired node",
                        FRESH_DOCUMENT,
                        "{\"state\":{\"desired\":{\"node\":{\"on\":true}}}}"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("updateCases")
    void GIVEN_document_WHEN_update_THEN_metadata_is_non_null_wherever_state_is(
            String name, String initialDocument, String updateRequest) throws IOException {
        ShadowDocument document = updatedDocument(initialDocument, updateRequest);

        if (document.getState().getDesired() != null) {
            assertNotNull(document.getMetadata().getDesired(),
                    "metadata.desired is null while state.desired is not");
        }
        if (document.getState().getReported() != null) {
            assertNotNull(document.getMetadata().getReported(),
                    "metadata.reported is null while state.reported is not");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("updateCases")
    void GIVEN_document_WHEN_update_THEN_get_delta_does_not_throw(
            String name, String initialDocument, String updateRequest) throws IOException {
        ShadowDocument document = updatedDocument(initialDocument, updateRequest);

        assertDoesNotThrow(document::getDelta);
    }

    private static ShadowDocument updatedDocument(String initialDocument, String updateRequest) throws IOException {
        ShadowDocument document = new ShadowDocument(initialDocument.getBytes(StandardCharsets.UTF_8), false);
        Optional<JsonNode> update = JsonUtil.getPayloadJson(updateRequest.getBytes(StandardCharsets.UTF_8));
        assertTrue(update.isPresent());
        document.update(update.get());
        return document;
    }
}
