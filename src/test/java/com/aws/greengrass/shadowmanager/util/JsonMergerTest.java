/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class JsonMergerTest {
    private final static String SOURCE_NODE_STRING = "{\"id\": 100, \"SomeKey\": \"SomeValue\",\"SomeObjectKey\": {\"foo\": \"bar\"}}";
    private final static String SOURCE_NODE_WITH_ARRAY_STRING = "{\"id\": 100, \"SomeArrayKey\": [\"SomeValue1\", \"SomeValue2\"]}";
    private final static String SOURCE_NODE_WITH_ARRAY_NODE_STRING = "[\"SomeValue1\", \"SomeValue2\"]";
    private final static String PATCH_NODE_WITH_NEW_FIELD_STRING = "{\"NewKey\": true, \"NewNullParent\": {\"NewNullChild1\": null, \"NewNullChild2\": {\"NewNullChild3\": null}}, \"NewChildLevel1\": {\"NewChildLevel2\": {\"NewChildLevel3\":\"NewChildValue\"}}}";
    private final static String MERGED_NODE_WITH_NEW_FIELD_STRING = "{\"id\": 100, \"SomeKey\": \"SomeValue\",\"SomeObjectKey\": {\"foo\": \"bar\"}, \"NewKey\": true, \"NewChildLevel1\": {\"NewChildLevel2\": {\"NewChildLevel3\":\"NewChildValue\"}}}";
    private final static String PATCH_NODE_WITH_NULL_FIELD_STRING = "{\"SomeKey\": null,\"SomeObjectKey\": {\"foo\": null}}";
    private final static String MERGED_NODE_WITHOUT_NULL_FIELD_STRING = "{\"id\": 100,\"SomeObjectKey\":{}}";
    private final static String MERGED_NODE_WITH_UPDATED_ARRAY_NODE_STRING = "[\"SomeValue3\", \"SomeValue4\"]";
    private final static String PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING = "[\"SomeValue3\", \"SomeValue4\"]";
    private final static String EMPTY_DOCUMENT = "{}";
    private final static String NULL_DOCUMENT = "null";
    private static JsonNode sourceNodeWithArray;

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> mergeTestInput() {
        return Stream.of(
                Arguments.of("GIVEN patch with new node, THEN add new field in source node", SOURCE_NODE_STRING, PATCH_NODE_WITH_NEW_FIELD_STRING, MERGED_NODE_WITH_NEW_FIELD_STRING),
                Arguments.of("GIVEN patch with null nodes, THEN removes all null nodes in source node", SOURCE_NODE_STRING, PATCH_NODE_WITH_NULL_FIELD_STRING, MERGED_NODE_WITHOUT_NULL_FIELD_STRING),
                Arguments.of("GIVEN patch with new nodes in an array node, THEN replaces all the elements in the source with the elements in the patch", SOURCE_NODE_WITH_ARRAY_NODE_STRING, PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING, MERGED_NODE_WITH_UPDATED_ARRAY_NODE_STRING),
                Arguments.of("GIVEN patch with empty state, THEN source node is cleared", SOURCE_NODE_STRING, EMPTY_DOCUMENT, EMPTY_DOCUMENT),
                Arguments.of("GIVEN patch with null state, THEN source node is cleared", SOURCE_NODE_STRING, NULL_DOCUMENT, EMPTY_DOCUMENT),
                Arguments.of("GIVEN empty source and non-empty patch, THEN patch is the result", EMPTY_DOCUMENT, SOURCE_NODE_STRING, SOURCE_NODE_STRING),
                Arguments.of("GIVEN null source and non-empty patch, THEN patch is the result", NULL_DOCUMENT, SOURCE_NODE_STRING, SOURCE_NODE_STRING)
        );
    }


    @BeforeEach
    void setup() throws IOException {
        sourceNodeWithArray = JsonUtil.getPayloadJson(SOURCE_NODE_WITH_ARRAY_STRING.getBytes()).get();
    }

    @ParameterizedTest
    @MethodSource("mergeTestInput")
    void mergeTest(String reason, String source, String patch, String merged) throws IOException {
        JsonNode sourceNode = JsonUtil.getPayloadJson(source.getBytes()).get();
        JsonNode patchNode = JsonUtil.getPayloadJson(patch.getBytes()).get();
        JsonNode mergedNode = JsonUtil.getPayloadJson(merged.getBytes()).get();

        JsonMerger.merge(sourceNode, patchNode);

        assertThat(reason, sourceNode, Matchers.is(mergedNode));
    }

    @Test
    void GIVEN_patch_with_updated_array_node_WHEN_merge_THEN_updates_array_field_in_source_node(ExtensionContext context) throws IOException {
        JsonNode patchNode = JsonUtil.getPayloadJson(PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING.getBytes()).get();
        InvalidRequestParametersException expected = new InvalidRequestParametersException(ErrorMessage.createInvalidPayloadJsonMessage("Merge only works with"
                + " Json objects whose underlying node are the same type."));
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> JsonMerger.merge(sourceNodeWithArray, patchNode));

        assertThat(thrown.getErrorMessage().getErrorCode(), Matchers.is(expected.getErrorMessage().getErrorCode()));
        assertThat(thrown.getErrorMessage().getMessage(), Matchers.is(expected.getErrorMessage().getMessage()));
    }
}
