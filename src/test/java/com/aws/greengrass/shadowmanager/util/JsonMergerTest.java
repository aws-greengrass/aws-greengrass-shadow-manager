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
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class JsonMergerTest {
    private final static String SOURCE_NODE_STRING = "{\"id\": 100, \"SomeKey\": \"SomeValue\"}";
    private final static String SOURCE_NODE_WITH_ARRAY_STRING = "{\"id\": 100, \"SomeArrayKey\": [\"SomeValue1\", \"SomeValue2\"]}";
    private final static String SOURCE_NODE_WITH_ARRAY_NODE_STRING = "[\"SomeValue1\", \"SomeValue2\"]";
    private final static String PATCH_NODE_WITH_NEW_FIELD_STRING = "{\"NewKey\": true, \"NewNullParent\": {\"NewNullChild1\": null, \"NewNullChild2\": {\"NewNullChild3\": null}}, \"NewChildLevel1\": {\"NewChildLevel2\": {\"NewChildLevel3\":\"NewChildValue\"}}}";
    private final static String MERGED_NODE_WITH_NEW_FIELD_STRING = "{\"id\": 100, \"SomeKey\": \"SomeValue\", \"NewKey\": true, \"NewChildLevel1\": {\"NewChildLevel2\": {\"NewChildLevel3\":\"NewChildValue\"}}}";
    private final static String PATCH_NODE_WITH_NULL_FIELD_STRING = "{\"SomeKey\": null}";
    private final static String MERGED_NODE_WITHOUT_NULL_FIELD_STRING = "{\"id\": 100}";
    private final static String MERGED_NODE_WITH_UPDATED_ARRAY_NODE_STRING = "[\"SomeValue3\", \"SomeValue4\"]";
    private final static String PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING = "[\"SomeValue3\", \"SomeValue4\"]";
    private JsonNode sourceNode;
    private JsonNode sourceNodeWithArray;
    private JsonNode sourceNodeAsArray;

    @BeforeEach
    void setup() throws IOException {
        sourceNode = JsonUtil.getPayloadJson(SOURCE_NODE_STRING.getBytes()).get();
        sourceNodeWithArray = JsonUtil.getPayloadJson(SOURCE_NODE_WITH_ARRAY_STRING.getBytes()).get();
        sourceNodeAsArray = JsonUtil.getPayloadJson(SOURCE_NODE_WITH_ARRAY_NODE_STRING.getBytes()).get();
    }

    @Test
    void GIVEN_patch_with_new_node_WHEN_merge_THEN_adds_new_field_in_source_node() throws IOException {
        JsonNode patchNode = JsonUtil.getPayloadJson(PATCH_NODE_WITH_NEW_FIELD_STRING.getBytes()).get();
        JsonNode mergedNode = JsonUtil.getPayloadJson(MERGED_NODE_WITH_NEW_FIELD_STRING.getBytes()).get();
        JsonMerger.merge(sourceNode, patchNode);

        assertThat(sourceNode, Matchers.is(mergedNode));
    }

    @Test
    void GIVEN_patch_with_null_node_WHEN_merge_THEN_removes_field_in_source_node() throws IOException {
        JsonNode patchNode = JsonUtil.getPayloadJson(PATCH_NODE_WITH_NULL_FIELD_STRING.getBytes()).get();
        JsonNode mergedNode = JsonUtil.getPayloadJson(MERGED_NODE_WITHOUT_NULL_FIELD_STRING.getBytes()).get();
        JsonMerger.merge(sourceNode, patchNode);

        assertThat(sourceNode, Matchers.is(mergedNode));
    }

    @Test
    void GIVEN_patch_with_updated_array_node_WHEN_merge_THEN_updates_array_node_in_source_node() throws IOException {
        JsonNode patchNode = JsonUtil.getPayloadJson(PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING.getBytes()).get();
        JsonNode mergedNode = JsonUtil.getPayloadJson(MERGED_NODE_WITH_UPDATED_ARRAY_NODE_STRING.getBytes()).get();
        JsonMerger.merge(sourceNodeAsArray, patchNode);

        assertThat(sourceNodeAsArray, Matchers.is(mergedNode));
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
