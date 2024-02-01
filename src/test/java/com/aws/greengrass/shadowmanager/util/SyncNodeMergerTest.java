/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class SyncNodeMergerTest {
    private static final byte[] LOCAL_DOCUMENT = "{\"name\": \"The Beach Boys\", \"temperature\": 80, \"NewField\": 100}".getBytes();
    private static final byte[] CLOUD_DOCUMENT = "{\"name\": \"The Beatles\", \"temperature\": 80, \"OldField\": true}".getBytes();
    private static final byte[] CLOUD_DOCUMENT_CHANGED = "{\"name\": \"Pink Floyd\", \"temperature\": 60, \"OldField\": true, \"SomeOtherThingNew\": 100}".getBytes();
    private static final byte[] BASE_DOCUMENT = "{\"name\": \"The Beatles\", \"temperature\": 70, \"OldField\": true}".getBytes();
    private static final byte[] MERGED_DOCUMENT = ("{\"name\":\"The Beach Boys\", \"temperature\": 80, \"NewField\":100, \"OldField\":null}").getBytes();
    private static final byte[] MERGED_DOCUMENT_WITH_CLOUD_CHANGED_AND_CLOUD_OWNER = "{\"name\":\"Pink Floyd\", \"temperature\": 60,\"NewField\":100,\"OldField\":null,\"SomeOtherThingNew\":100}".getBytes();
    private static final byte[] MERGED_DOCUMENT_WITH_CLOUD_CHANGED_AND_LOCAL_OWNER = "{\"name\":\"The Beach Boys\", \"temperature\": 80,\"NewField\":100,\"OldField\":null,\"SomeOtherThingNew\":100}".getBytes();

    private final static byte[] LOCAL_WITH_ARRAY_STRING = "{\"id\": 100, \"SomeArrayKey\": [\"SomeValue1\", \"SomeValue2\"]}".getBytes();
    private final static byte[] CLOUD_WITH_ARRAY_STRING = "{\"id\": 100, \"SomeArrayKey\": [\"SomeValue3\", \"SomeValue4\"]}".getBytes();
    private final static byte[] BASE_WITH_ARRAY_STRING = "{\"id\": 100, \"SomeArrayKey\": [\"SomeValue8\", \"SomeValue9\"]}".getBytes();

    private JsonNode localDocument;
    private JsonNode localDocumentWithArray;
    private JsonNode cloudDocument;
    private JsonNode cloudDocumentWithArray;
    private JsonNode cloudDocumentChanged;
    private JsonNode baseDocument;
    private JsonNode baseDocumentWithArray;
    private JsonNode mergedNode;
    private JsonNode mergedNodeWithCloudOwner;
    private JsonNode mergedNodeWithLocalOwner;

    @BeforeEach
    void setup() throws IOException {
        localDocument = JsonUtil.getPayloadJson(LOCAL_DOCUMENT).get();
        localDocumentWithArray = JsonUtil.getPayloadJson(LOCAL_WITH_ARRAY_STRING).get();
        cloudDocument = JsonUtil.getPayloadJson(CLOUD_DOCUMENT).get();
        cloudDocumentWithArray = JsonUtil.getPayloadJson(CLOUD_WITH_ARRAY_STRING).get();
        cloudDocumentChanged = JsonUtil.getPayloadJson(CLOUD_DOCUMENT_CHANGED).get();
        baseDocument = JsonUtil.getPayloadJson(BASE_DOCUMENT).get();
        baseDocumentWithArray = JsonUtil.getPayloadJson(BASE_WITH_ARRAY_STRING).get();
        mergedNode = JsonUtil.getPayloadJson(MERGED_DOCUMENT).get();
        mergedNodeWithCloudOwner = JsonUtil.getPayloadJson(MERGED_DOCUMENT_WITH_CLOUD_CHANGED_AND_CLOUD_OWNER).get();
        mergedNodeWithLocalOwner = JsonUtil.getPayloadJson(MERGED_DOCUMENT_WITH_CLOUD_CHANGED_AND_LOCAL_OWNER).get();
    }

    @ParameterizedTest
    @EnumSource(DataOwner.class)
    void GIVEN_local_updated_cloud_not_updated_WHEN_getMergedNode_THEN_gets_correct_merged_node(DataOwner owner) {
        JsonNode actual = SyncNodeMerger.getMergedNode(localDocument, cloudDocument, baseDocument, owner);
        assertThat(actual, is(mergedNode));
    }

    @Test
    void GIVEN_null_local_and_cloud_node_WHEN_getMergedNode_THEN_gets_null() {
        JsonNode actual = SyncNodeMerger.getMergedNode(null, null, baseDocument, DataOwner.CLOUD);
        assertThat(actual, is(nullValue()));
    }

    @Test
    void GIVEN_local_and_cloud_updated_WHEN_getMergedNode_THEN_gets_correct_merged_node1() {
        JsonNode actual = SyncNodeMerger.getMergedNode(localDocument, cloudDocumentChanged, baseDocument, DataOwner.LOCAL);
        assertThat(actual, is(mergedNodeWithLocalOwner));
    }

    @Test
    void GIVEN_local_and_cloud_updated_WHEN_getMergedNode_THEN_gets_correct_merged_node() {
        JsonNode actual = SyncNodeMerger.getMergedNode(localDocument, cloudDocumentChanged, baseDocument, DataOwner.CLOUD);
        assertThat(actual, is(mergedNodeWithCloudOwner));
    }

    @Test
    void GIVEN_local_and_cloud_updated_with_array_with_cloud_owner_WHEN_getMergedNode_THEN_gets_correct_merged_node() {
        JsonNode actual = SyncNodeMerger.getMergedNode(localDocumentWithArray, cloudDocumentWithArray, baseDocumentWithArray, DataOwner.CLOUD);
        assertThat(actual, is(cloudDocumentWithArray));
    }
    @Test
    void GIVEN_local_and_cloud_updated_with_array_with_local_owner_WHEN_getMergedNode_THEN_gets_correct_merged_node() {
        JsonNode actual = SyncNodeMerger.getMergedNode(localDocumentWithArray, cloudDocumentWithArray, baseDocumentWithArray, DataOwner.LOCAL);
        assertThat(actual, is(localDocumentWithArray));
    }
}
