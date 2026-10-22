/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowStateMetadataTest {

    @Mock
    Clock mockClock;
    @Mock
    Instant mockInstant;

    private final static String desiredPatchString = "{\"desired\": {\"id\": 100, \"SomeObject\": {\"SomeChild1\": \"SomeValue\", \"SomeChild2\": {\"SomeChild3\": true}}}}";
    private final static String reportedPatchString = "{\"reported\": {\"id\": 100, \"SomeObject\": {\"SomeChild1\": \"SomeValue\", \"SomeChild2\": {\"SomeChild3\": true}}}}";
    private final static String patchMetadataString = "{\"id\": 12345, \"SomeObject\": {\"SomeChild1\": 12345, \"SomeChild2\": {\"SomeChild3\": 12345}}}";
    private final static long timestamp = 1617731792L;
    private static final long OLD_TIMESTAMP = 1L;

    @BeforeEach
    void setup() {
        lenient().when(mockClock.instant()).thenReturn(mockInstant);
        lenient().when(mockInstant.getEpochSecond()).thenReturn(timestamp);
    }

    @Test
    void GIVEN_empty_state_and_metadata_and_non_empty_patch_WHEN_update_THEN_metadata_is_empty() throws IOException {
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(desiredPatchString.getBytes());
        assertTrue(patchJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(null, null, mockClock);
        ShadowState state = new ShadowState();

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));
        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        JsonNode patchToCheck = patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED);
        checkNodeForMetadata(patchToCheck);
    }

    @Test
    void GIVEN_empty_state_and_non_empty_patch_and_metadata_WHEN_update_THEN_metadata_is_empty() throws IOException {
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(desiredPatchString.getBytes());
        Optional<JsonNode> reportedPatchJson = JsonUtil.getPayloadJson(reportedPatchString.getBytes());
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchJson.isPresent());
        assertTrue(reportedPatchJson.isPresent());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(null, reportedPatchJson.get().get(SHADOW_DOCUMENT_STATE_REPORTED));

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));
        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        JsonNode patchToCheck = patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED);
        checkNodeForMetadata(patchToCheck);
    }

    @Test
    void GIVEN_non_empty_state_metadata_and_patch_desired_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(desiredPatchString.getBytes());
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchJson.isPresent());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(patchJson.get().get(SHADOW_DOCUMENT_STATE_DESIRED), null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        checkNodeForMetadata(shadowStateMetadata.getDesired());

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));
        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        JsonNode patchToCheck = patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED);
        checkNodeForMetadata(patchToCheck);
    }

    @Test
    void GIVEN_non_empty_state_metadata_and_patch_reported_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(reportedPatchString.getBytes());
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchJson.isPresent());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(null, patchMetadataJson.get(), mockClock);
        ShadowState state = new ShadowState(null, patchJson.get().get(SHADOW_DOCUMENT_STATE_REPORTED));

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));
        checkNodeForMetadata(shadowStateMetadata.getReported());

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_REPORTED));
        JsonNode patchToCheck = patchMetadata.get(SHADOW_DOCUMENT_STATE_REPORTED);
        checkNodeForMetadata(patchToCheck);
    }

    @Test
    void GIVEN_non_empty_state_metadata_and_patch_reported_WHEN_update_THEN_metadata_is_correctly_updated2() throws IOException {
        final String desiredPatchString = "{\"desired\": {\"id\": {\"NewIdChild\": 100}, \"SomeObject\": \"NewObjectValue\", \"NewObject\": {\"NewObjectChild\": true}}}}";
        final String patchMetadataString = "{\"id\": 12345, \"SomeObject\": {\"SomeChild1\": 12345, \"SomeChild2\": {\"SomeChild3\": 12345}}}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(desiredPatchString.getBytes());
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchJson.isPresent());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(patchJson.get().get(SHADOW_DOCUMENT_STATE_DESIRED), null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        JsonNode patchToCheck = patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED);
        assertTrue(patchToCheck.has("SomeObject"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject")));
        assertThat(patchToCheck.get("SomeObject").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));

        assertTrue(patchToCheck.has("id"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("id")));
        assertTrue(patchToCheck.get("id").has("NewIdChild"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("id").get("NewIdChild")));
        assertThat(patchToCheck.get("id").get("NewIdChild").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));

        assertTrue(patchToCheck.has("NewObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("NewObject")));
        assertTrue(patchToCheck.get("NewObject").has("NewObjectChild"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("NewObject").get("NewObjectChild")));
        assertThat(patchToCheck.get("NewObject").get("NewObjectChild").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    private void checkNodeForMetadata(JsonNode patchToCheck) {
        assertTrue(patchToCheck.has("id"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("id")));
        assertThat(patchToCheck.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));

        assertTrue(patchToCheck.has("SomeObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject")));
        assertTrue(patchToCheck.get("SomeObject").has("SomeChild1"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject").get("SomeChild1")));
        assertThat(patchToCheck.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));

        assertTrue(patchToCheck.get("SomeObject").has("SomeChild2"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject").get("SomeChild2")));
        assertTrue(patchToCheck.get("SomeObject").get("SomeChild2").has("SomeChild3"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject").get("SomeChild2").get("SomeChild3")));
        assertThat(patchToCheck.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    @Test
    void GIVEN_non_empty_state_and_non_empty_patch_with_array_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        String patchString = "{\"desired\": {\"SomeArray\": [100]}}";
        String patchMetadataString = "{\"SomeArray\": [12345]}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(patchString.getBytes());
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchJson.isPresent());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(patchJson.get().get(SHADOW_DOCUMENT_STATE_DESIRED), null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(shadowStateMetadata.getDesired().has("SomeArray"));
        shadowStateMetadata.getDesired().get("SomeArray").forEach(jsonNode -> {
            assertTrue(ShadowStateMetadata.isMetadataNode(jsonNode));
            assertThat(jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
        });

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));
        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomeArray"));
        patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeArray").forEach(jsonNode -> {
            assertTrue(ShadowStateMetadata.isMetadataNode(jsonNode));
            assertThat(jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
        });
    }

    @Test
    void GIVEN_non_empty_state_with_null_field_and_non_empty_patch_with_array_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        String patchString = "{\"desired\": {\"SomObject\": true}}";
        String patchMetadataString = "{\"SomObject\": 12345}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(patchString.getBytes());
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchJson.isPresent());
        assertTrue(patchMetadataJson.isPresent());
        JsonNode stateJson = ((ObjectNode)patchJson.get().get(SHADOW_DOCUMENT_STATE_DESIRED).deepCopy()).set("SomObject", null);
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(stateJson, null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomObject"));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomObject").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    @Test
    void GIVEN_non_empty_state_with_nested_empty_object_and_non_empty_patch_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        String stateString = "{\"SomeObject\": {}}";
        JsonNode stateJson = JsonUtil.getPayloadJson(stateString.getBytes()).get();
        String patchString = "{\"desired\": {\"SomeObject\": {\"SomeVal\": null}}}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(patchString.getBytes());
        String patchMetadataString = "{\"SomeObject\": {\"SomeVal\": \"123\"}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());

        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(stateJson, null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        // The desired field will be present because of the nested object but the object will be empty
        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        String expectedDesired = "{\"SomeObject\": {}}";
        assertThat(shadowStateMetadata.getDesired(), is(JsonUtil.getPayloadJson(expectedDesired.getBytes()).get()));

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomeObject"));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("SomeVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    @Test
    void GIVEN_non_empty_state_with_nested_object_and_nested_field_removed_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        String stateString = "{\"SomeObject\": {\"AnotherVal\": \"4567\"}}";
        JsonNode stateJson = JsonUtil.getPayloadJson(stateString.getBytes()).get();
        String patchString = "{\"desired\": {\"SomeObject\": {\"SomeVal\": null, \"AnotherVal\": \"4567\"}}}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(patchString.getBytes());
        String patchMetadataString = "{\"SomeObject\": {\"SomeVal\": \"123\", \"AnotherVal\": \"4567\"}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());

        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(stateJson, null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        // The desired field will be present because of the nested object but the object will be empty
        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(shadowStateMetadata.getDesired().has("SomeObject"));
        assertTrue(shadowStateMetadata.getDesired().get("SomeObject").has("AnotherVal"));
        assertTrue(shadowStateMetadata.getDesired().get("SomeObject").get("AnotherVal").has(SHADOW_DOCUMENT_TIMESTAMP));
        assertThat(shadowStateMetadata.getDesired().get("SomeObject").get("AnotherVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(),
                is(timestamp));

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomeObject"));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("AnotherVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("SomeVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    @Test
    void GIVEN_non_empty_state_with_nested_object_and_nested_field_added_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        String stateString = "{\"SomeObject\": {\"SomeVal\": \"456\", \"AnotherVal\": \"testingValue\"}}";
        JsonNode stateJson = JsonUtil.getPayloadJson(stateString.getBytes()).get();
        String patchString = "{\"desired\": {\"SomeObject\": {\"SomeVal\": \"456\", \"AnotherVal\": " +
                "\"testingValue\"}}}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(patchString.getBytes());
        String patchMetadataString = "{\"SomeObject\": {\"SomeVal\": \"123\", \"AnotherVal\": \"testingValue\"}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());

        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(stateJson, null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        // The desired field will be present because of the nested object but the object will be empty
        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(shadowStateMetadata.getDesired().has("SomeObject"));
        assertTrue(shadowStateMetadata.getDesired().get("SomeObject").has("AnotherVal"));
        assertTrue(shadowStateMetadata.getDesired().get("SomeObject").get("AnotherVal").has(SHADOW_DOCUMENT_TIMESTAMP));
        assertThat(shadowStateMetadata.getDesired().get("SomeObject").get("AnotherVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(),
                is(timestamp));

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomeObject"));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("SomeVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("AnotherVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    @Test
    void GIVEN_non_empty_state_with_nested_object_and_nested_field_exchanged_WHEN_update_THEN_metadata_is_correctly_updated() throws IOException {
        String stateString = "{\"SomeObject\": {\"AnotherVal\": \"testingValue\"}}";
        JsonNode stateJson = JsonUtil.getPayloadJson(stateString.getBytes()).get();
        String patchString = "{\"desired\": {\"SomeObject\": {\"SomeVal\": null, \"AnotherVal\": " +
                "\"testingValue\"}}}";
        Optional<JsonNode> patchJson = JsonUtil.getPayloadJson(patchString.getBytes());
        String patchMetadataString = "{\"SomeObject\": {\"SomeVal\": \"123\", \"AnotherVal\": \"testingValue\"}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());

        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowState state = new ShadowState(stateJson, null);

        JsonNode patchMetadata = shadowStateMetadata.update(patchJson.get(), state);

        // The desired field will be present because of the nested object but the object will be empty
        assertFalse(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(shadowStateMetadata.getDesired().has("SomeObject"));
        assertFalse(shadowStateMetadata.getDesired().get("SomeObject").has("SomeVal"));
        assertTrue(shadowStateMetadata.getDesired().get("SomeObject").has("AnotherVal"));
        assertTrue(shadowStateMetadata.getDesired().get("SomeObject").get("AnotherVal").has(SHADOW_DOCUMENT_TIMESTAMP));
        assertThat(shadowStateMetadata.getDesired().get("SomeObject").get("AnotherVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(),
                is(timestamp));

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomeObject"));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("SomeVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeObject").get("AnotherVal").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(timestamp));
    }

    @Test
    void GIVEN_reported_and_desired_metadata_WHEN_toJson_THEN_gets_the_correct_json() throws IOException {
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchMetadataJson.isPresent());
        ObjectNode expectedMetadataJson = JsonUtil.OBJECT_MAPPER.createObjectNode();
        expectedMetadataJson.set(SHADOW_DOCUMENT_STATE_DESIRED, patchMetadataJson.get());
        expectedMetadataJson.set(SHADOW_DOCUMENT_STATE_REPORTED, patchMetadataJson.get());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), patchMetadataJson.get(), mockClock);
        JsonNode metadataJson = shadowStateMetadata.toJson();
        assertThat(metadataJson, is(expectedMetadataJson));
    }

    @Test
    void GIVEN_null_delta_WHEN_getDeltaMetadata_THEN_gets_null_delta_metadata() throws IOException {
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(null, null, mockClock);
        JsonNode deltaMetadata = shadowStateMetadata.getDeltaMetadata(null);
        assertTrue(JsonUtil.isNullOrMissing(deltaMetadata));
    }

    @Test
    void GIVEN_delta_node_WHEN_getDeltaMetadata_THEN_gets_correct_delta_metadata() throws IOException {
        final String deltaString = "{\"id\": 100, \"SomeArray\": [100], \"SomeObject\": {\"SomeChild1\": \"SomeValue\", \"SomeChild2\": {\"SomeChild3\": true}}}";
        final String patchMetadataString = "{\"id\": {\"timestamp\": 12345}, \"SomeArray\": [{\"timestamp\": 12345}], \"SomeObject\": {\"SomeChild1\": {\"timestamp\": 12345}, \"SomeChild2\": {\"SomeChild3\": {\"timestamp\": 12345}}}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchMetadataJson.isPresent());
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(deltaString.getBytes());
        assertTrue(deltaJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        JsonNode deltaMetadata = shadowStateMetadata.getDeltaMetadata(deltaJson.get());
        assertFalse(JsonUtil.isNullOrMissing(deltaMetadata));
        assertTrue(deltaMetadata.has("id"));
        assertThat(deltaMetadata.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));

        assertTrue(deltaMetadata.has("SomeObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject")));
        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild1"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));
        assertEquals(12345, deltaMetadata.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong());

        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild2"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject").get("SomeChild2")));
        assertTrue(deltaMetadata.get("SomeObject").get("SomeChild2").has("SomeChild3"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));
        assertEquals(12345, deltaMetadata.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong());

        assertTrue(deltaMetadata.has("SomeArray"));
        deltaMetadata.get("SomeArray").forEach(jsonNode -> {
            assertEquals(12345, jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong());
            assertThat(jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));
        });


    }

    @Test
    void GIVEN_delta_node_with_array_WHEN_getDeltaMetadata_THEN_gets_correct_delta_metadata() throws IOException {
        final String deltaString = "{\"id\": 100, \"SomeObject\": {\"SomeChild1\": \"SomeValue\", \"SomeChild2\": {\"SomeChild3\": true}}}";
        final String patchMetadataString = "{\"id\": {\"timestamp\": 12345}, \"SomeObject\": {\"SomeChild1\": {\"timestamp\": 12345}, \"SomeChild2\": {\"SomeChild3\": {\"timestamp\": 12345}}}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchMetadataJson.isPresent());
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(deltaString.getBytes());
        assertTrue(deltaJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        JsonNode deltaMetadata = shadowStateMetadata.getDeltaMetadata(deltaJson.get());
        assertFalse(JsonUtil.isNullOrMissing(deltaMetadata));
        assertTrue(deltaMetadata.has("id"));
        assertThat(deltaMetadata.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));

        assertTrue(deltaMetadata.has("SomeObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject")));
        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild1"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));

        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild2"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject").get("SomeChild2")));
        assertTrue(deltaMetadata.get("SomeObject").get("SomeChild2").has("SomeChild3"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));

    }


    @Test
    void GIVEN_delta_field_missing_from_metadata_WHEN_getDeltaMetadata_THEN_does_not_throw() throws IOException {
        // Regression test: the delta contains an array field ("SomeArray") for which the desired metadata has no
        // corresponding node. buildMetadata must not dereference the null metadata node. Previously this threw a
        // NullPointerException at ShadowStateMetadata.buildMetadata when casting/indexing the null metadata array.
        final String deltaString = "{\"id\": 100, \"SomeArray\": [100, 200]}";
        final String patchMetadataString = "{\"id\": {\"timestamp\": 12345}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchMetadataJson.isPresent());
        Optional<JsonNode> deltaJson = JsonUtil.getPayloadJson(deltaString.getBytes());
        assertTrue(deltaJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);

        JsonNode deltaMetadata = assertDoesNotThrow(() -> shadowStateMetadata.getDeltaMetadata(deltaJson.get()));

        assertFalse(JsonUtil.isNullOrMissing(deltaMetadata));
        // The field that had corresponding metadata is still present.
        assertTrue(deltaMetadata.has("id"));
        assertThat(deltaMetadata.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), is(12345L));
        // The field with no corresponding metadata is handled gracefully instead of throwing.
        assertTrue(JsonUtil.isNullOrMissing(deltaMetadata.get("SomeArray")));
    }

    @Test
    void GIVEN_metadata_WHEN_deepCopy_THEN_gets_new_instance_of_metadata() throws IOException {
        final String patchMetadataString = "{\"id\": {\"timestamp\": 12345}, \"SomeObject\": {\"SomeChild1\": {\"timestamp\": 12345}, \"SomeChild2\": {\"SomeChild3\": {\"timestamp\": 12345}}}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowStateMetadata deepCopiedMetadata = shadowStateMetadata.deepCopy();
        assertThat(deepCopiedMetadata, is(Matchers.not(shadowStateMetadata)));
        assertThat(deepCopiedMetadata.toJson(), is(shadowStateMetadata.toJson()));
    }

    @Test
    void GIVEN_reported_and_desired_metadata_WHEN_getLatestUpdated_THEN_gets_the_correct_latest_updated_timestamp() throws IOException {
        final String desired = "{\"id\": {\"timestamp\": 1}, \"SomeObject\": {\"SomeChild1\": {\"timestamp\": 2}, \"SomeChild2\": {\"SomeChild3\": {\"timestamp\": 4}}}}";
        final String reported = "{\"id2\": [{\"timestamp\": 10}, {\"timestamp\": 9}, {\"timestamp\": 2}, {\"timestamp\": 1620163568}]}";
        Optional<JsonNode> desiredJson = JsonUtil.getPayloadJson(desired.getBytes());
        Optional<JsonNode> reportedJson = JsonUtil.getPayloadJson(reported.getBytes());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(desiredJson.get(), reportedJson.get(), mockClock);
        assertThat(shadowStateMetadata.getLatestUpdatedTimestamp(), is(1620163568L));
    }

    // Expected values below were recorded from IoT Device Shadow; see the PR description.

    private static JsonNode json(String s) throws IOException {
        return s == null ? null : JsonUtil.OBJECT_MAPPER.readTree(s);
    }

    private static JsonNode shape(JsonNode node) {
        if (node == null) {
            return null;
        }
        if (ShadowStateMetadata.isMetadataNode(node)) {
            long t = node.get(SHADOW_DOCUMENT_TIMESTAMP).asLong();
            return new TextNode(t == timestamp ? "ts" : t == OLD_TIMESTAMP ? "old" : String.valueOf(t));
        }
        if (node.isArray()) {
            ArrayNode result = JsonUtil.OBJECT_MAPPER.createArrayNode();
            node.forEach(n -> result.add(shape(n)));
            return result;
        }
        if (node.isObject()) {
            ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
            node.fields().forEachRemaining(e -> result.set(e.getKey(), shape(e.getValue())));
            return result;
        }
        return node;
    }

    private static JsonNode oldMetadata(JsonNode state) {
        if (state == null) {
            return null;
        }
        if (state.isArray()) {
            ArrayNode result = JsonUtil.OBJECT_MAPPER.createArrayNode();
            state.forEach(n -> result.add(oldMetadata(n)));
            return result;
        }
        if (state.isObject()) {
            ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
            state.fields().forEachRemaining(e -> result.set(e.getKey(), oldMetadata(e.getValue())));
            return result;
        }
        return JsonUtil.OBJECT_MAPPER.createObjectNode().put(SHADOW_DOCUMENT_TIMESTAMP, OLD_TIMESTAMP);
    }

    private ShadowDocument document(String desired, String reported) throws IOException {
        return new ShadowDocument(new ShadowState(json(desired), json(reported)),
                new ShadowStateMetadata(oldMetadata(json(desired)), oldMetadata(json(reported)), mockClock), 1L);
    }

    // Row 1 in the PR description.
    @Test
    void GIVEN_value_and_object_WHEN_replaced_by_arrays_THEN_metadata_has_timestamp_per_element() throws IOException {
        ShadowDocument document = document("{\"a\":1,\"b\":{\"x\":1}}", null);

        assertDoesNotThrow(() -> document.update(json("{\"state\":{\"desired\":{\"a\":[1,2],\"b\":[3]}}}")));

        assertThat(document.getState().getDesired(), is(json("{\"a\":[1,2],\"b\":[3]}")));
        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"a\":[\"ts\",\"ts\"],\"b\":[\"ts\"]}")));
    }

    // Row 2 in the PR description.
    @Test
    void GIVEN_desired_WHEN_nested_object_adds_nothing_THEN_update_succeeds() throws IOException {
        ShadowDocument document = document("{\"z\":1}", null);

        JsonNode response = document.update(json("{\"state\":{\"desired\":{\"a\":{\"x\":null}}}}"));

        assertThat(document.getState().getDesired(), is(json("{\"z\":1}")));
        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"z\":\"old\"}")));
        assertThat(shape(response), is(json("{\"desired\":{\"a\":{\"x\":\"ts\"}}}")));
    }

    // Row 3 in the PR description.
    @Test
    void GIVEN_desired_WHEN_top_level_field_removed_THEN_its_metadata_removed() throws IOException {
        ShadowDocument document = document("{\"a\":1,\"b\":1}", null);

        document.update(json("{\"state\":{\"desired\":{\"a\":null}}}"));

        assertThat(document.getState().getDesired(), is(json("{\"b\":1}")));
        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"b\":\"old\"}")));
    }

    // Row 4 in the PR description.
    @Test
    void GIVEN_desired_and_reported_WHEN_desired_set_to_null_THEN_desired_metadata_removed() throws IOException {
        ShadowDocument document = document("{\"a\":1}", "{\"a\":2}");

        document.update(json("{\"state\":{\"desired\":null}}"));

        assertThat(document.getState().getDesired(), is(Matchers.nullValue()));
        assertThat(document.getMetadata().getDesired(), is(Matchers.nullValue()));
        assertThat(shape(document.getMetadata().getReported()), is(json("{\"a\":\"old\"}")));
    }

    // Row 5 in the PR description.
    @Test
    void GIVEN_nested_object_WHEN_nested_field_removed_THEN_sibling_timestamps_kept() throws IOException {
        ShadowDocument document = document("{\"a\":{\"b\":{\"c\":1,\"d\":2}}}", null);

        document.update(json("{\"state\":{\"desired\":{\"a\":{\"b\":{\"c\":null}}}}}"));

        assertThat(document.getState().getDesired(), is(json("{\"a\":{\"b\":{\"d\":2}}}")));
        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"a\":{\"b\":{\"d\":\"old\"}}}")));
    }

    // Row 6 in the PR description.
    @Test
    void GIVEN_value_WHEN_replaced_by_object_with_nulls_THEN_stored_nulls_have_timestamps() throws IOException {
        ShadowDocument document = document("{\"a\":1}", null);

        document.update(json("{\"state\":{\"desired\":{\"a\":{\"x\":{\"y\":null},\"w\":{},\"v\":null}}}}"));

        assertThat(document.getState().getDesired(), is(json("{\"a\":{\"x\":{\"y\":null},\"w\":{},\"v\":null}}")));
        assertThat(shape(document.getMetadata().getDesired()),
                is(json("{\"a\":{\"x\":{\"y\":\"ts\"},\"w\":{},\"v\":\"ts\"}}")));
    }

    // Row 7 in the PR description.
    @Test
    void GIVEN_desired_absent_WHEN_array_with_null_stored_THEN_null_element_has_timestamp() throws IOException {
        ShadowDocument document = document(null, "{\"o\":1}");

        document.update(json("{\"state\":{\"desired\":{\"a\":[1,null]}}}"));

        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"a\":[\"ts\",\"ts\"]}")));
    }

    // Row 8 in the PR description.
    @Test
    void GIVEN_metadata_out_of_shape_WHEN_section_updated_THEN_metadata_matches_state() throws IOException {
        ShadowDocument document = new ShadowDocument(new ShadowState(json("{\"a\":1,\"b\":1}"), json("{\"r\":1}")),
                new ShadowStateMetadata(json("{\"a\":{\"timestamp\":1},\"c\":{\"timestamp\":1}}"),
                        json("{\"q\":{\"timestamp\":1}}"), mockClock), 1L);

        document.update(json("{\"state\":{\"desired\":{\"a\":2}}}"));

        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"a\":\"ts\",\"b\":\"ts\"}")));
        assertThat(shape(document.getMetadata().getReported()), is(json("{\"q\":\"old\"}")));
    }

    // Row 9 in the PR description.
    @Test
    void GIVEN_update_with_null_leaves_WHEN_update_THEN_response_has_timestamps_for_nulls() throws IOException {
        ShadowDocument document = document("{\"a\":1}", null);

        JsonNode response = document.update(json("{\"state\":{\"desired\":{\"a\":null,\"b\":2,\"c\":{\"x\":null}}}}"));

        assertThat(shape(response), is(json("{\"desired\":{\"a\":\"ts\",\"b\":\"ts\",\"c\":{\"x\":\"ts\"}}}")));
    }

    // Row 10 in the PR description.
    @Test
    void GIVEN_update_with_null_array_element_WHEN_update_THEN_response_has_timestamp_per_element() throws IOException {
        ShadowDocument document = document(null, null);

        JsonNode response = document.update(json("{\"state\":{\"desired\":{\"a\":[1,null]}}}"));

        assertThat(shape(response), is(json("{\"desired\":{\"a\":[\"ts\",\"ts\"]}}")));
    }

    // Row 11 in the PR description.
    @Test
    void GIVEN_document_WHEN_state_set_to_null_THEN_response_is_timestamp_and_metadata_removed() throws IOException {
        ShadowDocument document = document("{\"a\":1}", "{\"a\":2}");

        JsonNode response = document.update(json("{\"state\":null}"));

        assertThat(shape(response), is(json("\"ts\"")));
        assertThat(document.getMetadata().getDesired(), is(Matchers.nullValue()));
        assertThat(document.getMetadata().getReported(), is(Matchers.nullValue()));
    }

    // Unchanged behavior; see the PR description.
    @Test
    void GIVEN_document_WHEN_one_field_updated_THEN_other_timestamps_and_sections_kept() throws IOException {
        ShadowDocument document = document("{\"a\":{\"x\":1,\"y\":[1,2]},\"b\":1}", "{\"r\":1}");

        document.update(json("{\"state\":{\"desired\":{\"a\":{\"x\":2}}}}"));

        assertThat(shape(document.getMetadata().getDesired()),
                is(json("{\"a\":{\"x\":\"ts\",\"y\":[\"old\",\"old\"]},\"b\":\"old\"}")));
        assertThat(shape(document.getMetadata().getReported()), is(json("{\"r\":\"old\"}")));
    }

    // Unchanged behavior; see the PR description.
    @Test
    void GIVEN_object_WHEN_last_child_removed_THEN_empty_object_kept_in_metadata() throws IOException {
        ShadowDocument document = document("{\"a\":{\"x\":1},\"b\":1}", null);

        document.update(json("{\"state\":{\"desired\":{\"a\":{\"x\":null}}}}"));

        assertThat(document.getState().getDesired(), is(json("{\"a\":{},\"b\":1}")));
        assertThat(shape(document.getMetadata().getDesired()), is(json("{\"a\":{},\"b\":\"old\"}")));
    }
}
