/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    private final String desiredPatchString = "{\"desired\": {\"id\": 100, \"SomeObject\": {\"SomeChild1\": \"SomeValue\", \"SomeChild2\": {\"SomeChild3\": true}}}}";
    private final String reportedPatchString = "{\"reported\": {\"id\": 100, \"SomeObject\": {\"SomeChild1\": \"SomeValue\", \"SomeChild2\": {\"SomeChild3\": true}}}}";
    private final String patchMetadataString = "{\"id\": 12345, \"SomeObject\": {\"SomeChild1\": 12345, \"SomeChild2\": {\"SomeChild3\": 12345}}}";
    private final long timestamp = 1617731792L;

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
        assertThat(patchToCheck.get("SomeObject").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));

        assertTrue(patchToCheck.has("id"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("id")));
        assertTrue(patchToCheck.get("id").has("NewIdChild"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("id").get("NewIdChild")));
        assertThat(patchToCheck.get("id").get("NewIdChild").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));

        assertTrue(patchToCheck.has("NewObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("NewObject")));
        assertTrue(patchToCheck.get("NewObject").has("NewObjectChild"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("NewObject").get("NewObjectChild")));
        assertThat(patchToCheck.get("NewObject").get("NewObjectChild").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));
    }

    private void checkNodeForMetadata(JsonNode patchToCheck) {
        assertTrue(patchToCheck.has("id"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("id")));
        assertThat(patchToCheck.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));

        assertTrue(patchToCheck.has("SomeObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject")));
        assertTrue(patchToCheck.get("SomeObject").has("SomeChild1"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject").get("SomeChild1")));
        assertThat(patchToCheck.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));

        assertTrue(patchToCheck.get("SomeObject").has("SomeChild2"));
        assertFalse(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject").get("SomeChild2")));
        assertTrue(patchToCheck.get("SomeObject").get("SomeChild2").has("SomeChild3"));
        assertTrue(ShadowStateMetadata.isMetadataNode(patchToCheck.get("SomeObject").get("SomeChild2").get("SomeChild3")));
        assertThat(patchToCheck.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));
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
            assertThat(jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));
        });

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));
        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomeArray"));
        patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomeArray").forEach(jsonNode -> {
            assertTrue(ShadowStateMetadata.isMetadataNode(jsonNode));
            assertThat(jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));
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

        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getDesired()));
        assertTrue(JsonUtil.isNullOrMissing(shadowStateMetadata.getReported()));

        assertFalse(JsonUtil.isNullOrMissing(patchMetadata));
        assertTrue(patchMetadata.has(SHADOW_DOCUMENT_STATE_DESIRED));
        assertTrue(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).has("SomObject"));
        assertThat(patchMetadata.get(SHADOW_DOCUMENT_STATE_DESIRED).get("SomObject").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(timestamp));
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
        assertThat(metadataJson, Matchers.is(expectedMetadataJson));
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
        assertThat(deltaMetadata.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));

        assertTrue(deltaMetadata.has("SomeObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject")));
        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild1"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));
        assertEquals(12345, deltaMetadata.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong());

        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild2"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject").get("SomeChild2")));
        assertTrue(deltaMetadata.get("SomeObject").get("SomeChild2").has("SomeChild3"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));
        assertEquals(12345, deltaMetadata.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong());

        assertTrue(deltaMetadata.has("SomeArray"));
        deltaMetadata.get("SomeArray").forEach(jsonNode -> {
            assertEquals(12345, jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong());
            assertThat(jsonNode.get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));
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
        assertThat(deltaMetadata.get("id").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));

        assertTrue(deltaMetadata.has("SomeObject"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject")));
        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild1"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild1").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));

        assertTrue(deltaMetadata.get("SomeObject").has("SomeChild2"));
        assertFalse(ShadowStateMetadata.isMetadataNode(deltaMetadata.get("SomeObject").get("SomeChild2")));
        assertTrue(deltaMetadata.get("SomeObject").get("SomeChild2").has("SomeChild3"));
        assertThat(deltaMetadata.get("SomeObject").get("SomeChild2").get("SomeChild3").get(SHADOW_DOCUMENT_TIMESTAMP).asLong(), Matchers.is(12345L));

    }


    @Test
    void GIVEN_metadata_WHEN_deepCopy_THEN_gets_new_instance_of_metadata() throws IOException {
        final String patchMetadataString = "{\"id\": {\"timestamp\": 12345}, \"SomeObject\": {\"SomeChild1\": {\"timestamp\": 12345}, \"SomeChild2\": {\"SomeChild3\": {\"timestamp\": 12345}}}}";
        Optional<JsonNode> patchMetadataJson = JsonUtil.getPayloadJson(patchMetadataString.getBytes());
        assertTrue(patchMetadataJson.isPresent());
        ShadowStateMetadata shadowStateMetadata = new ShadowStateMetadata(patchMetadataJson.get(), null, mockClock);
        ShadowStateMetadata deepCopiedMetadata = shadowStateMetadata.deepCopy();
        assertThat(deepCopiedMetadata, Matchers.is(Matchers.not(shadowStateMetadata)));
        assertThat(deepCopiedMetadata.toJson(), Matchers.is(shadowStateMetadata.toJson()));
    }

}
