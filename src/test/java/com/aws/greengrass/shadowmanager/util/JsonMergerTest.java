/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class JsonMergerTest {
    private final static String SOURCE_NODE_STRING = "{\"id\": 100, \"SomeKey\": \"SomeValue\",\"SomeObjectKey\": {\"foo\": \"bar\"}}";
    private final static String SOURCE_STATE_NODE_STRING = "{\"state\":{\"id\": 100, \"SomeKey\": \"SomeValue\",\"SomeObjectKey\": {\"foo\": \"bar\"}}}";
    private final static String SOURCE_NODE_WITH_ARRAY_STRING = "{\"id\": 100, \"SomeArrayKey\": [\"SomeValue1\", \"SomeValue2\"]}";
    private final static String SOURCE_NODE_WITH_ARRAY_NODE_STRING = "[\"SomeValue1\", \"SomeValue2\"]";
    private final static String PATCH_NODE_WITH_NEW_FIELD_STRING = "{\"NewKey\": true, \"NewNullParent\": {\"NewNullChild1\": null, \"NewNullChild2\": {\"NewNullChild3\": null}}, \"NewChildLevel1\": {\"NewChildLevel2\": {\"NewChildLevel3\":\"NewChildValue\"}}}";
    private final static String MERGED_NODE_WITH_NEW_FIELD_STRING = "{\"id\": 100, \"SomeKey\": \"SomeValue\",\"SomeObjectKey\": {\"foo\": \"bar\"}, \"NewKey\": true, \"NewChildLevel1\": {\"NewChildLevel2\": {\"NewChildLevel3\":\"NewChildValue\"}}}";
    private final static String PATCH_NODE_WITH_NULL_FIELD_STRING = "{\"SomeKey\": null,\"SomeObjectKey\": {\"foo\": null}}";
    private final static String MERGED_NODE_WITHOUT_NULL_FIELD_STRING = "{\"id\": 100,\"SomeObjectKey\":{}}";
    private final static String MERGED_NODE_WITH_UPDATED_ARRAY_NODE_STRING = "[\"SomeValue3\", \"SomeValue4\"]";
    private final static String PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING = "[\"SomeValue3\", \"SomeValue4\"]";
    private final static String EMPTY_DOCUMENT = "{}";
    private final static String EMPTY_STATE_DOCUMENT = "{\"state\": {}}";
    private final static String NULL_DOCUMENT = "null";
    private final static String NULL_STATE_DOCUMENT = "{\"state\": null}";
    private static JsonNode sourceNodeWithArray;

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> mergeTestInput() {
        return Stream.of(
                Arguments.of("GIVEN patch with new node, THEN add new field in source node", SOURCE_NODE_STRING, PATCH_NODE_WITH_NEW_FIELD_STRING, MERGED_NODE_WITH_NEW_FIELD_STRING),
                Arguments.of("GIVEN patch with null nodes, THEN removes all null nodes in source node", SOURCE_NODE_STRING, PATCH_NODE_WITH_NULL_FIELD_STRING, MERGED_NODE_WITHOUT_NULL_FIELD_STRING),
                Arguments.of("GIVEN patch with new nodes in an array node, THEN replaces all the elements in the source with the elements in the patch", SOURCE_NODE_WITH_ARRAY_NODE_STRING, PATCH_NODE_WITH_ARRAY_VALUE_NODE_STRING, MERGED_NODE_WITH_UPDATED_ARRAY_NODE_STRING),
                Arguments.of("GIVEN empty patch, THEN source node is unaffected", SOURCE_NODE_STRING, EMPTY_DOCUMENT, SOURCE_NODE_STRING),
                Arguments.of("GIVEN null patch, THEN source node is cleared", SOURCE_NODE_STRING, NULL_DOCUMENT, EMPTY_DOCUMENT),
                Arguments.of("GIVEN empty source and non-empty patch, THEN patch is the result", EMPTY_DOCUMENT, SOURCE_NODE_STRING, SOURCE_NODE_STRING),
                Arguments.of("GIVEN empty state patch, THEN source node is unaffected", SOURCE_STATE_NODE_STRING, EMPTY_STATE_DOCUMENT, SOURCE_STATE_NODE_STRING),
                Arguments.of("GIVEN null state patch, THEN source node is cleared", SOURCE_STATE_NODE_STRING, NULL_STATE_DOCUMENT, EMPTY_STATE_DOCUMENT),
                Arguments.of("GIVEN empty source and non-empty state patch, THEN patch is the result", EMPTY_STATE_DOCUMENT, SOURCE_STATE_NODE_STRING, SOURCE_STATE_NODE_STRING),
                Arguments.of("GIVEN null source and non-empty state patch, THEN patch is the result", NULL_STATE_DOCUMENT, SOURCE_STATE_NODE_STRING, SOURCE_STATE_NODE_STRING),
                Arguments.of("GIVEN state patch containing null state subkey, THEN state subkey is removed",
                        "{\"state\":{\"key\":\"v\",\"state\":\"abc\"}}", "{\"state\": {\"state\":null}}",
                        "{\"state\":{\"key\":\"v\"}}")
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

    // Expected values were recorded from IoT Device Shadow.
    static Stream<Arguments> shadowStateMergeInput() {
        return Stream.of(
                // current desired, desired in the update, expected desired
                // desired absent: null leaves are removed, objects left empty by that are dropped
                Arguments.of(null, "{\"a\":null,\"b\":2}", "{\"b\":2}"),
                Arguments.of(null, "{\"a\":null}", null),
                Arguments.of(null, "{\"a\":{\"x\":null},\"b\":2}", "{\"b\":2}"),
                Arguments.of(null, "{\"a\":{\"x\":null,\"y\":1}}", "{\"a\":{\"y\":1}}"),
                Arguments.of(null, "{\"a\":{\"b\":{\"c\":null,\"d\":1},\"e\":null}}", "{\"a\":{\"b\":{\"d\":1}}}"),
                // desired absent: empty objects are kept
                Arguments.of(null, "{\"a\":{}}", "{\"a\":{}}"),
                Arguments.of(null, "{\"a\":{\"b\":{},\"c\":null}}", "{\"a\":{\"b\":{}}}"),
                Arguments.of(null, "{}", null),
                // arrays are stored as is
                Arguments.of(null, "{\"a\":[1,null,{\"x\":null}]}", "{\"a\":[1,null,{\"x\":null}]}"),
                // null removes the field; an object emptied by that is kept
                Arguments.of("{\"a\":{\"x\":1},\"b\":1}", "{\"a\":{\"x\":null}}", "{\"a\":{},\"b\":1}"),
                Arguments.of("{\"a\":{\"x\":1,\"y\":2}}", "{\"a\":{\"x\":null}}", "{\"a\":{\"y\":2}}"),
                Arguments.of("{\"b\":1}", "{\"nope\":null}", "{\"b\":1}"),
                Arguments.of("{\"a\":1}", "{\"a\":null}", null),
                // an empty object changes nothing, at any level
                Arguments.of("{\"a\":{\"x\":1},\"b\":1}", "{\"a\":{}}", "{\"a\":{\"x\":1},\"b\":1}"),
                Arguments.of("{\"a\":{\"b\":{\"c\":1}}}", "{\"a\":{\"b\":{}}}", "{\"a\":{\"b\":{\"c\":1}}}"),
                Arguments.of("{\"z\":1}", "{}", "{\"z\":1}"),
                // an empty object is added to a missing field and replaces a value
                Arguments.of("{\"z\":1}", "{\"a\":{}}", "{\"z\":1,\"a\":{}}"),
                Arguments.of("{\"a\":1}", "{\"a\":{}}", "{\"a\":{}}"),
                // an object for a missing field is added without nulls
                Arguments.of("{\"z\":1}", "{\"a\":{\"x\":null}}", "{\"z\":1}"),
                Arguments.of("{\"z\":1,\"a\":{}}", "{\"a\":{\"x\":null}}", "{\"z\":1,\"a\":{}}"),
                // an object replacing a value or an array is stored as is, nulls included
                Arguments.of("{\"a\":1}", "{\"a\":{\"x\":null}}", "{\"a\":{\"x\":null}}"),
                Arguments.of("{\"a\":[1,2]}", "{\"a\":{\"x\":null,\"y\":1}}", "{\"a\":{\"x\":null,\"y\":1}}"),
                // a stored null is not removed by another null
                Arguments.of("{\"a\":{\"x\":null}}", "{\"a\":{\"x\":null}}", "{\"a\":{\"x\":null}}"),
                // a stored null is replaced like a missing field
                Arguments.of("{\"a\":{\"x\":null}}", "{\"a\":{\"x\":{\"y\":null,\"k\":1}}}", "{\"a\":{\"x\":{\"k\":1}}}"),
                // arrays and values replace any field
                Arguments.of("{\"a\":{\"x\":1}}", "{\"a\":[null,{\"y\":null}]}", "{\"a\":[null,{\"y\":null}]}"),
                Arguments.of("{\"a\":[1,2]}", "{\"a\":3}", "{\"a\":3}"),
                // a field named "state" is an ordinary field
                Arguments.of(null, "{\"state\":null,\"b\":2}", "{\"b\":2}"),
                Arguments.of(null, "{\"state\":{},\"b\":2}", "{\"state\":{},\"b\":2}"),
                Arguments.of("{\"z\":1}", "{\"state\":null,\"b\":2}", "{\"z\":1,\"b\":2}"),
                Arguments.of("{\"z\":1}", "{\"state\":{},\"b\":2}", "{\"z\":1,\"state\":{},\"b\":2}"),
                Arguments.of("{\"state\":{\"q\":1},\"m\":\"on\"}", "{\"state\":null}", "{\"m\":\"on\"}"),
                Arguments.of("{\"state\":{\"q\":1},\"m\":\"on\"}", "{\"x\":1}",
                        "{\"state\":{\"q\":1},\"m\":\"on\",\"x\":1}"),
                Arguments.of("{\"state\":{\"q\":1}}", "{\"state\":{\"x\":1}}", "{\"state\":{\"q\":1,\"x\":1}}"));
    }

    @ParameterizedTest
    @MethodSource("shadowStateMergeInput")
    void GIVEN_section_and_update_WHEN_mergeShadowState_THEN_matches_iot_device_shadow(String current, String patch,
            String expected) throws IOException {
        JsonNode currentNode = current == null ? null : JsonUtil.getPayloadJson(current.getBytes()).get();
        JsonNode patchNode = JsonUtil.getPayloadJson(patch.getBytes()).get();
        JsonNode copy = patchNode.deepCopy();

        JsonNode merged = JsonMerger.mergeShadowState(currentNode, patchNode);

        assertThat(merged, Matchers.is(expected == null ? null : JsonUtil.getPayloadJson(expected.getBytes()).get()));
        assertThat(patchNode, Matchers.is(copy));
    }

    private static JsonNode json(String s) throws IOException {
        return s == null ? null : JsonUtil.getPayloadJson(s.getBytes()).get();
    }

    private static JsonNode apply(JsonNode state, JsonNode patch) {
        return JsonMerger.applyShadowStateUpdate(state, patch);
    }

    static Stream<Arguments> diffInput() {
        return Stream.of(
                // from state, to state, expected update state
                Arguments.of("{\"desired\":{\"a\":1}}", "{\"desired\":{\"a\":1}}", "{}"),
                Arguments.of("{\"desired\":{\"a\":1,\"b\":2}}", "{\"desired\":{\"a\":1}}", "{\"desired\":{\"b\":null}}"),
                Arguments.of("{\"desired\":{\"a\":{\"x\":1,\"y\":2}}}", "{\"desired\":{\"a\":{}}}",
                        "{\"desired\":{\"a\":{\"x\":null,\"y\":null}}}"),
                Arguments.of("{\"desired\":{\"a\":{\"x\":1}}}", "{\"desired\":{\"a\":5}}", "{\"desired\":{\"a\":5}}"),
                Arguments.of("{\"desired\":{\"a\":5}}", "{\"desired\":{\"a\":{\"x\":null,\"y\":1}}}",
                        "{\"desired\":{\"a\":{\"x\":null,\"y\":1}}}"),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"z\":1,\"a\":{}}}", "{\"desired\":{\"a\":{}}}"),
                Arguments.of("{\"desired\":{\"a\":[1,2]}}", "{\"desired\":{\"a\":[null]}}", "{\"desired\":{\"a\":[null]}}"),
                Arguments.of("{\"desired\":{\"a\":1},\"reported\":{\"r\":1}}", "{\"reported\":{\"r\":1}}", "{\"desired\":null}"),
                Arguments.of(null, "{\"reported\":{\"r\":{\"s\":{}}}}", "{\"reported\":{\"r\":{\"s\":{}}}}"),
                Arguments.of("{\"desired\":{\"state\":{\"q\":1}}}", "{\"desired\":{\"state\":{\"q\":2}}}",
                        "{\"desired\":{\"state\":{\"q\":2}}}")
        );
    }

    @ParameterizedTest
    @MethodSource("diffInput")
    void GIVEN_two_states_WHEN_diff_THEN_update_turns_one_into_the_other(String from, String to, String expected)
            throws IOException {
        Optional<ObjectNode> diff = JsonMerger.diffShadowState(json(from), json(to));

        assertThat(diff.isPresent(), Matchers.is(true));
        assertThat(diff.get(), Matchers.is(json(expected)));
        assertThat(apply(json(from), diff.get()), Matchers.is(apply(json(to), json("{}"))));
    }

    static Stream<Arguments> inexpressibleDiffInput() {
        return Stream.of(
                // a null stored inside an object can't be removed or added by an update
                Arguments.of("{\"desired\":{\"a\":{\"x\":null,\"y\":1}}}", "{\"desired\":{\"a\":{\"y\":1}}}"),
                Arguments.of("{\"desired\":{\"a\":{\"y\":1}}}", "{\"desired\":{\"a\":{\"x\":null,\"y\":1}}}"),
                Arguments.of("{\"desired\":{\"z\":1}}", "{\"desired\":{\"z\":1,\"a\":{\"x\":null}}}")
        );
    }

    @ParameterizedTest
    @MethodSource("inexpressibleDiffInput")
    void GIVEN_change_of_stored_null_WHEN_diff_THEN_no_update(String from, String to) throws IOException {
        assertThat(JsonMerger.diffShadowState(json(from), json(to)).isPresent(), Matchers.is(false));
    }

    private static JsonNode randomValue(Random random, int depth) {
        int r = random.nextInt(100);
        if (r < 15) {
            return JsonUtil.OBJECT_MAPPER.nullNode();
        }
        if (r < 25) {
            return JsonUtil.OBJECT_MAPPER.createObjectNode();
        }
        if (r < 55 && depth < 3) {
            return randomObject(random, depth + 1);
        }
        if (r < 62) {
            return JsonUtil.OBJECT_MAPPER.createArrayNode().add(random.nextInt(3)).addNull();
        }
        return JsonUtil.OBJECT_MAPPER.getNodeFactory().numberNode(random.nextInt(4));
    }

    private static ObjectNode randomObject(Random random, int depth) {
        ObjectNode node = JsonUtil.OBJECT_MAPPER.createObjectNode();
        for (int i = 1 + random.nextInt(2); i > 0; i--) {
            node.set(new String[]{"a", "b", "c"}[random.nextInt(3)], randomValue(random, depth));
        }
        return node;
    }

    private static JsonNode randomState(Random random) {
        JsonNode state = null;
        for (int i = random.nextInt(4); i > 0; i--) {
            ObjectNode update = JsonUtil.OBJECT_MAPPER.createObjectNode();
            update.set(random.nextBoolean() ? "desired" : "reported", random.nextInt(10) == 0
                    ? JsonUtil.OBJECT_MAPPER.nullNode() : randomObject(random, 1));
            state = apply(state, update);
        }
        return state;
    }

    @Test
    void GIVEN_random_states_WHEN_diff_THEN_update_reaches_the_target_or_is_not_expressible() {
        Random random = new Random(227);
        List<String> failures = new ArrayList<>();
        int expressible = 0;
        for (int i = 0; i < 5000; i++) {
            JsonNode from = randomState(random);
            JsonNode to = randomState(random);
            Optional<ObjectNode> diff = JsonMerger.diffShadowState(from, to);
            if (!diff.isPresent()) {
                continue;
            }
            expressible++;
            JsonNode reached = apply(from, diff.get());
            if (!reached.equals(apply(to, JsonUtil.OBJECT_MAPPER.createObjectNode()))) {
                failures.add(from + " -> " + to + " via " + diff.get() + " reached " + reached);
            }
        }
        assertThat(failures.toString(), failures.isEmpty(), Matchers.is(true));
        assertThat(expressible, greaterThan(4500));
    }
}
