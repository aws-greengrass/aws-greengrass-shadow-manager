/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Expected values were recorded from IoT Device Shadow.
 */
@ExtendWith({GGExtension.class})
class ShadowStateTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(String s) throws IOException {
        return s == null ? null : MAPPER.readTree(s);
    }

    static Stream<Arguments> desiredUpdates() {
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
                Arguments.of("{\"state\":{\"q\":1}}", "{\"state\":{\"x\":1}}", "{\"state\":{\"q\":1,\"x\":1}}")
        );
    }

    @ParameterizedTest
    @MethodSource("desiredUpdates")
    void GIVEN_desired_WHEN_update_desired_THEN_desired_matches_iot_device_shadow(String current, String patch,
            String expected) throws IOException {
        ShadowState state = new ShadowState(json(current), json("{\"o\":1}"));

        state.update(json("{\"desired\":" + patch + "}"));

        assertThat(state.getDesired(), is(json(expected)));
        assertThat(state.getReported(), is(json("{\"o\":1}")));
    }

    @ParameterizedTest
    @MethodSource("desiredUpdates")
    void GIVEN_reported_WHEN_update_reported_THEN_reported_matches_iot_device_shadow(String current, String patch,
            String expected) throws IOException {
        ShadowState state = new ShadowState(json("{\"o\":1}"), json(current));

        state.update(json("{\"reported\":" + patch + "}"));

        assertThat(state.getReported(), is(json(expected)));
        assertThat(state.getDesired(), is(json("{\"o\":1}")));
    }

    @Test
    void GIVEN_desired_WHEN_update_desired_null_THEN_desired_removed() throws IOException {
        ShadowState state = new ShadowState(json("{\"a\":1}"), json("{\"a\":2}"));

        state.update(json("{\"desired\":null}"));

        assertThat(state.getDesired(), is(nullValue()));
        assertThat(state.getReported(), is(json("{\"a\":2}")));
    }

    @Test
    void GIVEN_desired_absent_WHEN_null_leaf_sent_twice_THEN_null_leaf_never_stored() throws IOException {
        // https://github.com/aws-greengrass/aws-greengrass-shadow-manager/issues/227
        ShadowState state = new ShadowState(null, json("{\"a\":1,\"b\":1}"));

        state.update(json("{\"desired\":{\"a\":null,\"b\":2}}"));
        state.update(json("{\"desired\":{\"a\":null}}"));

        assertThat(state.getDesired(), is(json("{\"b\":2}")));
        assertThat(state.getDelta(), is(json("{\"b\":2}")));
    }

    @Test
    void GIVEN_update_WHEN_update_THEN_update_request_not_modified() throws IOException {
        JsonNode request = json("{\"desired\":{\"a\":{\"x\":null,\"y\":1},\"b\":[1,null]}}");
        JsonNode copy = request.deepCopy();
        ShadowState state = new ShadowState(json("{\"a\":1}"), null);

        state.update(request);
        ((ObjectNode) state.getDesired().get("a")).put("y", 5);

        assertThat(request, is(copy));
    }
}
