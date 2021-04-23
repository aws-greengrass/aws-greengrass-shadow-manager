/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public final class TestUtils {

    public static final String TEST_SERVICE = "TestService";
    public static final String THING_NAME = "testThingName";
    public static final String SHADOW_NAME = "testShadowName";
    public static final String INVALID_NAME_PATTERN = "invalidPattern$!@#";
    public static final String INVALID_NAME_LENGTH = "invalidThingOrShadowNameLengthOver128Characters----------------------------------------------------------------------------------------------------";
    public final static String SAMPLE_EXCEPTION_MESSAGE = "sample exception message";


    static Stream<Arguments> invalidThingAndShadowName() {
        return Stream.of(
                arguments(null, SHADOW_NAME),
                arguments("", SHADOW_NAME),
                arguments(INVALID_NAME_PATTERN, SHADOW_NAME),
                arguments(INVALID_NAME_LENGTH, SHADOW_NAME),
                arguments(THING_NAME, INVALID_NAME_PATTERN),
                arguments(THING_NAME, INVALID_NAME_LENGTH)
        );
    }

    static Stream<Arguments> invalidThingNames() {
        return Stream.of(
                arguments(INVALID_NAME_PATTERN),
                arguments(INVALID_NAME_LENGTH)
        );
    }

    static Stream<Arguments> invalidShadowNames() {
        return Stream.of(
                arguments(INVALID_NAME_PATTERN),
                arguments(INVALID_NAME_LENGTH)
        );
    }

    private TestUtils() {
    }
}
