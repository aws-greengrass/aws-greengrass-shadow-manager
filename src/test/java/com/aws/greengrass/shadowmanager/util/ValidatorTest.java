/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ValidatorTest {

    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private static Stream<Arguments> invalidShadowRequests() {
        String invalidNameLength = "invalidThingNameLengthOver128Characters----------------------------------------------------------------------------------------------------";
        String invalidPattern = "InvalidPatternName!@#";

        return Stream.of(
                Arguments.of(null, SHADOW_NAME, "ThingName is missing"),
                Arguments.of("", SHADOW_NAME, "ThingName is missing"),
                Arguments.of(invalidNameLength, SHADOW_NAME, "ThingName has a maximum"),
                Arguments.of(invalidPattern, SHADOW_NAME, "ThingName must match"),

                Arguments.of(THING_NAME, invalidNameLength, "ShadowName has a maximum"),
                Arguments.of(THING_NAME, invalidPattern, "ShadowName must match")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidShadowRequests")
    void GIVEN_invalid_shadow_request_WHEN_validate_shadow_request_THEN_throw_invalid_request_parameters_exception(String thingName, String shadowName, String errorMessage) {
        ShadowRequest shadowRequest = new ShadowRequest(thingName, shadowName);
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> Validator.validateShadowRequest(shadowRequest));
        assertThat(thrown.getMessage(), startsWith(errorMessage));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {SHADOW_NAME})
    void GIVEN_valid_shadow_request_WHEN_validate_shadow_request_THEN_do_nothing(String shadowName) {
        ShadowRequest shadowRequest = new ShadowRequest(THING_NAME, shadowName);
        assertDoesNotThrow(() -> Validator.validateShadowRequest(shadowRequest));
    }
}