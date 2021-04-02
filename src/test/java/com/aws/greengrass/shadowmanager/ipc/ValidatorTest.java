/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ValidatorTest {

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_missing_thing_name_WHEN_validate_thing_name_THEN_throw_invalid_request_parameters_exception(String thingName, ExtensionContext context) {
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> Validator.validateThingName(thingName));
        assertThat(thrown.getMessage(), is(equalTo("ThingName is missing")));
    }

    @Test
    void GIVEN_invalid_thing_name_length_WHEN_validate_thing_name_THEN_throw_invalid_request_parameters_exception(ExtensionContext context) {
        String invalidThingName = "invalidThingNameLengthOver128Characters----------------------------------------------------------------------------------------------------";
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> Validator.validateThingName(invalidThingName));
        assertThat(thrown.getMessage(), startsWith("ThingName has a maximum"));
    }

    @Test
    void GIVEN_invalid_thing_name_pattern_WHEN_validate_thing_name_THEN_throw_invalid_request_parameters_exception(ExtensionContext context) {
        String invalidThingName = "InvalidThingName!@#";
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> Validator.validateThingName(invalidThingName));
        assertThat(thrown.getMessage(), startsWith("ThingName must match"));
    }

    @Test
    void GIVEN_invalid_shadow_name_length_WHEN_validate_shadow_name_THEN_throw_invalid_request_parameters_exception(ExtensionContext context) {
        String invalidShadowName = "invalidShadowNameLengthOver128Characters----------------------------------------------------------------------------------------------------";
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> Validator.validateShadowName(invalidShadowName));
        assertThat(thrown.getMessage(), startsWith("ShadowName has a maximum"));
    }

    @Test
    void GIVEN_invalid_shadow_name_pattern_WHEN_validate_shadow_name_THEN_throw_invalid_request_parameters_exception(ExtensionContext context) {
        String invalidShadowName = "InvalidThingName!@#";
        InvalidRequestParametersException thrown = assertThrows(InvalidRequestParametersException.class, () -> Validator.validateShadowName(invalidShadowName));
        assertThat(thrown.getMessage(), startsWith("ShadowName must match"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_empty_or_null_shadow_name_WHEN_validate_shadow_name_THEN_do_nothing(String shadowName, ExtensionContext context) {
        assertDoesNotThrow(() -> Validator.validateShadowName(shadowName));
    }
}