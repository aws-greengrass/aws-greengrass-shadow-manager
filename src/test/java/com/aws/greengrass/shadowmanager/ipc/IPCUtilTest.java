/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class IPCUtilTest {

    @Test
    void GIVEN_missing_thing_name_WHEN_validate_thing_name_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        // test null thingName
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> IPCUtil.validateThingName(null));
        assertThat(thrown.getMessage(), startsWith("ThingName absent"));

        // test empty string thingName
        String thingName = "";
        thrown = assertThrows(InvalidArgumentsError.class, () -> IPCUtil.validateThingName(thingName));
        assertThat(thrown.getMessage(), startsWith("ThingName absent"));
    }

    @Test
    void GIVEN_invalid_thing_name_length_WHEN_validate_thing_name_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        String invalidThingName = "invalidThingNameLengthOver128Characters----------------------------------------------------------------------------------------------------";
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> IPCUtil.validateThingName(invalidThingName));
        assertThat(thrown.getMessage(), startsWith("ThingName has a maximum"));
    }

    @Test
    void GIVEN_invalid_thing_name_pattern_WHEN_validate_thing_name_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        String invalidThingName = "InvalidThingName!@#";
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> IPCUtil.validateThingName(invalidThingName));
        assertThat(thrown.getMessage(), startsWith("ThingName must match"));
    }

    @Test
    void GIVEN_invalid_shadow_name_length_WHEN_validate_shadow_name_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        String invalidShadowName = "invalidShadowNameLengthOver128Characters----------------------------------------------------------------------------------------------------";
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> IPCUtil.validateShadowName(invalidShadowName));
        assertThat(thrown.getMessage(), startsWith("ShadowName has a maximum"));
    }

    @Test
    void GIVEN_invalid_shadow_name_pattern_WHEN_validate_shadow_name_THEN_throw_invalid_arguments_error(ExtensionContext context) {
        String invalidShadowName = "InvalidThingName!@#";
        InvalidArgumentsError thrown = assertThrows(InvalidArgumentsError.class, () -> IPCUtil.validateShadowName(invalidShadowName));
        assertThat(thrown.getMessage(), startsWith("ShadowName must match"));
    }
}