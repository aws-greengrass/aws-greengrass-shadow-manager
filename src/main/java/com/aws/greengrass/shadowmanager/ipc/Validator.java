/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.util.Utils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_THING_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PATTERN;

public final class Validator {
    private static final Pattern PATTERN = Pattern.compile(SHADOW_PATTERN);

    private Validator() {
    }

    /**
     * Checks and returns the CLASSIC_SHADOW_IDENTIFIER if shadowName input was null.
     *
     * @param shadowName The name of the shadow
     * @return The shadowName input or CLASSIC_SHADOW_IDENTIFIER
     */
    static String getClassicShadowIfMissingShadowName(String shadowName) {
        return Optional.ofNullable(shadowName)
                .filter(s -> !s.isEmpty())
                .orElse(CLASSIC_SHADOW_IDENTIFIER);
    }

    /**
     * Validates thingName if it exists and if it has valid length and pattern.
     *
     * @param thingName The thingName of shadow
     */
    static void validateThingName(String thingName) {
        if (Utils.isEmpty(thingName)) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidThingNameMessage(
                    "ThingName is missing"));
        }

        if (thingName.length() > MAX_THING_NAME_LENGTH) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidThingNameMessage(String.format(
                    "ThingName has a maximum length of %d", MAX_THING_NAME_LENGTH)));
        }

        Matcher matcher = PATTERN.matcher(thingName);
        if (!matcher.matches()) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidThingNameMessage(String.format(
                    "ThingName must match pattern %s", SHADOW_PATTERN)));
        }
    }

    /**
     * Validates shadowName if it has valid length and pattern.
     *
     * @param shadowName The name of shadow
     */
    static void validateShadowName(String shadowName) {
        if (Utils.isEmpty(shadowName)) {
            return;
        }

        if (shadowName.length() > MAX_SHADOW_NAME_LENGTH) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidShadowNameMessage(String.format(
                    "ShadowName has a maximum length of %d", MAX_SHADOW_NAME_LENGTH)));
        }

        Matcher matcher = PATTERN.matcher(shadowName);
        if (!matcher.matches()) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidShadowNameMessage(String.format(
                    "ShadowName must match pattern %s", SHADOW_PATTERN)));
        }
    }
}
