/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.util.Utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_THING_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PATTERN;

public final class Validator {
    private static final Pattern PATTERN = Pattern.compile(SHADOW_PATTERN);

    private Validator() {
    }

    /**
     * Validates the shadow request if the thing name and shadow name has valid length and pattern.
     *
     * @param shadowRequest The shadow request object containing the thingName and shadowName
     */
    static void validateShadowRequest(ShadowRequest shadowRequest) {
        String thingName = shadowRequest.getThingName();
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

        String shadowName = shadowRequest.getShadowName();
        if (Utils.isEmpty(shadowName)) {
            return;
        }

        if (shadowName.length() > MAX_SHADOW_NAME_LENGTH) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidShadowNameMessage(String.format(
                    "ShadowName has a maximum length of %d", MAX_SHADOW_NAME_LENGTH)));
        }

        matcher = PATTERN.matcher(shadowName);
        if (!matcher.matches()) {
            throw new InvalidRequestParametersException(ErrorMessage.createInvalidShadowNameMessage(String.format(
                    "ShadowName must match pattern %s", SHADOW_PATTERN)));
        }
    }
}
