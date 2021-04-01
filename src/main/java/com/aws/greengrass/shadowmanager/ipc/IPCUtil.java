/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.authorization.AuthorizationHandler;
import com.aws.greengrass.authorization.Permission;
import com.aws.greengrass.authorization.exceptions.AuthorizationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.util.Utils;

import java.util.Optional;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_THING_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_MANAGER_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PATTERN;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_RESOURCE_JOINER;

public final class IPCUtil {
    static final Pattern PATTERN = Pattern.compile(SHADOW_PATTERN);

    public enum LogEvents {
        GET_THING_SHADOW("handle-get-thing-shadow"),
        UPDATE_THING_SHADOW("handle-update-thing-shadow"),
        DELETE_THING_SHADOW("handle-delete-thing-shadow"),
        LIST_NAMED_SHADOWS("handle-list-named-shadows-for-thing");

        String code;

        LogEvents(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }
    }

    private IPCUtil() {
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

    static void doAuthorization(AuthorizationHandler authorizationHandler, String opCode, String serviceName,
                                String thingName) throws AuthorizationException {
        doAuthorization(authorizationHandler, opCode, serviceName, thingName, CLASSIC_SHADOW_IDENTIFIER);
    }

    static void doAuthorization(AuthorizationHandler authorizationHandler, String opCode, String serviceName,
                                String thingName, String shadowName) throws AuthorizationException {
        StringJoiner shadowResource = new StringJoiner("/");
        shadowResource.add(thingName);
        shadowResource.add(SHADOW_RESOURCE_JOINER);

        if (Utils.isNotEmpty(shadowName)) {
            shadowResource.add(shadowName);
        }

        authorizationHandler.isAuthorized(
                SHADOW_MANAGER_NAME,
                Permission.builder()
                        .principal(serviceName)
                        .operation(opCode)
                        .resource(shadowResource.toString())
                        .build());
    }

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
