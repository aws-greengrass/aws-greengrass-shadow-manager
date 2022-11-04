/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import com.aws.greengrass.util.Utils;
import lombok.Getter;
import lombok.Setter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_THING_NAME_LENGTH;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_PATTERN;

public final class Validator {
    private static final Pattern PATTERN = Pattern.compile(SHADOW_PATTERN);
    @Getter
    @Setter
    private static int maxShadowDocumentSize = DEFAULT_DOCUMENT_SIZE;

    private Validator() {
    }

    /**
     * Validates the shadow request if the thing name and shadow name has valid length and pattern.
     *
     * @param shadowRequest The shadow request object containing the thingName and shadowName
     * @throws InvalidRequestParametersException if the thing name or shadow name validation fails.
     */
    public static void validateShadowRequest(ShadowRequest shadowRequest) {
        validateThingName(shadowRequest.getThingName());
        validateShadowName(shadowRequest.getShadowName());
    }

    /**
     * Validates the shadow name has valid length and pattern.
     *
     * @param shadowName The shadow name to validate.
     * @throws InvalidRequestParametersException if the shadow name validation fails.
     */
    public static void validateShadowName(String shadowName) {
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

    /**
     * Validates the thing name has valid length and pattern.
     *
     * @param thingName The thing name to validate.
     * @throws InvalidRequestParametersException if the thing name validation fails.
     */
    public static void validateThingName(String thingName) {
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
     * Validates the maximum shadow size is within the appropriate limits.
     *
     * @param newMaxShadowSize The new max shadow size
     * @throws InvalidConfigurationException if the new max shadow size is less than 0 or more than the default
     *                                       max size (30 MB).
     */
    public static void validateMaxShadowSize(int newMaxShadowSize) throws InvalidConfigurationException {
        if (MAX_SHADOW_DOCUMENT_SIZE < newMaxShadowSize || newMaxShadowSize <= 0) {
            throw new InvalidConfigurationException(String.format(
                    "Maximum shadow size provided %d is either less than 0 "
                            + "or exceeds default maximum shadow size of %d",
                    newMaxShadowSize,
                    MAX_SHADOW_DOCUMENT_SIZE));
        }
    }

    /**
     * Validates the maximum outbound sync updates per second is within the appropriate limits.
     *
     * @param newMaxOutboundSyncUpdatesPerSecond The new max outbound sync updates per second
     * @throws InvalidConfigurationException if the new outbound sync updates per second is less than 0.
     */
    public static void validateOutboundSyncUpdatesPerSecond(int newMaxOutboundSyncUpdatesPerSecond) {
        if (newMaxOutboundSyncUpdatesPerSecond <= 0) {
            throw new InvalidConfigurationException(String.format(
                    "Maximum outbound sync update per second provided %d is invalid. It should be greater than 0.",
                    newMaxOutboundSyncUpdatesPerSecond));
        }
    }

    /**
     * Validates the total maximum inbound shadow requests rate is within the appropriate limits.
     *
     * @param maxTotalLocalRequestRate The combined total local shadow request rate
     * @throws InvalidConfigurationException if the new total local shadow requests rate is less than 0.
     */
    public static void validateTotalLocalRequestRate(int maxTotalLocalRequestRate) {
        if (maxTotalLocalRequestRate <= 0) {
            throw new InvalidConfigurationException(String.format(
                    "Maximum total local shadow request rate per second provided %d is invalid. It should be "
                            + "greater than 0.", maxTotalLocalRequestRate));
        }
    }

    /**
     * Validates the maximum inbound shadow requests rate per thing is within the appropriate limits.
     *
     * @param maxLocalShadowRequestsPerThingPerSecond The new max local shadow requests rate per thing
     * @throws InvalidConfigurationException if the new local shadow requests rate per thing is less than 0.
     */
    public static void validateLocalShadowRequestsPerThingPerSecond(int maxLocalShadowRequestsPerThingPerSecond) {
        if (maxLocalShadowRequestsPerThingPerSecond <= 0) {
            throw new InvalidConfigurationException(String.format(
                    "Maximum local shadow requests per thing per second provided %d is invalid. It should be "
                            + "greater than 0.", maxLocalShadowRequestsPerThingPerSecond));
        }
    }

    /**
     * Validate that a shadow size does not exceed the maximum.
     * @param size size of shadow
     * @throws InvalidRequestParametersException if the shadow is too large
     */
    public static void validateShadowSize(int size) throws InvalidRequestParametersException {
        if (size > maxShadowDocumentSize) {
            throw new InvalidRequestParametersException(ErrorMessage.PAYLOAD_TOO_LARGE_MESSAGE);
        }
    }

}
