/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * Class that manages error messages to send when a Shadow Operation is rejected.
 */
@Builder
@AllArgsConstructor
@Getter
public class ErrorMessage implements Serializable {
    private static final long serialVersionUID = -1488980916089225328L;

    private final int errorCode;
    private final String message;

    public static final ErrorMessage INVALID_VERSION_MESSAGE =
            ErrorMessage.builder().errorCode(400).message("Invalid version").build();

    public static final ErrorMessage INVALID_STATE_NODE_DEPTH_MESSAGE =
            ErrorMessage.builder().errorCode(400).message("JSON contains too many levels of nesting; maximum is 6")
                    .build();

    public static final ErrorMessage STATE_OBJECT_NOT_OBJECT_ERROR_MESSAGE =
            ErrorMessage.builder().errorCode(400).message("State node must be an object").build();

    public static final ErrorMessage INVALID_CLIENT_TOKEN_MESSAGE =
            ErrorMessage.builder().errorCode(400).message("Invalid clientToken").build();

    public static final ErrorMessage UNAUTHORIZED_MESSAGE =
            ErrorMessage.builder().errorCode(401).message("Unauthorized").build();

    public static final ErrorMessage PAYLOAD_MISSING_MESSAGE =
                    ErrorMessage.builder().errorCode(400).message("Missing update payload").build();

    public static final ErrorMessage PAYLOAD_TOO_LARGE_MESSAGE =
            ErrorMessage.builder().errorCode(413).message("The payload exceeds the maximum size allowed").build();

    public static final ErrorMessage VERSION_CONFLICT_MESSAGE =
            ErrorMessage.builder().errorCode(409).message("Version conflict").build();

    public static final ErrorMessage INTERNAL_SERVICE_FAILURE_MESSAGE =
            ErrorMessage.builder().errorCode(500).message("Internal service failure").build();

    /**
     * Creates the error message when the payload JSON is not valid.
     *
     * @param errorMessages All the error messages separated by a new line.
     * @return the ErrorMessage object for thing Not Found exception.
     */
    public static ErrorMessage createInvalidPayloadJsonMessage(String errorMessages) {
        return ErrorMessage.builder().errorCode(400).message(String.format("Invalid JSON: %s", errorMessages)).build();
    }

    /**
     * Creates the error message if the thing name is invalid.
     *
     * @param errorMessage the error message for why thing name was invalid.
     * @return the ErrorMessage object for InvalidRequestParametersException.
     */
    public static ErrorMessage createInvalidThingNameMessage(String errorMessage) {
        return ErrorMessage.builder().errorCode(400).message(errorMessage).build();
    }

    /**
     * Creates the error message if shadow name is invalid.
     *
     * @param errorMessage the error message for why shadow name was invalid.
     * @return the ErrorMessage object for InvalidRequestParametersException.
     */
    public static ErrorMessage createInvalidShadowNameMessage(String errorMessage) {
        return ErrorMessage.builder().errorCode(400).message(errorMessage).build();
    }

    /**
     * Creates the error message when the shadow is not found.
     *
     * @param shadowName The name of the shdaow.
     * @return the ErrorMessage object for Shadow Not Found exception.
     */
    public static ErrorMessage createShadowNotFoundMessage(String shadowName) {
        shadowName = Utils.isEmpty(shadowName) ? "Unnamed Shadow" : shadowName;
        return ErrorMessage.builder().errorCode(404)
                .message(String.format("No shadow exists with name: %s", shadowName)).build();
    }
}
