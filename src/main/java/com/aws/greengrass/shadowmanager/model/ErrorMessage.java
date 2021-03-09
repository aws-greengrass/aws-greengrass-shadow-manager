/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Class that manages error messages to send when a Shadow Operation is rejected.
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorMessage {
    private int errorCode;
    private String message;
    private long timestamp;
    // TODO: Set the client token correctly based on the Shadow Request.
    private String clientToken;

    public static final ErrorMessage INVALID_VERSION_MESSAGE =
            ErrorMessage.builder().errorCode(400).message("Invalid version").build();

    public static final ErrorMessage INVALID_CLIENT_TOKEN_MESSAGE =
            ErrorMessage.builder().errorCode(400).message("Invalid clientToken").build();

    public static final ErrorMessage UNAUTHORIZED_MESSAGE =
            ErrorMessage.builder().errorCode(401).message("Unauthorized").build();

    public static final ErrorMessage FORBIDDEN_MESSAGE =
            ErrorMessage.builder().errorCode(403).message("Forbidden").build();

    /**
     * Creates the error message when the thing is not found.
     *
     * @return the ErrorMessage object for thing Not Found exception.
     */
    public static ErrorMessage createThingNotFoundMessage() {
       return ErrorMessage.builder().errorCode(404).timestamp(Instant.now().toEpochMilli())
                .message("Thing not found").build();
    }

    /**
     * Creates the error message when the shadow is not found.
     *
     * @param shadowName The name of the shdaow.
     * @return the ErrorMessage object for Shadow Not Found exception.
     */
    public static ErrorMessage createShadowNotFoundMessage(String shadowName) {
        shadowName = Utils.isEmpty(shadowName) ? "Unnamed Shadow" : shadowName;
        return ErrorMessage.builder().errorCode(404).timestamp(Instant.now().toEpochMilli())
                .message(String.format("No shadow exists with name: %s", shadowName)).build();
    }

    /**
     * Creates the error message when there is a version conflict in the request. The version of the
     * update should be exactly one higher than the last received update.
     *
     * @return the ErrorMessage object for Version Conflict exception.
     */
    public static ErrorMessage createVersionConflictMessage() {
        return ErrorMessage.builder().errorCode(409).timestamp(Instant.now().toEpochMilli())
                .message("Version conflict").build();
    }

    /**
     * Creates the error message when there is an internal server error.
     *
     * @return the ErrorMessage object for Internal Service Failure exception.
     */
    public static ErrorMessage createInternalServiceErrorMessage() {
        return ErrorMessage.builder().errorCode(500).timestamp(Instant.now().toEpochMilli())
                .message("Internal service failure").build();
    }
}
