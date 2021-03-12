/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import lombok.Getter;

public class InvalidRequestParametersException extends RuntimeException {
    @Getter
    private final ErrorMessage errorMessage;

    public InvalidRequestParametersException(String message, ErrorMessage errorMessage) {
        super(message);
        this.errorMessage = errorMessage;
    }

    public InvalidRequestParametersException(ErrorMessage errorMessage) {
        super(errorMessage.getMessage());
        this.errorMessage = errorMessage;
    }
}
