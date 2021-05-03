/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import lombok.Getter;

public class InvalidRequestParametersException extends RuntimeException {
    private static final long serialVersionUID = -1488980916089225328L;
    @Getter
    private final ErrorMessage errorMessage;

    public InvalidRequestParametersException(ErrorMessage errorMessage) {
        super(errorMessage.getMessage());
        this.errorMessage = errorMessage;
    }
}
