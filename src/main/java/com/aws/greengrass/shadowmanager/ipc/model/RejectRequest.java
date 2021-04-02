/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc.model;

import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import lombok.Builder;
import lombok.Getter;

/**
 * Class containing the details when a shadow operation is rejected.
 */
@Getter
public class RejectRequest extends IPCRequest {
    // TODO: change this to be byte payload and use the ResponseBuilder class.
    ErrorMessage errorMessage;
    Operation publishOperation;

    /**
     * Constructor/Builder for Reject request class.
     *
     * @param thingName        The thing name
     * @param shadowName       The name of the shadow on which the operation was requested
     * @param errorMessage     The ErrorMessage class containing the error information for rejection
     * @param publishOperation The Operation type to be performed on the shadow
     */
    @Builder
    public RejectRequest(String thingName, String shadowName, ErrorMessage errorMessage,
                         Operation publishOperation) {
        super(thingName, shadowName);
        this.errorMessage = errorMessage;
        this.publishOperation = publishOperation;
    }
}
