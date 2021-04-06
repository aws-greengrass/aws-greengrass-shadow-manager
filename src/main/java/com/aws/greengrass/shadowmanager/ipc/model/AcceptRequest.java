/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc.model;

import lombok.Builder;
import lombok.Getter;

/**
 * Class containing the details when a shadow operation is accepted.
 */
@Getter
public class AcceptRequest extends IPCRequest {
    byte[] payload;

    /**
     * Constructor/Builder for Accept request class.
     *
     * @param thingName        The thing name
     * @param shadowName       The name of the shadow on which the operation was requested
     * @param payload          The payload to be published
     * @param publishOperation The Operation type to be performed on the shadow
     */
    @Builder
    public AcceptRequest(String thingName, String shadowName, byte[] payload,
                         Operation publishOperation) {
        super(thingName, shadowName, publishOperation);
        this.payload = payload;
    }
}
