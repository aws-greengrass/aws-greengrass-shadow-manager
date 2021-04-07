/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc.model;

import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import lombok.Builder;
import lombok.Getter;

import javax.validation.constraints.NotNull;

/**
 * Class to store information needed for publishing message over local PubSub.
 */
@Getter
public class PubSubRequest extends ShadowRequest {
    byte[] payload;
    Operation publishOperation;

    /**
     * Constructor/Builder for PubSub request class.
     *
     * @param thingName        The thing name
     * @param shadowName       The name of the shadow on which the operation was requested
     * @param payload          The payload to be published
     * @param publishOperation The Operation type to be performed on the shadow
     */
    @Builder
    public PubSubRequest(@NotNull String thingName, String shadowName, Operation publishOperation, byte[] payload) {
        super(thingName, shadowName);
        this.publishOperation = publishOperation;
        this.payload = payload;
    }
}
