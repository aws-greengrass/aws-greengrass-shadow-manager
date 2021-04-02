/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.validation.constraints.NotNull;

/**
 * Base class containing IPC request information for publishing messages for Shadow operations.
 */
@Getter
@AllArgsConstructor
public class IPCRequest {
    @NotNull
    String thingName;
    String shadowName;
    Operation publishOperation;
}
