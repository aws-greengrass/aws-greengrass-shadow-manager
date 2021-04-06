/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc.model;

import com.aws.greengrass.shadowmanager.model.LogEvents;
import lombok.Getter;

/**
 * Enum to state the operation for the shadow.
 */
public enum Operation {
    GET_SHADOW("/get", LogEvents.GET_THING_SHADOW.code()),
    DELETE_SHADOW("/delete", LogEvents.DELETE_THING_SHADOW.code()),
    UPDATE_SHADOW("/update", LogEvents.UPDATE_THING_SHADOW.code());

    @Getter
    String op;

    @Getter
    String logEventType;

    Operation(String op, String logEventType) {
        this.op = op;
        this.logEventType = logEventType;
    }
}
