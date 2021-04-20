/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

public enum LogEvents {
    AUTHORIZATION_ERROR("shadow-authorization-error"),
    IPC_REGISTRATION("shadow-ipc-registration"),
    IPC_ERROR("shadow-ipc-error"),
    DCM_ERROR("shadow-dcm-error"),
    DATABASE_OPERATION_ERROR("shadow-database-operation-error"),
    DATABASE_CLOSE_ERROR("shadow-database-close-error"),
    GET_THING_SHADOW("handle-get-thing-shadow"),
    UPDATE_THING_SHADOW("handle-update-thing-shadow"),
    DELETE_THING_SHADOW("handle-delete-thing-shadow"),
    LIST_NAMED_SHADOWS("handle-list-named-shadows-for-thing"),
    MQTT_CLIENT_SUBSCRIPTION_ERROR("mqtt-client-subscription-error"),
    LOCAL_UPDATE_SYNC_REQUEST("local-update-sync-request");

    String code;

    LogEvents(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}