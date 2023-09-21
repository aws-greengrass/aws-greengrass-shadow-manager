/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.util.ShadowWriteSynchronizeHelper;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class SyncContext {
    ShadowManagerDAO dao;
    UpdateThingShadowRequestHandler updateHandler;
    DeleteThingShadowRequestHandler deleteHandler;
    IotDataPlaneClientWrapper iotDataPlaneClientWrapper;
    ShadowWriteSynchronizeHelper synchronizeHelper;
}
