/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.utils;

import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPC;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.QOS;
import software.amazon.awssdk.aws.greengrass.model.ReportedLifecycleState;
import software.amazon.awssdk.aws.greengrass.model.UpdateStateRequest;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public final class IPCTestUtils {
    private IPCTestUtils() {
    }

    public static GreengrassCoreIPCClientV2 getGreengrassClient() throws InterruptedException, IOException {
        return GreengrassCoreIPCClientV2.builder().build();
    }

    public static QOS getQOSFromValue(int qos) {
        if (qos == 1) {
            return QOS.AT_LEAST_ONCE;
        } else if (qos == 0) {
            return QOS.AT_MOST_ONCE;
        }
        return QOS.AT_LEAST_ONCE; //default value
    }

    public static void reportState(GreengrassCoreIPC greengrassCoreIPCClient,
                                    ReportedLifecycleState state)
            throws ExecutionException, InterruptedException {
        UpdateStateRequest updateStateRequest = new UpdateStateRequest();
        updateStateRequest.setState(state);
        greengrassCoreIPCClient.updateState(updateStateRequest, Optional.empty()).getResponse().get();
    }
}

