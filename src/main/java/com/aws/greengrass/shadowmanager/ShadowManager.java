/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.PluginService;

import javax.inject.Inject;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.ShadowManager";

    /**
     * Ctr for ShadowManager.
     * @param topics topics passed by by the kernel
     */
    @Inject
    public ShadowManager(Topics topics) {
        super(topics);
    }

    @Override
    public void startup() {
        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
    }
}
