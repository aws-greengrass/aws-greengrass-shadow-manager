package com.aws.iot.greengrass.shadowmanager;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;

import javax.inject.Inject;

@ImplementsService(name = ShadowManager.SERVICE_NAME)
public class ShadowManager extends EvergreenService {
    public static final String SERVICE_NAME = "aws.greengrass.shadowmanager";

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
