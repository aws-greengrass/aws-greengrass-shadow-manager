/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;
import vendored.com.google.common.util.concurrent.RateLimiter;

import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS;

/**
 * Class which acts as the interface between ShadowManager and the IoT Data Plane.
 */
public class IotDataPlaneClient {
    private final IotDataPlaneClientFactory iotDataPlaneClientFactory;
    private final RateLimiter rateLimiter;

    /**
     * Ctr for the IotDataPlaneClient.
     *
     * @param iotDataPlaneClientFactory Factory for the IoT data plane client
     */
    @Inject
    public IotDataPlaneClient(IotDataPlaneClientFactory iotDataPlaneClientFactory) {
        this.iotDataPlaneClientFactory = iotDataPlaneClientFactory;
        this.rateLimiter = RateLimiter.create(DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS);
    }

    /**
     * Sets the rate for the RateLimiter for outbound requests.
     *
     * @param rate Max outbound requests per second
     */
    public void setRate(double rate) {
        rateLimiter.setRate(rate);
    }

    /**
     * Makes DeleteThingShadow request to Iot Data Plane.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public DeleteThingShadowResponse deleteThingShadow(String thingName, String shadowName) {
        rateLimiter.acquire();
        return iotDataPlaneClientFactory.getIotDataPlaneClient().deleteThingShadow(DeleteThingShadowRequest.builder()
                .thingName(thingName)
                .shadowName(shadowName)
                .build());
    }

    /**
     * Makes UpdateThingShadow request to Iot Data Plane.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     * @param payload    The update payload
     */
    public UpdateThingShadowResponse updateThingShadow(String thingName, String shadowName, byte[] payload) {
        rateLimiter.acquire();
        return iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(UpdateThingShadowRequest.builder()
                .thingName(thingName)
                .shadowName(shadowName)
                .payload(SdkBytes.fromByteArray(payload)).build());
    }

    /**
     * Makes GetThingShadow request to Iot Data Plane.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public GetThingShadowResponse getThingShadow(String thingName, String shadowName) {
        rateLimiter.acquire();
        return iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(GetThingShadowRequest.builder()
                .thingName(thingName)
                .shadowName(shadowName).build());
    }

}
