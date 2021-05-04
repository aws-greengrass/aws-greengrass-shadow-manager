/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;


import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import vendored.com.google.common.util.concurrent.RateLimiter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_SHADOW_REQUESTS_PER_THING_PS;

/**
 * Class which handles request throttling for all inbound local shadow requests.
 */
public class InboundRateLimiter {
    private static final Map<String, RateLimiter> rateLimiterMap = new HashMap<>();
    private static double rate = DEFAULT_LOCAL_SHADOW_REQUESTS_PER_THING_PS;

    /**
     * Ctr for InboundRateLimiter.
     */
    @Inject
    public InboundRateLimiter() {

    }

    /**
     * Attempts to acquire lock for the specified thing.
     *
     * @param thingName Thing to acquire lock from assigned rate limiter
     * @throws ThrottledRequestException Max requests per thing per second exceeded
     */
    public void acquireLockForThing(String thingName) throws ThrottledRequestException {
        RateLimiter rateLimiter = rateLimiterMap.computeIfAbsent(thingName, k -> RateLimiter.create(rate));

        if (!rateLimiter.tryAcquire()) {
            throw new ThrottledRequestException("Local shadow request throttled for thing");
        }
    }

    /**
     * Removes any inbound rate limiters no longer being supported.
     *
     * @param things New set of things to track rate limiters for
     */
    public void resetRateLimiters(Set<String> things) {
        rateLimiterMap.entrySet().removeIf(entry -> things.contains(entry.getKey()));
    }

    /**
     * Sets each inbound rate limiter per thing to specified rate.
     *
     * @param rate Max inbound requests per second per thing
     */
    public void setRate(double rate) {
        InboundRateLimiter.rate = rate;
        rateLimiterMap.forEach((k, v) -> v.setRate(rate));
    }
}
