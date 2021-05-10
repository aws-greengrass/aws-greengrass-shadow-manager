/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;


import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import vendored.com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_REQUESTS_RATE;

/**
 * Class which handles request throttling for all inbound local shadow requests.
 */
public class InboundRateLimiter {
    private final ConcurrentHashMap<String, RateLimiter> rateLimiterMap;
    private double rate = DEFAULT_LOCAL_REQUESTS_RATE;

    /**
     * Ctr for InboundRateLimiter.
     */
    @Inject
    public InboundRateLimiter() {
        this(new ConcurrentHashMap<>());
    }

    InboundRateLimiter(ConcurrentHashMap<String, RateLimiter> rateLimiterMap) {
        this.rateLimiterMap = rateLimiterMap;
    }

    /**
     * Attempts to acquire lock for the specified thing.
     *
     * @param thingName Thing to acquire lock from assigned rate limiter
     * @throws ThrottledRequestException Max requests per thing per second exceeded
     */
    public void acquireLockForThing(String thingName) throws ThrottledRequestException {

        // TODO: have calling class validate thingName prior to getting lock
        if (thingName == null || thingName.isEmpty()) {
            return;
        }

        RateLimiter rateLimiter = rateLimiterMap.computeIfAbsent(thingName, k -> RateLimiter.create(rate));

        if (!rateLimiter.tryAcquire()) {
            throw new ThrottledRequestException("Local shadow request throttled for thing");
        }
    }

    /**
     * Clears all inbound rate limiters.
     */
    public void clear() {
        rateLimiterMap.clear();
    }

    /**
     * Sets each inbound rate limiter per thing to specified rate.
     *
     * @param rate Max inbound requests per second per thing
     */
    public void setRate(int rate) {
        this.rate = rate;
        rateLimiterMap.forEach((k, v) -> v.setRate(rate));
    }
}
