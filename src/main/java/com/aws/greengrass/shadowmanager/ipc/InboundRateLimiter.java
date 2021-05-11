/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;


import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import vendored.com.google.common.util.concurrent.RateLimiter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_TOTAL_LOCAL_REQUESTS_RATE;

/**
 * Class which handles request throttling for all inbound local shadow requests.
 */
public class InboundRateLimiter {
    private double ratePerThing = DEFAULT_LOCAL_REQUESTS_RATE;
    private volatile int totalRate = DEFAULT_TOTAL_LOCAL_REQUESTS_RATE;

    @Setter(AccessLevel.PACKAGE)
    private RateLimiter totalInboundRateLimiter = RateLimiter.create(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE);

    @Setter(AccessLevel.PACKAGE) @Getter(AccessLevel.PACKAGE)
    private Map<String, RateLimiter> rateLimitersPerThing = Collections.synchronizedMap(
            new LinkedHashMap<String, RateLimiter>(totalRate, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > totalRate;
        }
    });

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

        if (!totalInboundRateLimiter.tryAcquire()) {
            throw new ThrottledRequestException("Max total local shadow request rate exceeded");
        }

        RateLimiter rateLimiter = rateLimitersPerThing.computeIfAbsent(thingName, k ->
                RateLimiter.create(ratePerThing));

        if (!rateLimiter.tryAcquire()) {
            throw new ThrottledRequestException("Local shadow request throttled for thing");
        }
    }

    /**
     * Clears all inbound rate limiters.
     */
    public void clear() {
        rateLimitersPerThing.clear();
    }

    /**
     * Sets the overall inbound rate limiter rate.
     *
     * @param rate Max inbound requests per second for all things
     */
    public void setTotalRate(int rate) {
        totalRate = rate;
        this.totalInboundRateLimiter.setRate(rate);
    }

    /**
     * Sets each inbound rate limiter per thing to specified rate.
     *
     * @param rate Max inbound requests per second per thing
     */
    public void setRate(int rate) {
        ratePerThing = rate;
        rateLimitersPerThing.forEach((k, v) -> v.setRate(rate));
    }
}
