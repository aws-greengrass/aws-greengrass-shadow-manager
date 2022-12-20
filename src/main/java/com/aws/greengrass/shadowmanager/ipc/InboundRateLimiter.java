/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;


import com.aws.greengrass.shadowmanager.configuration.RateLimitsConfiguration;
import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_TOTAL_LOCAL_REQUESTS_RATE;

/**
 * Class which handles request throttling for all inbound local shadow requests.
 */
public class InboundRateLimiter {
    private final AtomicInteger ratePerThing = new AtomicInteger(DEFAULT_LOCAL_REQUESTS_RATE);
    private final AtomicInteger totalRate = new AtomicInteger(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE);

    @Setter(AccessLevel.PACKAGE)
    private IpcRateLimiter totalInboundRateLimiter = new IpcRateLimiter(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE);

    @Setter(AccessLevel.PACKAGE) @Getter(AccessLevel.PACKAGE)
    private Map<String, IpcRateLimiter> rateLimitersPerThing = Collections.synchronizedMap(
            new LinkedHashMap<String, IpcRateLimiter>(totalRate.get()) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > totalRate.get();
        }
    });

    /**
     * Attempts to acquire lock for the specified thing.
     *
     * @param thingName Thing to acquire lock from assigned rate limiter
     * @throws ThrottledRequestException Max requests per thing per second exceeded
     */
    public void acquireLockForThing(String thingName) throws ThrottledRequestException {

        // TODO: [GG-36229]: have calling class validate thingName prior to getting lock
        if (thingName == null || thingName.isEmpty()) {
            return;
        }

        if (!totalInboundRateLimiter.tryAcquire()) {
            throw new ThrottledRequestException("Max total local shadow request rate exceeded");
        }

        IpcRateLimiter rateLimiter = rateLimitersPerThing.computeIfAbsent(thingName, k ->
                new IpcRateLimiter(ratePerThing.get()));

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
        totalRate.set(rate);
        totalInboundRateLimiter.setRate(totalRate.get());
    }

    /**
     * Sets each inbound rate limiter per thing to specified rate.
     *
     * @param rate Max inbound requests per second per thing
     */
    public void setRate(int rate) {
        ratePerThing.set(rate);
        rateLimitersPerThing.forEach((k, v) -> v.setRate(ratePerThing.get()));
    }

    public void updateRateLimits(RateLimitsConfiguration rateLimitsConfiguration) {
        setTotalRate(rateLimitsConfiguration.getMaxTotalLocalRequestRate());
        setRate(rateLimitsConfiguration.getMaxLocalRequestRatePerThing());
    }
}
