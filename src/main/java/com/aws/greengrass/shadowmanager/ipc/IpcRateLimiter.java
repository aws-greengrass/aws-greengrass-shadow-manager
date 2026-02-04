/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;

@Getter(AccessLevel.PACKAGE)
public class IpcRateLimiter {
    private int count = 0;
    private int rate;
    private long timestamp = System.nanoTime() / 1_000_000;

    /**
     * Constructor.
     *
     * @param rate locks given per second
     */
    public IpcRateLimiter(int rate) {
        this.rate = rate;
    }

    /**
     * Sets the rate for the RateLimiter.
     *
     * @param rate locks given per second
     */
    @Synchronized
    public void setRate(int rate) {
        count = count * rate / this.rate;
        this.rate = rate;
    }

    /**
     * Tries to get lock. This is a non blocking call where if lock was not retrieved it will immediately return.
     */
    @Synchronized
    public boolean tryAcquire() {
        long currentTime = System.nanoTime() / 1_000_000;

        if (currentTime - timestamp >= 1000) {
            count = 1;
            timestamp = currentTime;
            return true;
        }

        if (count < rate) {
            count++;
            return true;
        }

        return false;
    }
}
