/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_REQUESTS_RATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class IpcRateLimiterTest {
    private static final int TEST_RATE_LIMIT = 10;

    @Test
    void GIVEN_rate_WHEN_set_rate_THEN_rate_set() {
        int newRate = 5;
        IpcRateLimiter ipcRateLimiter = new IpcRateLimiter(DEFAULT_LOCAL_REQUESTS_RATE);

        ipcRateLimiter.setRate(newRate);

        assertThat(ipcRateLimiter.getRate(), is(equalTo(newRate)));
        assertThat(ipcRateLimiter.getCount(), is(0));
    }

    @Test
    void GIVEN_max_locks_called_WHEN_try_acquire_THEN_locks_acquired() {
        IpcRateLimiter ipcRateLimiter = new IpcRateLimiter(TEST_RATE_LIMIT);
        for (int i = 0; i < TEST_RATE_LIMIT; i++) {
            assertThat("lock acquired", ipcRateLimiter.tryAcquire(), is(true));
        }
    }

    @Test
    void GIVEN_getting_lock_after_rate_reached_WHEN_try_acquire_THEN_lock_not_acquired() {
        IpcRateLimiter ipcRateLimiter = new IpcRateLimiter(TEST_RATE_LIMIT);
        for (int i = 0; i < TEST_RATE_LIMIT; i++) {
            assertThat("lock acquired", ipcRateLimiter.tryAcquire(), is(true));
        }

        assertThat("lock not acquired", ipcRateLimiter.tryAcquire(), is(false));
    }

    @Test
    void GIVEN_acquire_lock_after_one_second_WHEN_try_acquire_THEN_count_reset() throws InterruptedException {
        IpcRateLimiter ipcRateLimiter = new IpcRateLimiter(TEST_RATE_LIMIT);

        ipcRateLimiter.tryAcquire();
        long initialTimestamp = ipcRateLimiter.getTimestamp();

        TimeUnit.SECONDS.sleep(1);

        assertThat("lock acquired", ipcRateLimiter.tryAcquire(), is(true));
        assertThat("count reset", ipcRateLimiter.getCount(), is(1));
        assertThat(ipcRateLimiter.getTimestamp(), greaterThanOrEqualTo(initialTimestamp + 1000));
    }
}
