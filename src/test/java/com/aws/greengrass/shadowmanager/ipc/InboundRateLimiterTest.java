/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.shadowmanager.exception.ThrottledRequestException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class InboundRateLimiterTest {
    private static final int TEST_RATE_LIMIT = 1;
    private InboundRateLimiter inboundRateLimiter;

    @Mock
    IpcRateLimiter mockRateLimiter;

    @Mock
    IpcRateLimiter mockTotalInboundRateLimiter;

    @Mock
    Map<String, IpcRateLimiter> mockRateLimiterMap;

    @Captor
    ArgumentCaptor<Integer> rateCaptor;

    @Captor
    ArgumentCaptor<String> thingNameCaptor;

    @BeforeEach
    void setup() {
        lenient().when(mockRateLimiterMap.computeIfAbsent(anyString(), any())).thenReturn(mockRateLimiter);
        lenient().when(mockRateLimiter.tryAcquire()).thenReturn(true);
        lenient().when(mockTotalInboundRateLimiter.tryAcquire()).thenReturn(true);
        inboundRateLimiter = new InboundRateLimiter();
        inboundRateLimiter.setTotalInboundRateLimiter(mockTotalInboundRateLimiter);
        inboundRateLimiter.setRateLimitersPerThing(mockRateLimiterMap);
    }

    @Test
    void GIVEN_new_thing_WHEN_acquire_lock_for_thing_THEN_lock_acquired() {
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(THING_NAME));

        verify(mockRateLimiterMap, times(1)).computeIfAbsent(thingNameCaptor.capture(), any());
        assertThat(thingNameCaptor.getValue(), is(THING_NAME));
    }

    @Test
    void GIVEN_failure_to_get_lock_WHEN_acquire_lock_for_thing_THEN_throw_throttled_request_exception() {
        when(mockRateLimiter.tryAcquire()).thenReturn(false);

        ThrottledRequestException thrown = assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(THING_NAME));
        assertThat(thrown.getMessage(), is(equalTo("Local shadow request throttled for thing")));

        verify(mockRateLimiterMap, times(1)).computeIfAbsent(thingNameCaptor.capture(), any());
        assertThat(thingNameCaptor.getValue(), is(THING_NAME));
    }

    @Test
    void GIVEN_failure_to_get_lock_due_to_total_inbound_rate_throttle_WHEN_acquire_lock_for_thing_THEN_throw_throttled_request_exception() {
        when(mockTotalInboundRateLimiter.tryAcquire()).thenReturn(false);

        ThrottledRequestException thrown = assertThrows(ThrottledRequestException.class, () -> inboundRateLimiter.acquireLockForThing(THING_NAME));
        assertThat(thrown.getMessage(), is(equalTo("Max total local shadow request rate exceeded")));

        verify(mockRateLimiterMap, times(0)).computeIfAbsent(anyString(), any());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_acquiring_lock_for_missing_thing_WHEN_acquire_lock_for_thing_THEN_do_nothing(String thingName) {
        assertDoesNotThrow(() -> inboundRateLimiter.acquireLockForThing(thingName));
        verify(mockRateLimiterMap, times(0)).computeIfAbsent(anyString(), any());
        verify(mockRateLimiter, times(0)).tryAcquire();
    }

    @Test
    void GIVEN_different_thing_requests_surpass_total_inbound_rate_WHEN_acquire_lock_for_thing_THEN_rate_limiter_map_bounded() throws ThrottledRequestException, InterruptedException {
        final String thing1 = "thing1";
        final String thing2 = "thing2";
        final String thing3 = "thing3";

        // set rate limiter map with bounded cap of 2
        int testRate = 2;
        inboundRateLimiter = new InboundRateLimiter();
        inboundRateLimiter.setTotalRate(testRate);

        Map<String, IpcRateLimiter> rateLimiterMap = inboundRateLimiter.getRateLimitersPerThing();

        // fill out map to max capacity before removing last accessed rate limiter
        inboundRateLimiter.acquireLockForThing(thing1);
        TimeUnit.MILLISECONDS.sleep(500);
        inboundRateLimiter.acquireLockForThing(thing2);

        assertThat(rateLimiterMap.keySet().toArray(), arrayContainingInAnyOrder(thing1,thing2));

        // last accessed thing should be removed
        TimeUnit.MILLISECONDS.sleep(500);
        inboundRateLimiter.acquireLockForThing(thing3);
        assertThat(rateLimiterMap.keySet().toArray(), arrayContainingInAnyOrder(thing2, thing3));
    }

    @Test
    void GIVEN_new_rate_WHEN_set_rate_THEN_rate_set_for_existing_limiters() {
        IpcRateLimiter mockLimiter1 = mock(IpcRateLimiter.class);
        IpcRateLimiter mockLimiter2 = mock(IpcRateLimiter.class);

        LinkedHashMap<String, IpcRateLimiter> testRateLimiterMap = new LinkedHashMap<>();
        testRateLimiterMap.put("existingThing1", mockLimiter1);
        testRateLimiterMap.put("existingThing2", mockLimiter2);
        inboundRateLimiter = new InboundRateLimiter();
        inboundRateLimiter.setRateLimitersPerThing(testRateLimiterMap);

        inboundRateLimiter.setRate(TEST_RATE_LIMIT);

        verify(mockLimiter1, times(1)).setRate(rateCaptor.capture());
        verify(mockLimiter2, times(1)).setRate(rateCaptor.capture());
        assertThat(rateCaptor.getAllValues().get(0), is(TEST_RATE_LIMIT));
        assertThat(rateCaptor.getAllValues().get(1), is(TEST_RATE_LIMIT));
    }

    @Test
    void GIVEN_new_total_inbound_rate_WHEN_set_total_rate_THEN_rate_set_for_total_inbound_rate_limiter() throws ThrottledRequestException {
        inboundRateLimiter.setTotalRate(TEST_RATE_LIMIT);
        verify(mockTotalInboundRateLimiter, times(1)).setRate(rateCaptor.capture());
        assertThat(rateCaptor.getValue(), is(TEST_RATE_LIMIT));
    }

    @Test
    void GIVEN_existing_rate_limiters_for_things_WHEN_clear_THEN_rate_limiters_cleared() {
        inboundRateLimiter.clear();
        verify(mockRateLimiterMap, times(1)).clear();
    }
}
