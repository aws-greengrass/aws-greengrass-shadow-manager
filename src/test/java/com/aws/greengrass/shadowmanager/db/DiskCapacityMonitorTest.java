/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.db;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
@ExtendWith({MockitoExtension.class, GGExtension.class})
public class DiskCapacityMonitorTest {

    DiskCapacityMonitor monitor;

    @Mock
    DiskSpaceNotifier notifier;

    @Mock
    ExecutorService executorService;

    @BeforeEach
    void setup() {
        monitor = new DiskCapacityMonitor(executorService, notifier);
        monitor.start();
    }

    @Test
    void GIVEN_usage_exceeds_max_disk_usage_WHEN_check_size_THEN_capacity_is_exceeded() {
        monitor.setMaxDiskUsage(10);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(11);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        // we only start the notifier task once
        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_usage_below_max_disk_usage_WHEN_check_size_THEN_capacity_is_not_exceeded() {
        monitor.setMaxDiskUsage(10);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(5);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        // we only start the notifier task once
        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_usage_stays_above_max_disk_usage_WHEN_check_size_THEN_capacity_is_exceeded() {
        monitor.setMaxDiskUsage(10);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(11);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        monitor.checkSize(12);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        monitor.checkSize(1000);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        // we only start the notifier task once
        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_usage_above_max_disk_usage_and_falls_below_WHEN_check_size_THEN_capacity_is_not_exceeded() {
        monitor.setMaxDiskUsage(10);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(11);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        monitor.checkSize(12);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        monitor.checkSize(5);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        // we only start the notifier task once
        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_usage_below_max_disk_usage_and_steps_above_WHEN_check_size_THEN_capacity_is_exceeded() {
        monitor.setMaxDiskUsage(100);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(11);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(12);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(200);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        // we only start the notifier task once
        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_zero_max_usage_WHEN_set_max_disk_usage_THEN_no_monitor_started() {
        monitor.setMaxDiskUsage(0);

        verify(executorService, never()).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_non_zero_max_usage_WHEN_set_zero_max_disk_usage_THEN_monitor_stopped() {
        Future task = mock(Future.class);
        when(executorService.submit(any(Runnable.class))).thenReturn(task);
        monitor.setMaxDiskUsage(10);
        monitor.setMaxDiskUsage(0);

        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
        verify(task, times(1)).cancel(true);
    }

    @Test
    void GIVEN_non_zero_max_usage_WHEN_set_non_zero_max_disk_usage_THEN_size_rechecked() {
        monitor.setMaxDiskUsage(10);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.checkSize(5);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(false));

        monitor.setMaxDiskUsage(1);
        assertThat("capacity exceeded", monitor.isCapacityExceeded(), is(true));

        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
    }

    @Test
    void GIVEN_monitor_started_WHEN_stop_THEN_monitor_stopped() {
        Future task = mock(Future.class);
        when(executorService.submit(any(Runnable.class))).thenReturn(task);

        monitor.setMaxDiskUsage(10);

        monitor.stop();
        verify(executorService, times(1)).submit(Mockito.any(Runnable.class));
        verify(task, times(1)).cancel(true);
    }
}
