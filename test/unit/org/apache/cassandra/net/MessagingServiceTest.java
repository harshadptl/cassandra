/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.codahale.metrics.Timer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.db.SystemKeyspace;
import org.caffinitas.ohc.histo.EstimatedHistogram;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessagingServiceTest
{
    private final static long ONE_SECOND = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private final static long[] bucketOffsets = new EstimatedHistogram(160).getBucketOffsets();
    private final MessagingService messagingService = MessagingService.test();

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setBackPressureStrategy(new MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.1"));
    }

    @Before
    public void before() throws UnknownHostException
    {
        MockBackPressureStrategy.applied = false;
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.2"));
        messagingService.destroyConnectionPool(InetAddress.getByName("127.0.0.3"));
    }

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 1; i <= 5000; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 internal and 2500 cross node. Mean internal dropped latency: 2730 ms and Mean cross-node dropped latency: 2731 ms", logs.get(0));
        assertEquals(5000, (int) messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 internal and 1250 cross node. Mean internal dropped latency: 2277 ms and Mean cross-node dropped latency: 2278 ms", logs.get(0));
        assertEquals(7500, (int) messagingService.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testDCLatency() throws Exception
    {
        int latency = 100;
        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;
        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNotNull(dcLatency.get("datacenter1"));
        assertEquals(1, dcLatency.get("datacenter1").getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, TimeUnit.MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, dcLatency.get("datacenter1").getSnapshot().getMax());
    }

    @Test
    public void testNegativeDCLatency() throws Exception
    {
        // if clocks are off should just not track anything
        int latency = -100;

        ConcurrentHashMap<String, Timer> dcLatency = MessagingService.instance().metrics.dcLatency;
        dcLatency.clear();

        long now = ApproximateTime.currentTimeMillis();
        long sentAt = now - latency;

        assertNull(dcLatency.get("datacenter1"));
        addDCLatency(sentAt, now);
        assertNull(dcLatency.get("datacenter1"));
    }

    @Test
    public void testQueueWaitLatency() throws Exception
    {
        int latency = 100;
        String verb = MessagingService.Verb.MUTATION.toString();

        ConcurrentHashMap<String, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency);
        assertNotNull(queueWaitLatency.get(verb));
        assertEquals(1, queueWaitLatency.get(verb).getCount());
        long expectedBucket = bucketOffsets[Math.abs(Arrays.binarySearch(bucketOffsets, TimeUnit.MILLISECONDS.toNanos(latency))) - 1];
        assertEquals(expectedBucket, queueWaitLatency.get(verb).getSnapshot().getMax());
    }

    @Test
    public void testNegativeQueueWaitLatency() throws Exception
    {
        int latency = -100;
        String verb = MessagingService.Verb.MUTATION.toString();

        ConcurrentHashMap<String, Timer> queueWaitLatency = MessagingService.instance().metrics.queueWaitLatency;
        queueWaitLatency.clear();

        assertNull(queueWaitLatency.get(verb));
        MessagingService.instance().metrics.addQueueWaitTime(verb, latency);
        assertNull(queueWaitLatency.get(verb));
    }

    @Test
    public void testUpdatesBackPressureOnSendWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddress.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        MessageOut<?> ignored = null;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), noCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), bpCallback, ignored);
        assertFalse(backPressureState.onSend);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnSend(InetAddress.getByName("127.0.0.2"), bpCallback, ignored);
        assertTrue(backPressureState.onSend);
    }

    @Test
    public void testUpdatesBackPressureOnReceiveWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddress.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = false;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertTrue(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);
    }

    @Test
    public void testUpdatesBackPressureOnTimeoutWhenEnabledAndWithSupportedCallback() throws UnknownHostException
    {
        MockBackPressureStrategy.MockBackPressureState backPressureState = (MockBackPressureStrategy.MockBackPressureState) messagingService.getBackPressureState(InetAddress.getByName("127.0.0.2"));
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();
        boolean timeout = true;

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), noCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertFalse(backPressureState.onTimeout);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureOnReceive(InetAddress.getByName("127.0.0.2"), bpCallback, timeout);
        assertFalse(backPressureState.onReceive);
        assertTrue(backPressureState.onTimeout);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.2")), ONE_SECOND);
        assertTrue(MockBackPressureStrategy.applied);
    }

    @Test
    public void testDoesntApplyBackPressureToBroadcastAddress() throws UnknownHostException
    {
        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(Arrays.asList(InetAddress.getByName("127.0.0.1")), ONE_SECOND);
        assertFalse(MockBackPressureStrategy.applied);
    }

    private static void addDCLatency(long sentAt, long nowTime) throws IOException
    {
        MessageIn.deriveConstructionTime(InetAddress.getLocalHost(), (int)sentAt, nowTime);
    }

    public static class MockBackPressureStrategy implements BackPressureStrategy<MockBackPressureStrategy.MockBackPressureState>
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public void apply(Set<MockBackPressureState> states, long timeout, TimeUnit unit)
        {
            if (!Iterables.isEmpty(states))
                applied = true;
        }

        @Override
        public MockBackPressureState newState(InetAddress host)
        {
            return new MockBackPressureState(host);
        }

        public static class MockBackPressureState implements BackPressureState
        {
            private final InetAddress host;
            public volatile boolean onSend = false;
            public volatile boolean onReceive = false;
            public volatile boolean onTimeout = false;

            private MockBackPressureState(InetAddress host)
            {
                this.host = host;
            }

            @Override
            public void onMessageSent(MessageOut<?> message)
            {
                onSend = true;
            }

            @Override
            public void onResponseReceived()
            {
                onReceive = true;
            }

            @Override
            public void onResponseTimeout()
            {
                onTimeout = true;
            }

            @Override
            public double getBackPressureRateLimit()
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public InetAddress getHost()
            {
                return host;
            }
        }
    }

    private static class BackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static class NoBackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return false;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    public void switchIpAddr() throws UnknownHostException
    {
        InetAddress publicIp = InetAddress.getByName("127.0.0.2");
        InetAddress privateIp = InetAddress.getByName("127.0.0.3");

        // reset the preferred IP value, for good test hygene
        SystemKeyspace.updatePreferredIP(publicIp, publicIp);

        // create pool/conn with public addr
        Assert.assertEquals(publicIp, messagingService.getCurrentEndpoint(publicIp));
        messagingService.reconnectWithNewIp(publicIp, privateIp);
        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));

        messagingService.destroyConnectionPool(publicIp);

        // recreate the pool/conn, and make sure the preferred ip addr is used
        Assert.assertEquals(privateIp, messagingService.getCurrentEndpoint(publicIp));
    }
}
