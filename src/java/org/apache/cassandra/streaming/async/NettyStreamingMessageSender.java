/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.streaming.async;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionParams;
import org.apache.cassandra.net.async.OutboundConnector;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingMessageSender;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Responsible for sending {@link StreamMessage}s to a given peer. We manage an array of netty {@link Channel}s
 * for sending {@link OutgoingFileMessage} instances; all other {@link StreamMessage} types are sent via
 * the normal internode messaging via {@link MessagingService}. The reason for this is to treat those messages
 * like any other cassandra message type, and only special case the {@link OutgoingFileMessage} as it is
 * the one doing the heavy lifting of transferring sstables. Also, multiple netty channels allow multiple files
 * to be sent in parallel
 *
 * One of the challenges is, when sending files, we might need to delay shipping the file if, for example,
 * we've exceeded our network I/O use due to rate limiting (at the cassandra level). At that point, it's easy enough
 * to reschedule processing the file once we acquire the permits from the rate limiter; however, we need to ensure that
 * no other messages are submitted to the same channel while the current file is still being processed.
 * Hence, the use of an internal {@link #queue} to hold pending outbound messages.
 *
 * We use a netty channel {@link Attribute}, specifically {@link #CHANNEL_BUSY_ATTR} to indicate if a channel
 * is currently processing a file transfer. (Remember,
 *
 * While a channel attribute is not the highest performing, our needs for performance wrt a flag indicating a busy
 * state are pretty low, so the attribute is fine.
 */
public class NettyStreamingMessageSender implements StreamingMessageSender
{
    private static final Logger logger = LoggerFactory.getLogger(NettyStreamingMessageSender.class);
    private static final int DEFAULT_CHANNEL_BUFFER_SIZE = 1 << 16;

    private static final long BYTES_PER_MEGABIT = (1024 * 1024) / 8; // from bits

    /**
     * A netty-specific attribute that is assigned to each {@link Channel} instance that is used to indicate
     * if the channel is currently processing a message (transferring a sstable).
     */
    private static final AttributeKey<Boolean> CHANNEL_BUSY_ATTR = AttributeKey.newInstance("channelBusy");

    // TODO:JEB should this be configurable? also, is this local to this stream session or global to all stream sessions or per-remote peer?
    private static final int DEFAULT_MAX_PARALLEL_TRANSFERS = 4;

    private final StreamSession session;
    private final int protocolVersion;
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;
    private final ServerEncryptionOptions encryptionOptions;
    private final int channelBufferSize;

    /**
     * A queue to use for backlogging messages is there is already a message being processed.
     */
    private final Queue<OutgoingFileMessage> queue = new ConcurrentLinkedQueue<>();

    private final AtomicReferenceArray<Channel> transferChannels;
    private volatile boolean closed;

    // TODO:JEB need extra "boolean forceSecure" param to account for behavior in BulkLoadConnFactory
    public NettyStreamingMessageSender(StreamSession session, InetAddress connectionAddress, int protocolVersion)
    {
        this.session = session;
        this.protocolVersion = protocolVersion;
        localAddress = new InetSocketAddress(FBUtilities.getLocalAddress(), 0);
        boolean secure = MessagingService.isEncryptedConnection(connectionAddress);
        remoteAddress = new InetSocketAddress(connectionAddress, secure ? DatabaseDescriptor.getSSLStoragePort() : DatabaseDescriptor.getStoragePort());
        encryptionOptions = secure ? DatabaseDescriptor.getServerEncryptionOptions() : null;
        channelBufferSize = DEFAULT_CHANNEL_BUFFER_SIZE;

        // set size from ctor param
        transferChannels = new AtomicReferenceArray<>(new Channel[DEFAULT_MAX_PARALLEL_TRANSFERS]);
    }

    @Override
    public void initialize()
    {
        // this message is sent over the normal internode messaging (MessagingService),
        // not a netty channel, so it's ok that we don't have any channels established yet
        sendMessage(new StreamInitMessage(localAddress.getAddress(),
                                          session.sessionIndex(),
                                          session.planId(),
                                          session.description(),
                                          session.keepSSTableLevel(),
                                          session.isIncremental(),
                                          session.getPendingRepair()));
    }


    /**
     * Submit a {@link StreamMessage} to be sent to the peer via normal internode messaging.
     */
    @Override
    public void sendMessage(StreamMessage message)
    {
        logger.debug("[Stream #{}] Sending {}", session.planId(), message);
        MessagingService.instance().sendOneWay(message.createMessageOut(), remoteAddress.getAddress());
    }

    /**
     * Submit the {@link OutgoingFileMessage} to transfer a file via a netty channel. If there is a slot open in
     * {@link #transferChannels}, create a new {@link Channel} and publish the message to it; else, enqueue the message
     * for future consumption.
     */
    public void streamFile(OutgoingFileMessage message)
    {
        if (closed)
            throw new IllegalStateException("Outgoing streaming channel has been closed");

        queue.offer(message);
        Channel channel = getNewTransferChannel();
        if (channel != null)
            tryPublishFromQueue(channel);
    }

    /**
     * Get a new or not currently busy netty {@link Channel} if we haven't hit the max count.
     */
    private Channel getNewTransferChannel()
    {
        for (int i = 0; i < transferChannels.length(); i++)
        {
            Channel channel = transferChannels.get(i);
            if (channel != null)
            {
                Attribute<Boolean> busy = channel.attr(CHANNEL_BUSY_ATTR);
                if (busy.compareAndSet(false, true))
                    return channel;
            }
            else
            {
                channel = createChannel();
                channel.attr(CHANNEL_BUSY_ATTR).set(false);
                if (transferChannels.compareAndSet(i, null, channel))
                    return channel;

                // TODO:JEB be careful as there might be legacy code on the receiver to handle the closing of a connection
                // which is treated as a failed/closed session
                channel.close();
            }
        }
        return null;
    }

    /**
     * Create a new netty {@link Channel} and block for it to connect successfully.
     */
    private Channel createChannel()
    {
        ConnectionWaiter blocker = new ConnectionWaiter();
        // TODO:JEB channelBufferSize
        // TODO:JEB double check this *socket buffer* size (not channel buffer size!!!)
        int sendBufferSize = 1 << 16;
        if (DatabaseDescriptor.getInternodeSendBufferSize() > 0)
            sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize();
        // TODO:JEB set the high/low water marks

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .localAddr(localAddress)
                                                                  .remoteAddr(remoteAddress)
                                                                  .protocolVersion(protocolVersion)
                                                                  .callback(blocker::connectCallback)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(NettyFactory.Mode.STREAMING)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .build();

        Bootstrap bootstrap = NettyFactory.createOutboundBootstrap(params, NettyFactory.STREAMING_CLIENT_GROUP);
        OutboundConnector connector = new OutboundConnector(bootstrap, localAddress, remoteAddress);
        connector.connect();

        // TODO:JEB fix this?!?!?
        if (!blocker.await() )//|| blocker.result.result != ConnectionHandshakeResult.Result.GOOD)
        {
            connector.cancel();
            throw new RuntimeException("failed to make streaming connection to " + remoteAddress);
        }

        Channel channel = blocker.result.channel;
        ChannelPipeline pipeline = channel.pipeline();

        StreamRateLimiter rateLimiter = new StreamRateLimiter(remoteAddress.getAddress());
        pipeline.addLast(NettyFactory.STREAMING_CLIENT_GROUP, "networkThrottleHandler", new NetworkThrottlingHandler(rateLimiter));
        pipeline.addLast(NettyFactory.STREAMING_CLIENT_GROUP, "outboundStreamHandler", new StreamingOutboundHandler(session, protocolVersion));
        return channel;
    }

    private static class ConnectionWaiter
    {
        private final CountDownLatch latch;
        private ConnectionHandshakeResult result;

        ConnectionWaiter()
        {
            latch = new CountDownLatch(1);
        }

        /**
         * @return true if the the {@link #latch} hit zero before the timeout; else, false.
         */
        boolean await()
        {
            try
            {
                latch.await(30, TimeUnit.SECONDS);
                return true;
            }
            catch (InterruptedException e)
            {
                return false;
            }
        }

        private void connectCallback(ConnectionHandshakeResult result)
        {
            this.result = result;
            latch.countDown();
        }
    }

    /**
     * Attempts to pull a message off the {@link #queue} and publish it to the provided netty channel.
     * Each channel should only process (or transfer) one file at a time, so we need careful handling of how messages
     * get published to each channel. This method handles the tricky race conditions that could arise between threads
     * adding new messages to the queue versus the netty event loop thread that executes
     * {@code {@link #onMessageComplete(Future, Channel)}}  upon completing the previous message.
     *
     * We need to be careful about concurrency as we get here from several paths:
     *
     * 1) {@link StreamSession#startStreamingFiles()}, which is only called once in the life of the session (at the beginning),
     * 2) {@link #onMessageComplete(Future, Channel)}, the callback from the netty event loop
     *
     * @return true if a message was sent to the netty channel; else, false.
     */
    private boolean tryPublishFromQueue(Channel channel)
    {
        if (!queue.isEmpty() && channel.attr(CHANNEL_BUSY_ATTR).compareAndSet(false, true))
        {
            OutgoingFileMessage msg = queue.poll();
            if (msg != null)
            {
                ChannelFuture future = channel.write(msg);
                future.addListener(f -> onMessageComplete(f, channel));
                return true;
            }
            // we won the race to mark this channel as busy (and the queue was not empty when we checked),
            // but now the queue is empty (we lost the race to get a message), so set the channel back to not busy.
            channel.attr(CHANNEL_BUSY_ATTR).set(false);

            // Note: it's arguable that we could close the connection here if there's no work remaining to do, but we don't know the
            // array index into {@link #transferChannels). further, an idle socket consumes virtually zero resources, and becasue
            // it's a netty channel it's not necessarily a distinct thread - tl;dr skip the complexity of closing the channel here.
        }
        return false;
    }

    /**
     * Decides what to do after a {@link StreamMessage} is processed.
     *
     * Note: this is called from the netty event loop.
     *
     * @return true if the message was processed sucessfully; else, false.
     */
    private boolean onMessageComplete(Future<? super Void> future, Channel channel)
    {
        if (!future.isDone())
            return false;

        ChannelFuture channelFuture = (ChannelFuture)future;
        if (channelFuture.isSuccess())
        {
            channel.attr(CHANNEL_BUSY_ATTR).set(false);
            tryPublishFromQueue(channel);
            return true;
        }

        logger.error("future = {}, cause = {}", future, future.cause());

        // TODO:JEB re-evaluate this error condition
        // we only want to call session.onError when the socket is still open
        if (!channel.isOpen() && future.cause() != null)
            session.onError(future.cause());

        // TODO:JEB yank this log
        logger.error("failed to stream file", channelFuture.cause());

        close();

        return false;
    }

    @Override
    public boolean connected()
    {
        return !closed;
    }

    @Override
    public void close()
    {
        logger.debug("[Stream #{}] Closing stream connection channel on {}", session.planId(), session.peer);

        if (!closed)
        {
            closed = true;
            queue.clear();
            for (int i = 0; i < transferChannels.length(); i++)
            {
                Channel channel = transferChannels.get(i);
                if (channel != null)
                    channel.close();

                transferChannels.set(i, null);
            }
        }
    }
}
