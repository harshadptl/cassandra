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

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.utils.Pair;

class NetworkThrottlingHandler extends ChannelDuplexHandler
{
    /**
     * TL;DR It's a constant, don't ask questions.
     *
     * The way the channel writability works in the {@link ChannelOutboundBuffer} is that you can have multiple
     * handlers (represented by different 'index' values) that all contribute to the writability status of the channel.
     * Each index is a bit in a bit mask and they all need to be set (or unset, read {@link ChannelOutboundBuffer} for details)
     * to the same value to agree upon the writability status of the channel. Thus, when updating it's status, a given handler
     * should update it's (hopefully unique) index bit in the bitmask.
     */
    private static final int CHANNEL_WRITABILITY_INDEX = 27;

    private enum State { START, AWAIT_GLOBAL_THROTTLE, AWAIT_INTER_DC_THROTTLE }

    private final StreamRateLimiter limiter;

    private State state;
    // pair.left with either be a ByteBuf or FileRegion
    private Pair<Object, ChannelPromise> delayedMessage;
    private ScheduledFuture<?> scheduledWrite;


    NetworkThrottlingHandler(StreamRateLimiter limiter)
    {
        state = State.START;
        this.limiter = limiter;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise)
    {
        if (!(message instanceof ByteBuf || message instanceof FileRegion) )
        {
            ctx.write(message, promise);
            return;
        }

        assert delayedMessage == null : "upstream handlers should not have sent more buffers while there is a delayed buffer";
        assert state == State.START;
        assert scheduledWrite == null;

        writeInternal(ctx, message, promise);
    }

    /**
     * Write a {@link ByteBuf} to the downstream handlers. We break up the check for the rate limiting into individual
     * steps to avoid the degenerate case of always being able to acquire from the global rate limit, but never from the
     * inter-dc limit, and thus constantly waste valuable allotments from the global limiter (which could be used for
     * intra-dc streaming).
     *
     * // TODO: see if there's a better algorithm than having to different rate limiters - meaning, is there a snazy
     * algo that can intelligently check the allotment for both global & and inter-dc in 'one call'?
     */
    @VisibleForTesting
    void writeInternal(final ChannelHandlerContext ctx, Object message, ChannelPromise promise)
    {
        scheduledWrite = null;
        final int size = size(message);
        switch (state)
        {
            case START:
                if (!limiter.tryAcquireGlobal(size))
                {
                    state = State.AWAIT_GLOBAL_THROTTLE;
                    delayedMessage = Pair.create(message, promise);
                    setUserDefinedWritability(ctx, false);
                    scheduledWrite = ctx.executor().schedule(() -> writeInternal(ctx, message, promise), 200, TimeUnit.MILLISECONDS);
                    return;
                }
                // fall-through
            case AWAIT_GLOBAL_THROTTLE:
                if (!limiter.tryAcquireInterDc(size))
                {
                    state = State.AWAIT_INTER_DC_THROTTLE;
                    delayedMessage = Pair.create(message, promise);
                    setUserDefinedWritability(ctx, false);
                    scheduledWrite = ctx.executor().schedule(() -> writeInternal(ctx, message, promise), 200, TimeUnit.MILLISECONDS);
                    return;
                }
                // fall-through
        }

        ctx.write(message, promise);
        state = State.START;
        delayedMessage = null;
        setUserDefinedWritability(ctx, true);
    }

    private int size(Object msg)
    {
        if (msg instanceof ByteBuf)
            return ((ByteBuf)msg).readableBytes();
        if (msg instanceof FileRegion)
            return (int)((FileRegion)msg).count();
        return 0;
    }

    private void setUserDefinedWritability(ChannelHandlerContext ctx, boolean writable)
    {
        if (ctx.channel().isWritable() == writable)
            return;

        ChannelOutboundBuffer cob = ctx.channel().unsafe().outboundBuffer();
        if (cob != null)
            cob.setUserDefinedWritability(CHANNEL_WRITABILITY_INDEX, writable);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        closeResources();
        ctx.fireChannelInactive();
    }

    private void closeResources()
    {
        if (delayedMessage != null)
        {
            ReferenceCountUtil.release(delayedMessage.left);
            delayedMessage.right.setFailure(new ClosedChannelException());
            delayedMessage = null;
        }

        if (scheduledWrite != null)
        {
            scheduledWrite.cancel(false);
            scheduledWrite = null;
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        closeResources();
        ctx.close(promise);
    }
}
