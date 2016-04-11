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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.utils.concurrent.Locks;

public class AppendingByteArrayInputStream extends InputStream
{
    private static final Logger logger = LoggerFactory.getLogger(AppendingByteArrayInputStream.class);

    private static final int DISABLED_WATER_MARK = Integer.MIN_VALUE;

    private final BlockingQueue<byte[]> queue;
    private final int lowWaterMark;
    private final int highWaterMark;
    private final ChannelHandlerContext ctx;

    private byte[] currentBuf;
    private int currentPosition;
    private volatile boolean closed;
    private long bytesRead;

    /**
     * Total count of bytes in all {@link ByteBuf}s held by this instance. This is retained so that we know when to enable/disable
     * the netty channel's auto-read behavior. This value only indicates that total number of bytes in all buffers, and does
     * not distinguish between read and unread bytes - just anything taking up memory.
     */
    private final AtomicInteger liveByteCount;

    public AppendingByteArrayInputStream(int lowWaterMark, int highWaterMark, ChannelHandlerContext ctx)
    {
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.ctx = ctx;
        queue = new LinkedBlockingQueue<>();
        liveByteCount = new AtomicInteger(0);
    }

    public void append(byte[] buf) throws IllegalStateException
    {
        Objects.requireNonNull(buf, "incoming byte array must not be null");
        updateBufferedByteCount(buf.length);
        queue.add(buf);
    }

    void updateBufferedByteCount(int diff)
    {
        int liveBytes = liveByteCount.addAndGet(diff);
        // optimistically check to see if we should try to change the auto-read status. as we need to be *safe* about modifying it,
        // we need a lock :( - so avoid going down that path at all costs
        boolean safeLiveByteCount = highWaterMark == DISABLED_WATER_MARK || liveBytes >= lowWaterMark || liveBytes <= highWaterMark;
        if (safeLiveByteCount)
            return;

        if (highWaterMark != DISABLED_WATER_MARK)
        {
            // TODO:JEB document this
            Locks.monitorEnterUnsafe(this);
            try
            {
                liveBytes = liveByteCount.get();
                if (liveBytes < lowWaterMark)
                    ctx.channel().config().setAutoRead(true);
                else if (liveBytes > highWaterMark)
                    ctx.channel().config().setAutoRead(false);
            }
            finally
            {
                Locks.monitorExitUnsafe(this);
            }
        }
    }

    @Override
    public int read() throws IOException
    {
        if (currentBuf != null)
        {
            if (currentPosition < currentBuf.length)
            {
                byte b = currentBuf[currentPosition];
                currentPosition++;
                bytesRead++;
                return b & 0xFF;
            }

            updateBufferedByteCount(-currentBuf.length);
            currentBuf = null;
        }

        try
        {
            currentBuf = queue.take();
        }
        catch (InterruptedException ie)
        {
            throw new EOFException();
        }

        byte b = currentBuf[0];
        currentPosition = 1;
        bytesRead++;
        return b & 0xFF;
    }

    public int read(byte[] out, int off, final int len) throws IOException
    {
        int remaining = len;
        while (true)
        {
            if (currentBuf != null)
            {
                int currentBufRemaining = currentBuf.length - currentPosition;
                if (currentBufRemaining > 0)
                {
                    int toReadCount = Math.min(remaining, currentBufRemaining);
                    System.arraycopy(currentBuf, currentPosition, out, off, toReadCount);
                    remaining -= toReadCount;
                    currentPosition += toReadCount;

                    if (remaining == 0)
                    {
                        // TODO:JEB refactor this code - to avoid duplication
                        if (currentBufRemaining - toReadCount == 0)
                        {
                            updateBufferedByteCount(-currentBuf.length);
                            currentBuf = null;
                        }
                        bytesRead += len;
                        return len;
                    }
                    off += toReadCount;
                }

                updateBufferedByteCount(-currentBuf.length);
                currentBuf = null; // unreference the buffer ASAP
            }

            try
            {
                currentBuf = queue.take();
                currentPosition = 0;
            }
            catch (InterruptedException ie)
            {
                throw new EOFException();
            }
        }
    }

    public long getBytesRead()
    {
        return bytesRead;
    }
}
