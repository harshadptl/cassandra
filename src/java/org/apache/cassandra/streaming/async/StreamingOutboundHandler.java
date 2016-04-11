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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;

/**
 * A netty {@link ChannelHandler} responsible for transferring files. Has the ability to defer the transfer of an individual file
 * if, for example, the throttle limit has been reached.
 *
 * Note: this class extends from {@link ChannelDuplexHandler} in order to get the {@link #channelWritabilityChanged(ChannelHandlerContext)}
 * behavior. That function allows us to know when the channel's high water mark has been exceeded (as the channel's writablility
 * will have changed), and thus we can back off reading bytes from disk/writing them to the channel.
 */
class StreamingOutboundHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingOutboundHandler.class);

    enum State { READY, PROCESSING, DONE}

    private final StreamSession session;
    private final int protocolVersion;

    private State state;
    private CurrentMessage currentMessage;

    StreamingOutboundHandler(StreamSession session, int protocolVersion)
    {
        this.session = session;
        this.protocolVersion = protocolVersion;
        state = State.READY;
    }

    /**
     * Responsible for serializing and sending {@link StreamMessage}s.
     * {@link OutgoingFileMessage} headers are serialized first, and then the
     * ssatble is sent via the reentrant {@link #transferFile(ChannelHandlerContext, CurrentMessage)}.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) throws Exception
    {
        if (!canProcessMessage(message, promise))
            return;

        OutgoingFileMessage ofm = (OutgoingFileMessage) message;
        try
        {
            logger.debug("[Stream #{}] Sending {}", session.planId(), ofm);

            CompressionInfo compressionInfo = ofm.header.getCompressionInfo();
            boolean secure = NettyFactory.isSecure(ctx.channel());
            OutboundSstableDataChunker chunker = compressionInfo == null
                      ? new OutboundSstableDataChunker(ctx, ofm)
                      : new OutboundSstableDataChunker(ctx, ofm, compressionInfo, secure);

            state = State.PROCESSING;
            currentMessage = new CurrentMessage(ofm, chunker, promise);
            ByteBuf buf = null;
            try
            {
                buf = serializeHeader(ctx.alloc(), ofm, protocolVersion);
                ctx.writeAndFlush(buf);
            }
            catch (Exception e)
            {
                if (buf != null)
                    buf.release();
                throw e;
            }

            logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                         ofm.ref.get().getFilename(), session.peer, ofm.ref.get().getSSTableMetadata().repairedAt, chunker.getTotalSize());
            transferFile(ctx, currentMessage);
        }
        catch (Throwable t)
        {
            logger.debug("failed to send file", t);
            state = State.DONE;

            // TODO:JEB review this error handling
//            if (currentMessage == null)
//                ofm.ref.release();
//            else
//                currentMessage.chunker.close();
            session.onError(t);
            ctx.close();
        }
    }

    @VisibleForTesting
    boolean canProcessMessage(Object message, ChannelPromise promise)
    {
        boolean canProcess = true;

        if (state == State.DONE)
        {
            promise.tryFailure(new ClosedChannelException());
            canProcess = false;
        }
        else if (state == State.PROCESSING)
        {
            promise.tryFailure(new IllegalStateException("currently processing a outbound message but got another"));
            canProcess = false;
        }
        else if (!(message instanceof OutgoingFileMessage))
        {
            promise.tryFailure(new UnsupportedMessageTypeException("message must be instance of OutgoingFileMessage"));
            canProcess = false;
        }

        if (!canProcess)
        {
            ReferenceCountUtil.release(message);
            // TODO:JEB should we call session.onError() && ctx.close() ??
        }

        return canProcess;
    }

    /**
     * Serializer the file message header. We send along the standard {@link MessagingService#PROTOCOL_MAGIC}
     * and a frame for the header so the recipient can check the validity of the message (via the PROTOCOL_MAGIC)
     * and then wait for the appropriate number of bytes for the header.
     */
    @VisibleForTesting
    static ByteBuf serializeHeader(ByteBufAllocator allocator, OutgoingFileMessage ofm, int protocolVersion) throws IOException
    {
        final int headerSize = 4 + 4;
        final int msgSize = (int) FileMessageHeader.serializer.serializedSize(ofm.header, protocolVersion);
        final int totalSize = headerSize + msgSize;
        ByteBuf buf = allocator.buffer(totalSize, totalSize);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(msgSize);

        ByteBuffer byteBuffer = buf.nioBuffer(buf.writerIndex(), buf.writableBytes());
        @SuppressWarnings("resource")
        DataOutputBuffer buffer = new DataOutputBufferFixed(byteBuffer);
        FileMessageHeader.serializer.serialize(ofm.header, buffer, protocolVersion);
        buf.writerIndex(buf.writerIndex() + byteBuffer.position());

        assert buf.readableBytes() == totalSize;

        return buf;
    }

    /**
     * Transfers chunks of the target sstable. This method can, and probably will, be called mulitple times
     * for a given file as we'll need to delay processing for one of the following reasons:
     * <p>
     * 1) If the {@code ctx#channel} is not writable becasue we've hit the high water mark for
     * the number bytes allowed in the channel, this method will return without sending further chunks.
     * However, when outstanding chunks complete, {@link #onChunkComplete(Future, ChannelHandlerContext, CurrentMessage, boolean)}
     * is invoked, and it will call this method again if the {@code chunker} has more data to transfer.
     * <p>
     * 2) If we couldn't aqcuire the byte count of a chunk from the {@code #limiter}, then the transfer is
     * suspended for some number of milliseconds, and attempted again.
     *
     * Is it critical that, when an exception is thrown, callers invoke {@link StreamingFileChunker#close()} to
     * release any resources.
     */
    private void transferFile(ChannelHandlerContext ctx, CurrentMessage currentMessage)
    {
        StreamingFileChunker chunker = currentMessage.chunker;
        while (chunker.hasNext())
        {
            if (state == State.DONE || !ctx.channel().isWritable())
                return;

            try
            {
                Object chunk = chunker.next();
                boolean isLastChunk = !chunker.hasNext();
                ChannelFuture channelFuture = ctx.writeAndFlush(chunk);
                channelFuture.addListener((future) -> onChunkComplete(future, ctx, currentMessage, isLastChunk));
            }
            catch (Throwable t)
            {
                // TODO:JEB review if throwing an exception is the best here.....
                throw new IllegalStateException("failed to get next chunk for transfer", t);
            }
        }
    }

    /**
     * A callback function invoked after each chunk of the sstable has been sent. A side effect of this method
     * is that on the first failure or the last success (for the given file being transfered) the {@code promise}
     * will be updated appropriately.
     *
     * @param future  The future for the result of transferring a chunk of an sstable.
     * @param ctx     The current netty Channel context.
     * @param isLastChunk indicator if this is the callback for the last chubnk of the file; if true,
     *                    resources can be cleaned up and released and the promise can be fulfilled as a success.
     * @return true if the chunk was sent; else false.
     */
    boolean onChunkComplete(Future<? super Void> future, ChannelHandlerContext ctx, CurrentMessage currentMessage, boolean isLastChunk)
    {
        if (state == State.DONE)
        {
            if (!currentMessage.promise.isDone())
                currentMessage.promise.cancel(false);
            return future.isSuccess();
        }
        else if (state == State.READY)
            // TODO:JEB release resources!!
            throw new AssertionError("shouldn't get to this state (state == READY, but we're still we're processing a chunk)");

        if (!future.isDone())
            return false;

        if (future.isSuccess())
        {
            currentMessage.chunker.updateSessionProgress(session);

            if (isLastChunk)
            {
                SSTableReader reader = currentMessage.ofm.ref.get();
                logger.debug("[Stream #{}] Finished streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                             reader.getFilename(), session.peer, reader.getSSTableMetadata().repairedAt, currentMessage.chunker.getTotalSize());
                currentMessage.chunker.close();
                currentMessage.promise.setSuccess();
                state = State.READY;
                currentMessage = null;
            }
            else
            {
                transferFile(ctx, currentMessage);
            }
            return true;
        }

        // if we got this far, we've failed in some way.
        // set the 'stopped' field out of paranoia
        state = State.DONE;
        currentMessage.chunker.close();

        ChannelFuture channelFuture = (ChannelFuture) future;
        if (channelFuture.isCancelled())
            currentMessage.promise.cancel(false);
        else
            currentMessage.promise.setFailure(future.cause());
        return false;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
    {
        // TODO:JEB this needs help!!
//        logger.info("SHH.channelWritabilityChanged: {}", ctx.channel().isWritable());

        // TODO:JEB check state, make sure we *can* send a message (and that we're currently in the middle of sending one)
        if (ctx.channel().isWritable() && currentMessage != null)
            transferFile(ctx, currentMessage);

        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        ChannelPromise promise = close();
        if (promise != null)
            promise.tryFailure(new ClosedChannelException());

        ctx.fireChannelInactive();
    }

    private ChannelPromise close()
    {
        if (state == State.DONE)
            return null;

        state = State.DONE;

        if (currentMessage == null)
            return null;

        currentMessage.chunker.close();
        ChannelPromise promise = currentMessage.promise;
        currentMessage = null;

        return promise;
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        close();
        ctx.close(promise);
    }

    // should only be used in testing
    @VisibleForTesting
    void setState(State s)
    {
        state = s;
    }

    static class CurrentMessage
    {
        final OutgoingFileMessage ofm;
        final StreamingFileChunker chunker;
        final ChannelPromise promise;

        CurrentMessage(OutgoingFileMessage ofm, StreamingFileChunker chunker, ChannelPromise promise)
        {
            this.ofm = ofm;
            this.chunker = chunker;
            this.promise = promise;
        }
    }
}