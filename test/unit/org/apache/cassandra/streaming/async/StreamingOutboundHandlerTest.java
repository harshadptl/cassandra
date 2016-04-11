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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.StreamingOutboundHandler.CurrentMessage;
import org.apache.cassandra.streaming.async.StreamingOutboundHandler.State;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.Pair;

public class StreamingOutboundHandlerTest
{
    private static final ByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;
    private static final int PROTOCOL_VERSION = StreamMessage.CURRENT_VERSION;

    private static final String KEYSPACE = "streamingsendkeyspace";
    private static final String TABLE = "streamingsendtable";

    private static final InetAddress PEER;
    private static final InetAddress CONNECTING;

    static
    {
        DatabaseDescriptor.daemonInitialization();

        try
        {
            PEER = InetAddress.getByName("127.0.0.2");
            CONNECTING = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static TableMetadata metadata;
    private StreamSession session;
    private StreamingOutboundHandler handler;
    private EmbeddedChannel channel;

    @BeforeClass
    public static void before()
    {
        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE, TABLE);
        builder.addPartitionKeyColumn("pk", LongType.instance);
        builder.addClusteringColumn("col", LongType.instance);
        metadata = builder.build();
    }

    @Before
    public void setUp()
    {
        session = new StreamSession(PEER, CONNECTING, 1, true, true, null);
        handler = new StreamingOutboundHandler(session, StreamMessage.CURRENT_VERSION);
        channel = new EmbeddedChannel(handler);
    }

    @Test
    public void serializeHeader() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        sections.add(Pair.create(25L, 50L));
        SerializationHeader serializationHeader = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        FileMessageHeader header = new FileMessageHeader(metadata.id, UUID.randomUUID(),
                                                         100, 1000, BigFormat.latestVersion, SSTableFormat.Type.BIG,
                                                         100, sections, (CompressionMetadata) null, System.currentTimeMillis(), 0, serializationHeader.toComponent());
        OutgoingFileMessage ofm = new OutgoingFileMessage(header, null,  null);
        ByteBuf byteBuf = StreamingOutboundHandler.serializeHeader(ALLOCATOR, ofm, PROTOCOL_VERSION);
        MessagingService.validateMagic(byteBuf.readInt());
        long headerSize = FileMessageHeader.serializer.serializedSize(header, PROTOCOL_VERSION);
        Assert.assertEquals(headerSize, byteBuf.readInt());
        FileMessageHeader deserialized = FileMessageHeader.serializer.deserialize(new DataInputStreamPlus(new ByteBufInputStream(byteBuf)), PROTOCOL_VERSION);
        Assert.assertEquals(header, deserialized);
        Assert.assertFalse(byteBuf.isReadable());
    }

    @Test
    public void onChunkComplete_ChunkSent_Success()
    {
        handler.setState(State.PROCESSING);
        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        future.setSuccess();
        TestChunker chunker = new TestChunker();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), new CurrentMessage(null, chunker, promise), false);

        Assert.assertTrue(chunkSent);
        Assert.assertTrue(!promise.isDone());
    }

    @Test
    public void onChunkComplete_ChunkSent_Fail()
    {
        handler.setState(State.PROCESSING);
        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        Throwable cause = new NullPointerException();
        future.setFailure(cause);
        TestChunker chunker = new TestChunker();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), new CurrentMessage(null, chunker, promise), true);

        Assert.assertFalse(chunkSent);
        Assert.assertTrue(promise.isDone());
        Assert.assertTrue(!promise.isSuccess());
        Assert.assertEquals(cause, promise.cause());
    }

    @Test
    public void onChunkComplete_LastChunkSent_Success()
    {
        handler.setState(State.PROCESSING);
        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        future.setSuccess();
        TestChunker chunker = new TestChunker();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), new CurrentMessage(null, chunker, promise), true);

        Assert.assertTrue(chunkSent);
        Assert.assertTrue(promise.isSuccess());
    }

    @Test
    public void onChunkComplete_Stopped_ChunkSent()
    {
        handler.setState(State.DONE);

        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        future.setSuccess();
        TestChunker chunker = new TestChunker();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), new CurrentMessage(null, chunker, promise), true);

        Assert.assertTrue(chunkSent);
        Assert.assertTrue(promise.isCancelled());
    }

    @Test
    public void onChunkComplete_Stopped_ChunkNotSent()
    {
        handler.setState(State.DONE);

        ChannelFuture future = channel.newPromise();
        future.cancel(false);
        TestChunker chunker = new TestChunker();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), new CurrentMessage(null, chunker, promise), true);

        Assert.assertFalse(chunkSent);
        Assert.assertTrue(promise.isCancelled());
    }

    @Test
    public void onChunkComplete_FutureNotDone()
    {
        handler.setState(State.PROCESSING);
        ChannelFuture future = channel.newPromise();
        TestChunker chunker = new TestChunker();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), new CurrentMessage(null, chunker, promise), true);

        Assert.assertFalse(chunkSent);
    }

    @Test
    public void canProcessMessage_Good()
    {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        OutgoingFileMessage ofm = new OutgoingFileMessage();
        Assert.assertTrue(handler.canProcessMessage(ofm, promise));
        Assert.assertFalse(promise.isDone());
    }

    @Test
    public void canProcessMessage_ProcessingState()
    {
        handler.setState(State.PROCESSING);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        OutgoingFileMessage ofm = new OutgoingFileMessage();
        Assert.assertFalse(handler.canProcessMessage(ofm, promise));
        Assert.assertTrue(promise.isDone());
        Assert.assertFalse(promise.isSuccess());
    }

    @Test
    public void canProcessMessage_DoneState()
    {
        handler.setState(State.DONE);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        OutgoingFileMessage ofm = new OutgoingFileMessage();
        Assert.assertFalse(handler.canProcessMessage(ofm, promise));
        Assert.assertTrue(promise.isDone());
        Assert.assertFalse(promise.isSuccess());
    }

    @Test
    public void canProcessMessage_InstanceTypeWrong()
    {
        handler.setState(State.PROCESSING);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.canProcessMessage(new CompleteMessage(UUID.randomUUID(), 42), promise));
        Assert.assertTrue(promise.isDone());
        Assert.assertFalse(promise.isSuccess());
    }

    private static class TestChunker implements StreamingFileChunker
    {
        public boolean hasNext()
        {
            return false;
        }

        public int nextChunkSize()
        {
            return 0;
        }

        public Object next()
        {
            return null;
        }

        public void updateSessionProgress(StreamSession session)
        {

        }

        public Iterator<Object> iterator()
        {
            return this;
        }

        public void close()
        {

        }

        public long getTotalSize()
        {
            return 0;
        }
    }
}
