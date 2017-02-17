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

package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Pair;

public class StreamMessagesSerializersTest
{
    private static final int PROTOCOL_VERSION = StreamMessage.CURRENT_VERSION;

    private static final String KEYSPACE = "streamingmsgserializekeyspace";
    private static final String TABLE = "streamingmsgserializetable";

    private static TableMetadata metadata;

    @BeforeClass
    public static void before()
    {
        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE, TABLE);
        builder.addPartitionKeyColumn("pk", LongType.instance);
        builder.addClusteringColumn("col", LongType.instance);
        metadata = builder.build();
    }

    @Test
    public void completeMessage() throws IOException
    {
        serializeRoundTrip(new CompleteMessage(UUID.randomUUID(), 42), CompleteMessage.serializer);
    }

    @Test
    public void prepareSynMessage() throws IOException
    {
        serializeRoundTrip(new PrepareSynMessage(UUID.randomUUID(), 42), PrepareSynMessage.serializer);
    }

    @Test
    public void prepareSynAckMessage() throws IOException
    {
        serializeRoundTrip(new PrepareSynAckMessage(UUID.randomUUID(), 42), PrepareSynAckMessage.serializer);
    }

    @Test
    public void prepareAckMessage() throws IOException
    {
        serializeRoundTrip(new PrepareAckMessage(UUID.randomUUID(), 42), PrepareAckMessage.serializer);
    }

    @Test
    public void receivedMessage() throws IOException
    {
        serializeRoundTrip(new ReceivedMessage(UUID.randomUUID(), 42, metadata.id, 7), ReceivedMessage.serializer);
    }

    @Test
    public void streamFailedMessage() throws IOException
    {
        serializeRoundTrip(new SessionFailedMessage(UUID.randomUUID(), 42), SessionFailedMessage.serializer);
    }

    @Test
    public void streamInitMessage_WithPendingRepair() throws IOException
    {
        StreamInitMessage msg = new StreamInitMessage(InetAddress.getByName("127.0.0.1"), 123, UUID.randomUUID(),
                                                      "test-me", true, true, UUID.randomUUID());
        serializeRoundTrip(msg, StreamInitMessage.serializer);
    }

    @Test
    public void streamInitMessage_WithoutPendingRepair() throws IOException
    {
        StreamInitMessage msg = new StreamInitMessage(InetAddress.getByName("127.0.0.1"), 123, UUID.randomUUID(),
                                                      "test-me", true, true, null);
        serializeRoundTrip(msg, StreamInitMessage.serializer);
    }

    @Test
    public void streamInitAckMessage() throws IOException
    {
        serializeRoundTrip(new StreamInitAckMessage(UUID.randomUUID(), 42), StreamInitAckMessage.serializer);
    }

    @Test
    public void compressionInfo_Null() throws IOException
    {
        serializeRoundTrip(null, CompressionInfo.serializer);
    }

    @Test
    public void compressionInfo_NotNull() throws IOException
    {
        int chunkCount = 4;
        CompressionMetadata.Chunk[] chunks = new CompressionMetadata.Chunk[chunkCount];
        for (int i = 0; i < chunkCount; i++)
            chunks[i] = new CompressionMetadata.Chunk(i * 20, 20);

        CompressionInfo info = new CompressionInfo(chunks, CompressionParams.DEFAULT);
        serializeRoundTrip(info, CompressionInfo.serializer);
    }

    @Test
    public void roundtrip_FileMessageHeader_NoCompression() throws IOException
    {
        roundtrip_FileMessageHeader(null);
    }

    @Test
    public void roundtrip_FileMessageHeader_WithCompression() throws IOException
    {
        int chunkCount = 4;
        try(Memory offsets = Memory.allocate(chunkCount * 8L))
        {
            for (int i = 0; i < chunkCount; i++)
                offsets.setLong(i * 8L, i * 17);

            CompressionMetadata compressionMetadata = new CompressionMetadata("metadata-file", CompressionParams.DEFAULT, offsets, 8, 100, 100);
            roundtrip_FileMessageHeader(compressionMetadata);
        }
    }

    private void roundtrip_FileMessageHeader(CompressionMetadata compressionMetadata) throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        sections.add(Pair.create(25L, 50L));

        SerializationHeader serializationHeader = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        FileMessageHeader msg = new FileMessageHeader(metadata.id, UUID.randomUUID(),
                                                      100, 1000, BigFormat.latestVersion, SSTableFormat.Type.BIG,
                                                      100, sections, compressionMetadata, System.currentTimeMillis(), 0, serializationHeader.toComponent());

        serializeRoundTrip(msg, FileMessageHeader.serializer);
    }

    private <T> void serializeRoundTrip(T msg, IVersionedSerializer<T> serializer) throws IOException
    {
        long size = serializer.serializedSize(msg, PROTOCOL_VERSION);

        ByteBuffer buf = ByteBuffer.allocate((int)size);
        DataOutputPlus out = new DataOutputBufferFixed(buf);
        serializer.serialize(msg, out, PROTOCOL_VERSION);
        Assert.assertEquals(size, buf.position());

        buf.flip();
        DataInputPlus in = new DataInputBuffer(buf, false);
        T deserialized = serializer.deserialize(in, PROTOCOL_VERSION);
        Assert.assertEquals(msg, deserialized);
    }

    /**
     * Need a separate test for the parent {@link StreamMessage} as it is abstract (and can't be instantiated directly)
     */
    @Test
    public void roundtrip_SreamMessage() throws IOException
    {
        StreamMessage msg = new CompleteMessage(UUID.randomUUID(), 42);
        long size = StreamMessage.serializedSize(msg, PROTOCOL_VERSION);

        ByteBuffer buf = ByteBuffer.allocate((int)size);
        DataOutputPlus out = new DataOutputBufferFixed(buf);
        StreamMessage.serialize(msg, out, PROTOCOL_VERSION);
        Assert.assertEquals(size, buf.position());

        buf.flip();
        DataInputPlus in = new DataInputBuffer(buf, false);
        Pair<UUID, Integer> deserialized = StreamMessage.deserialize(in, PROTOCOL_VERSION);
        Assert.assertEquals(msg.planId, deserialized.left);
        Assert.assertEquals(msg.sessionIndex, deserialized.right.intValue());
    }
}
