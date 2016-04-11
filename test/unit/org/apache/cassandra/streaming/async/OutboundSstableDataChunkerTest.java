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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

public class OutboundSstableDataChunkerTest
{
    private static final ByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;
    private ChannelProxy channelProxy;

    @Before
    public void setUp()
    {
        channelProxy = new ChannelProxy(new File("./CHANGES.txt"));
    }

    @After
    public void tearDown()
    {
        FileUtils.closeQuietly(channelProxy);
    }

    @Test
    public void iterate_EmptySections()
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        iterate(sections);
    }

    private OutboundSstableDataChunker iterate(List<Pair<Long, Long>> sections)
    {
        return iterate(sections, null);
    }

    private OutboundSstableDataChunker iterate(List<Pair<Long, Long>> sections, ChecksumValidator validator)
    {
        OutboundSstableDataChunker chunker = new OutboundSstableDataChunker(ALLOCATOR, sections, validator, channelProxy);
        while (chunker.hasNext())
            chunker.markHasConsumedCurrentChunk();
        Assert.assertEquals(chunker.getProgress(), chunker.getTotalSize());
        Assert.assertEquals(OutboundSstableDataChunker.EOF, chunker.nextChunkSize());
        return chunker;
    }

    @Test
    public void iterate_OneSmallSection()
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long start = 1200;
        long end = 1400;
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections);
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_OneLargeSection()
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long start = 1200;
        long end = 923492347L;
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections);
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_ManySections()
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long totalSize = 0;

        Random r = new Random(1234567890L);
        long start;
        long end = 0;
        for (int i = 0; i < 11; i++)
        {
            start = end + r.nextInt(2048);
            end = start + r.nextInt(1000000);
            sections.add(Pair.create(start, end));
            totalSize += end - start;
        }
        OutboundSstableDataChunker chunker = iterate(sections);
        Assert.assertEquals(totalSize, chunker.getProgress());
    }

    @Test
    public void iterate_OneSection_WithValidation_SmallChunk_NoOffsets() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long start = 1200;
        long end = 1400;
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections, new Validator(channelProxy, 100));
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_OneSection_WithValidation_SmallChunk_WithOffsets() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long start = 1250;
        long end = 1350;
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections, new Validator(channelProxy, 100));
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_OneSection_WithValidation_LargeChunk_NoOffsets() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long start = 1200;
        long end = 1400;
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections, new Validator(channelProxy, 64000));
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_OneSection_WithValidation_LargeChunk_WithOffsets() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        int chunkSize = 64000;
        long start = 1250;
        long end = start + (long)(chunkSize * 2.3);
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections, new Validator(channelProxy, chunkSize));
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_OneSection_WithValidation_AtEndOfFile() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long start = channelProxy.size() - 100;
        long end = start + 98;
        sections.add(Pair.create(start, end));
        OutboundSstableDataChunker chunker = iterate(sections, new Validator(channelProxy, 100));
        Assert.assertEquals(end - start, chunker.getProgress());
    }

    @Test
    public void iterate_ManySections_WithValidation_WithOffsets() throws IOException
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        long totalSize = 0;

        Random r = new Random(987654321L);
        long start;
        long end = 0;
        for (int i = 0; i < 5; i++)
        {
            start = end + r.nextInt(256);
            end = start + r.nextInt(10000);
            sections.add(Pair.create(start, end));
            totalSize += end - start;
        }
        OutboundSstableDataChunker chunker = iterate(sections, new Validator(channelProxy, 1000));
        Assert.assertEquals(totalSize, chunker.getProgress());
    }

    @Test
    public void readChunk_Simple()
    {
        int readSize = 1024;
        ByteBuf chunk = OutboundSstableDataChunker.readChunk(ALLOCATOR, channelProxy, 0, readSize);
        Assert.assertEquals(readSize, chunk.readableBytes());
    }

    @Test
    public void readChunk_WithValidation_Aligned() throws IOException
    {
        long start = 0;
        long end = 100;
        ByteBuf chunk = OutboundSstableDataChunker.readChunk(ALLOCATOR, channelProxy, new Validator(channelProxy, (int)(end - start)), start, end, start, end);
        Assert.assertEquals(end - start, chunk.readableBytes());
    }

    @Test
    public void readChunk_WithValidation_Unaligned() throws IOException
    {
        long start = 0;
        long end = 100;
        long streamStart = 0;
        long streamEnd = 100;
        ByteBuf chunk = OutboundSstableDataChunker.readChunk(ALLOCATOR, channelProxy, new Validator(channelProxy, (int)(end - start)), start, end, streamStart, streamEnd);
        Assert.assertEquals(streamEnd - streamStart, chunk.readableBytes());
    }

    private static class Validator extends ChecksumValidator
    {
        private final ChannelProxy proxy;

        Validator(ChannelProxy proxy, int chunkSize) throws IOException
        {
            super(null, null, chunkSize);
            this.proxy = proxy;
        }

        public void seek(long offset)
        {
            Assert.assertTrue(offset <= proxy.size());
        }

        public void validate(ByteBuffer buffer) throws IOException
        {
            Assert.assertTrue(buffer.remaining() <= chunkSize);

            // move the position to the end of the buffer, just like what the parent function does
            buffer.position(buffer.limit());
        }

        public void close()
        {

        }
    }
}