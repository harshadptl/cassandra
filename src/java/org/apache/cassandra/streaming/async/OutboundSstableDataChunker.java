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
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingUtils;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.Pair;

/**
 * Used when only tranferring subsections of an sstable, and thus only the DATA component is shipped.
 * Responsible for determining the bounds of the next chunk of the sstable to transfer.
 *
 * There are three "modes" of an sstable transfer that need to be handled somewhat differently:
 *
 * 1) uncompressed sstable - data needs to be read into user space so it can be manipulated (checksum validation, possiblly
 * on-the-fly compression).
 *
 * 2) compressed sstable, transferred with SSL/TLS - data needs to be read into user space as that is where the TLS encryption
 * needs to happen.
 *
 * 3) compressed sstable, transferred without SSL/TLS - data can be streamed via zero-copy transfer as the data does not
 * need to be manipulated (it can be sent "as-is").
 *
 * In order to perform zero-copy transfers with netty, a {@link DefaultFileRegion} instance *must* be used sent down the channel
 * (the netty transport, and it's C bindings, are hard coded to *only* do zero-copy operations with a {@link DefaultFileRegion}).
 * However, {@link DefaultFileRegion} requires a reference to a {@link FileChannel}, which we *do not* hand out
 * from {@link SSTableReader}; instead those channel(s) are internalized behind a {@link ChannelProxy} which doesn't expose
 * the {@link FileChannel}. All this to say, we need to manage our own reference to the channel of the file to stream.
 *
 * Note: it is assumed that the {@link #sections} of the file to transfer are: non-overlapping, in-order,
 * and within the bounds of the target file. The first two are not critical, but the last one is.
 *
 * Note: upon creation, an instance will take ownership of the {@link OutgoingFileMessage} passed into the constructor,
 * and thus this instance is responsbible for releasing any resources held by the {@link OutgoingFileMessage}.
 */
class OutboundSstableDataChunker implements StreamingFileChunker
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundSstableDataChunker.class);

    /**
     * chunk size used on the uncompressed sstable path.
     */
    public static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    /**
     * chunk size used on the compressed ssatble path.
     */
    public static final int CHUNK_SIZE = 20 * 1024 * 1024;

    static final int EOF = -1;

    private final ByteBufAllocator allocator;
    private final String fileName;
    private final ChecksumValidator validator;
    private final List<Pair<Long, Long>> sections;
    private final boolean compressed;
    private final boolean secureTransfer;
    private final long totalSize;
    private final int chunkSize;

    /**
     * A reference to the underlying file to be streamed. See class-level documentation for more information, but,
     * basically, this is only used for zero-copy transfers.
     */
//    private final FileInputStream fileInputStream;

    private final ChannelProxy channelProxy;
    private final OutgoingFileMessage ofm;

    /**
     * Overall count of bytes written, used for monitoring/reporting.
     */
    private long progress;

    private int sectionIdx;

    private long nextStartPosition;
    private long nextEndPosition;

    private long nextValidatorStartPosition;
    private long nextValidatorEndPosition;

    private boolean isEOF;
    private boolean needsRecalucaltion = true;

    OutboundSstableDataChunker(ChannelHandlerContext ctx, OutgoingFileMessage ofm) throws IOException
    {
        this.allocator = ctx.alloc();
        this.ofm = ofm;
        fileName = ofm.ref.get().descriptor.filenameFor(Component.DATA);
        this.secureTransfer = false;
//        fileInputStream = null;
        channelProxy = ofm.ref.get().getDataChannel();
        this.sections = ofm.header.sections;
        compressed = false;
        totalSize = StreamingUtils.totalSize(sections);

        if (new File(ofm.ref.get().descriptor.filenameFor(Component.CRC)).exists())
        {
            validator = DataIntegrityMetadata.checksumValidator(ofm.ref.get().descriptor);
            chunkSize = validator.chunkSize;
        }
        else
        {
            validator = null;
            chunkSize = DEFAULT_CHUNK_SIZE;
        }
    }

    OutboundSstableDataChunker(ChannelHandlerContext ctx, OutgoingFileMessage ofm, CompressionInfo compressionInfo, boolean secureTransfer)
    {
        this.allocator = ctx.alloc();
        this.ofm = ofm;
        fileName = ofm.ref.get().descriptor.filenameFor(Component.DATA);
        this.secureTransfer = secureTransfer;
        compressed = true;

        if (!secureTransfer)
        {
            // this allows us to do zero-copy transfers in netty - this is the unholy part :(
            channelProxy = null;
        }
        else
        {
//            fileInputStream = null;
            channelProxy = ofm.ref.get().getDataChannel();
        }

        validator = null;
        sections = getTransferSections(compressionInfo.chunks);
        totalSize = StreamingUtils.totalSize(compressionInfo.chunks);
        chunkSize = CHUNK_SIZE;
    }

    // chunks are assumed to be sorted by offset
    private static List<Pair<Long, Long>> getTransferSections(CompressionMetadata.Chunk[] chunks)
    {
        List<Pair<Long, Long>> transferSections = new ArrayList<>();
        Pair<Long, Long> lastSection = null;
        for (CompressionMetadata.Chunk chunk : chunks)
        {
            if (lastSection != null)
            {
                if (chunk.offset == lastSection.right)
                {
                    // extend previous section to end of this chunk
                    lastSection = Pair.create(lastSection.left, chunk.offset + chunk.length + 4); // 4 bytes for CRC
                }
                else
                {
                    transferSections.add(lastSection);
                    lastSection = Pair.create(chunk.offset, chunk.offset + chunk.length + 4);
                }
            }
            else
            {
                lastSection = Pair.create(chunk.offset, chunk.offset + chunk.length + 4);
            }
        }
        if (lastSection != null)
            transferSections.add(lastSection);
        return transferSections;
    }

    /**
     * A constructor to be used for testing only the iteration aspects of this class (updating the file positions)
     * and not the actual reading of data from the file.
     */
    @VisibleForTesting
    OutboundSstableDataChunker(ByteBufAllocator allocator, List<Pair<Long, Long>> sections, ChecksumValidator validator, ChannelProxy proxy)
    {
        this.allocator = allocator;
        this.sections = sections;
        this.validator = validator;
        this.channelProxy = proxy;
        totalSize = StreamingUtils.totalSize(sections);

        compressed = false;
        secureTransfer = false;
        fileName = null;
//        fileInputStream = null;
        ofm = null;

        chunkSize = validator == null ? DEFAULT_CHUNK_SIZE : validator.chunkSize;
    }

    public Iterator<Object> iterator()
    {
        return this;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public void updateSessionProgress(StreamSession session)
    {
        session.progress(fileName, ProgressInfo.Direction.OUT, progress, totalSize);
    }

    public boolean hasNext()
    {
        if (needsRecalucaltion)
            calculateNextPositions();

        return !isEOF;
    }

    public int nextChunkSize()
    {
        if (needsRecalucaltion)
            calculateNextPositions();

        // TODO:JEB this is incorrect wrt to uncompressed ssatbles, as we'll compress the data on-the-fly
        // fix when we support compressing on-the-fly again
        return isEOF ? EOF : (int)(nextEndPosition - nextStartPosition);
    }

    /**
     * Determine the values for {@link #nextStartPosition} and {@link #nextEndPosition}, taking into account where in the
     * current {@link #sections} we are as well the {@link #chunkSize}.
     */
    @VisibleForTesting
    void calculateNextPositions()
    {
        // check if we're at the end
        if (isEOF || sections.isEmpty() || (sectionIdx == sections.size() - 1 && nextEndPosition == sections.get(sectionIdx).right))
        {
            isEOF = true;
            needsRecalucaltion = false;
            return;
        }

        Pair<Long, Long> currentSection = sections.get(sectionIdx);
        nextStartPosition = nextEndPosition;

        // we're at the very beginning
        if (nextStartPosition <= 0)
        {
            assert sectionIdx == 0;
            nextStartPosition = currentSection.left;
        }
        // we're at the end of the current section
        else if (nextEndPosition >= currentSection.right)
        {
            sectionIdx++;
            if (sectionIdx >= sections.size())
            {
                isEOF = true;
                return;
            }
            currentSection = sections.get(sectionIdx);
            nextStartPosition = currentSection.left;
        }
        // we're partially into the current section
        else
        {
            nextStartPosition = nextEndPosition;
        }

        final int toTransfer;
        // if we're transferring a compressed sstable without SSL/TLS, send the entire section down (let DefaultFileRegion deal chunking it).
        // with DefaultFileRegion, no bytes are read into user space, and thus do not contribute toward the ChannelOutboundBuffer size & the high water mark
        if (compressed && !secureTransfer)
        {
            toTransfer = (int)(currentSection.right - currentSection.left);
        }
        else
        {
            final long length = currentSection.right - nextStartPosition;
            toTransfer = (int) Math.min(chunkSize, length);
        }

        nextEndPosition = nextStartPosition + toTransfer;

        if (validator != null)
        {
            // nudge the nextEndPosition down to align on the checksum chuck's boundary. we may end up with a small
            // block to stream on the first pass through here, but it simplifies the complexity of this code and will be dramitaclly more efficient
            // wrt disk use (not rereading data if not aligned on the checksum boundary) and cpu use (not recalculating the checksum for the same block).
            nextValidatorStartPosition = validator.chunkStart(nextStartPosition);
            nextValidatorEndPosition = nextValidatorStartPosition + chunkSize;

            if (currentSection.right > nextValidatorEndPosition)
                nextEndPosition = nextValidatorEndPosition;
            else if (nextValidatorEndPosition > channelProxy.size()) // we may be at the end of the file
                nextValidatorEndPosition = channelProxy.size();
        }

        needsRecalucaltion = false;
    }

    /**
     * Gets the next transferable chunk of the ssatble. The returned object depends on cases as defined in the class-level
     * documentation, but, in brief, if we're doing a zero-copy transfer, {@link DefaultFileRegion} is returned,
     * else a {@link ByteBuf} is returned.
     */
    public Object next()
    {
        Object retVal;
        if (compressed)
        {
            if (secureTransfer)
            {
                retVal = readChunk(allocator, channelProxy, nextStartPosition, nextEndPosition);
            }
            else
            {
                try
                {
                    @SuppressWarnings("resources") // the stream & channel will be closed by DefaultFileRegion#close
                    FileInputStream fileInputStream = new FileInputStream(ofm.ref.get().getDataChannel().filePath());
                    retVal = new DefaultFileRegion(fileInputStream.getChannel(), nextStartPosition, nextEndPosition - nextStartPosition);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }
        else
        {
            ByteBuf buf;
            if (validator != null)
                buf = readChunk(allocator, channelProxy, validator, nextValidatorStartPosition, nextValidatorEndPosition, nextStartPosition, nextEndPosition);
            else
                buf = readChunk(allocator, channelProxy, nextStartPosition, nextEndPosition);

            // TODO:JEB this is where we can apply on-the-fly compression
            // if we do compression, we'll need to pass along a per-chunk 'header' that includes
            // compressed block length (to know how many bytes to wait for on receiver) and uncompressed
            // block size (so we create an appropriately sized buffer).
            // NOTE: StreamWriter/StreamReader uses LZ4Block{Output,Input}Stream, so not sure how to mimic that here :-( :-( :-( :-(

            retVal = buf;
        }

        markHasConsumedCurrentChunk();
        return retVal;
    }

    @VisibleForTesting
    void markHasConsumedCurrentChunk()
    {
        needsRecalucaltion = true;
        progress += nextEndPosition - nextStartPosition;
    }


    /**
     * Performs a simple read of the file from the {@link #channelProxy}.
     */
    @VisibleForTesting
    static ByteBuf readChunk(ByteBufAllocator allocator, ChannelProxy proxy, long startPosition, long endPosition)
    {
        // this will be a reasonable buffer size due to the calculations in calculateNextPositions()
        int size = (int)(endPosition - startPosition);
        ByteBuf buf = allocator.directBuffer(size, size);
        ByteBuffer buffer = buf.nioBuffer(0, size);
        proxy.read(buffer, startPosition);

        buf.writerIndex(buffer.position());

        return buf;
    }

    /**
     * Performs a read of the file from the {@link #channelProxy}, and then validates the checksum. The complicating factor
     * for this method is that checksum validation may need to read extra bytes versus what we'll actually transfer
     * due to the bounds of the checksummed block. All the logic for calculating positions for the bytes
     * to stream versus the bytes to read are all handled in {@link #calculateNextPositions()}.
     */
    @VisibleForTesting
    static ByteBuf readChunk(ByteBufAllocator allocator, ChannelProxy proxy, ChecksumValidator validator,
                             long validatorStartPosition, long validatorEndPosition, long streamStartPosition, long streamEndPosition)
    {
        final int readSize = (int)(validatorEndPosition - validatorStartPosition);
        ByteBuf buf = allocator.buffer(readSize, readSize);

        // read
        ByteBuffer byteBuffer = buf.nioBuffer(0, readSize);
        proxy.read(byteBuffer, validatorStartPosition);
        byteBuffer.flip();

        // validate
        try
        {
            validator.seek(validatorStartPosition);
            validator.validate(byteBuffer);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // adjust the buf's indices
        final int endOffset = (int)(validatorEndPosition - streamEndPosition);
        buf.writerIndex(byteBuffer.limit() - endOffset);
        buf.readerIndex((int)(streamStartPosition - validatorStartPosition));

        return buf;
    }

    public void close()
    {
        // should only be null when testing!
//        FileUtils.closeQuietly(fileInputStream);
    }

    public long getProgress()
    {
        return progress;
    }
}
