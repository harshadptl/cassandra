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

import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
public class OutgoingFileMessage
{
    public final FileMessageHeader header;
    public final Ref<SSTableReader> ref;
    private final String filename;
    private boolean completed = false;
    private boolean transferring = false;

    public OutgoingFileMessage(Ref<SSTableReader> ref, StreamSession session, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt)
    {
        this.ref = ref;

        SSTableReader sstable = ref.get();
        filename = sstable.getFilename();
        this.header = new FileMessageHeader(sstable.metadata().id,
                                            session.planId(),
                                            session.sessionIndex(),
                                            sequenceNumber,
                                            sstable.descriptor.version,
                                            sstable.descriptor.formatType,
                                            estimatedKeys,
                                            sections,
                                            sstable.compression ? sstable.getCompressionMetadata() : null,
                                            repairedAt,
                                            session.keepSSTableLevel() ? sstable.getSSTableLevel() : 0,
                                            sstable.header == null ? null : sstable.header.toComponent());
    }

    @VisibleForTesting
    public OutgoingFileMessage(FileMessageHeader header, Ref<SSTableReader> ref, String filename)
    {
        this.ref = ref;
        this.header = header;
        this.filename = filename;
    }

    /**
     * Constructor solely for tests
     */
    @VisibleForTesting
    public OutgoingFileMessage()
    {
        this.ref = null;
        this.header = null;
        this.filename = null;
    }

    @VisibleForTesting
    public synchronized void finishTransfer()
    {
        transferring = false;
        //session was aborted mid-transfer, now it's safe to release
        if (completed)
        {
            ref.release();
        }
    }

    @VisibleForTesting
    public synchronized void startTransfer()
    {
        if (completed)
            throw new RuntimeException(String.format("Transfer of file %s already completed or aborted (perhaps session failed?).",
                                                     filename));
        transferring = true;
    }

    public synchronized void complete()
    {
        if (!completed)
        {
            completed = true;
            //release only if not transferring
            if (!transferring)
            {
                ref.release();
            }
        }
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + filename + ")";
    }
}

