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

package org.apache.cassandra.streaming;

import java.util.Collection;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.streaming.async.StreamingInboundHandler;
import org.apache.cassandra.utils.Pair;

/**
 * A temporary place to put some functions whilst things are being moved around
 */
public class StreamingUtils
{
    public static long totalSize(Collection<Pair<Long, Long>> sections)
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    public static long totalSize(CompressionMetadata.Chunk[] chunks)
    {
        long size = 0;
        // calculate total length of transferring chunks
        for (CompressionMetadata.Chunk chunk : chunks)
            size += chunk.length + StreamingInboundHandler.CHECKSUM_LENGTH;
        return size;
    }
}
