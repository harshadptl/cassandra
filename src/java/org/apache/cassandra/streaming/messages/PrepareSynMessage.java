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
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.utils.Pair;

public class PrepareSynMessage extends StreamMessage
{
    public static final IVersionedSerializer<PrepareSynMessage> serializer = new IVersionedSerializer<PrepareSynMessage>()
    {
        public void serialize(PrepareSynMessage prepareSynMessage, DataOutputPlus out, int version) throws IOException
        {
            StreamMessage.serialize(prepareSynMessage, out, version);

            // requests
            out.writeInt(prepareSynMessage.requests.size());
            for (StreamRequest request : prepareSynMessage.requests)
                StreamRequest.serializer.serialize(request, out, version);
            // summaries
            out.writeInt(prepareSynMessage.summaries.size());
            for (StreamSummary summary : prepareSynMessage.summaries)
                StreamSummary.serializer.serialize(summary, out, version);
        }

        public PrepareSynMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            Pair<UUID, Integer> header = StreamMessage.deserialize(in, version);
            PrepareSynMessage message = new PrepareSynMessage(header.left, header.right);
            // requests
            int numRequests = in.readInt();
            for (int i = 0; i < numRequests; i++)
                message.requests.add(StreamRequest.serializer.deserialize(in, version));
            // summaries
            int numSummaries = in.readInt();
            for (int i = 0; i < numSummaries; i++)
                message.summaries.add(StreamSummary.serializer.deserialize(in, version));
            return message;
        }

        public long serializedSize(PrepareSynMessage prepareSynMessage, int version)
        {
            long size = StreamMessage.serializedSize(prepareSynMessage, version);
            size += 4 + 4; // count of requests and count of summaries
            for (StreamRequest request : prepareSynMessage.requests)
                size += StreamRequest.serializer.serializedSize(request, version);
            for (StreamSummary summary : prepareSynMessage.summaries)
                size += StreamSummary.serializer.serializedSize(summary, version);
            return size;
        }
    };

    /**
     * Streaming requests
     */
    public final Collection<StreamRequest> requests = new ArrayList<>();

    /**
     * Summaries of streaming out
     */
    public final Collection<StreamSummary> summaries = new ArrayList<>();

    public PrepareSynMessage(UUID planId, int sessionIndex)
    {
        super(planId, sessionIndex);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Prepare SYN (");
        sb.append(requests.size()).append(" requests, ");
        int totalFile = 0;
        for (StreamSummary summary : summaries)
            totalFile += summary.files;
        sb.append(" ").append(totalFile).append(" files");
        sb.append('}');
        return sb.toString();
    }

    @Override
    public MessageOut<PrepareSynMessage> createMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.STREAM_PREPARE_SYN, this, serializer);
    }

    public Type getType()
    {
        return Type.PREPARE_SYN;
    }

    public IVersionedSerializer<? extends StreamMessage> getSerializer()
    {
        return serializer;
    }
}
