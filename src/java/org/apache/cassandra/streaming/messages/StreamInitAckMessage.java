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
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public class StreamInitAckMessage extends StreamMessage
{
    public static final IVersionedSerializer<StreamInitAckMessage> serializer = new IVersionedSerializer<StreamInitAckMessage>()
    {
        public void serialize(StreamInitAckMessage msg, DataOutputPlus out, int version) throws IOException
        {
            StreamMessage.serialize(msg, out, version);
        }

        public StreamInitAckMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            Pair<UUID, Integer> header = StreamMessage.deserialize(in, version);
            return new StreamInitAckMessage(header.left, header.right);
        }

        public long serializedSize(StreamInitAckMessage msg, int version)
        {
            return StreamMessage.serializedSize(msg, version);
        }
    };

    public StreamInitAckMessage(UUID planId, int sessionIndex)
    {
        super(planId, sessionIndex);
    }

    @Override
    public MessageOut<StreamInitAckMessage> createMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.STREAM_INIT_ACK, this, serializer);
    }

    public Type getType()
    {
        return Type.STREAM_INIT_ACK;
    }

    public IVersionedSerializer<? extends StreamMessage> getSerializer()
    {
        return serializer;
    }
}
