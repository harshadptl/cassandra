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
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * StreamMessage is an abstract base class that every messages in streaming protocol inherit.
 */
public abstract class StreamMessage
{
    /** Streaming protocol version */
    public static final int VERSION_30 = 4;
    public static final int VERSION_40 = 4;
    public static final int CURRENT_VERSION = VERSION_40;

    public enum Type
    {
        STREAM_INIT,
        STREAM_INIT_ACK,
        PREPARE_SYN,
        PREPARE_SYNACK,
        PREPARE_ACK,
        FILE,
        COMPLETE,
        RECEIVED,
        RETRY,
        SESSION_FAILED
    }

    public final UUID planId;
    public final int sessionIndex;

    protected StreamMessage(UUID planId, int sessionIndex)
    {
        this.planId = planId;
        this.sessionIndex = sessionIndex;
    }

    public abstract IVersionedSerializer<? extends StreamMessage> getSerializer();

    public abstract MessageOut<? extends StreamMessage> createMessageOut();

    public abstract Type getType();

    /**
     * To be invoked by {@link IVersionedSerializer}s of derived classes.
     */
    protected static void serialize(StreamMessage msg, DataOutputPlus out, int version) throws IOException
    {
        UUIDSerializer.serializer.serialize(msg.planId, out, version);
        out.writeInt(msg.sessionIndex);
    }

    /**
     * To be invoked by {@link IVersionedSerializer}s of derived classes.
     */
    protected static int serializedSize(StreamMessage msg, int version)
    {
        return UUIDGen.UUID_LEN + 4;
    }

    /**
     * To be invoked by {@link IVersionedSerializer}s of derived classes.
     */
    protected static Pair<UUID, Integer> deserialize(DataInputPlus in, int version) throws IOException
    {
        UUID planId = UUIDSerializer.serializer.deserialize(in, version);
        int sessionIndex = in.readInt();
        return Pair.create(planId, sessionIndex);
    }

    @Override
    public String toString()
    {
        return String.format("%s - planId: %s, sessionIndex: %d", getClass().getSimpleName(), planId, sessionIndex);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof StreamMessage))
            return false;
        if (o == this)
            return true;
        StreamMessage msg = (StreamMessage)o;
        return planId.equals(msg.planId) && sessionIndex == msg.sessionIndex;
    }
}
