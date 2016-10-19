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

package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionHandshakeResult;

/**
 * A collection of data opints to be passed around for outbound connections
 */
public class OutboundConnectionParams
{
    final InetSocketAddress localAddr;
    final InetSocketAddress remoteAddr;
    final int protocolVersion;
    final Consumer<ConnectionHandshakeResult> callback;
    final ServerEncryptionOptions encryptionOptions;
    final NettyFactory.Mode mode;
    final int bufferSize;
    final boolean maybeCoalesce;
    final boolean compress;
    final AtomicLong droppedMessageCount;
    final AtomicLong completedMessageCount;

    public OutboundConnectionParams(InetSocketAddress localAddr, InetSocketAddress remoteAddr, int protocolVersion, int bufferSize,
                                    Consumer<ConnectionHandshakeResult> callback, ServerEncryptionOptions encryptionOptions, NettyFactory.Mode mode,
                                    boolean maybeCoalesce, boolean compress, AtomicLong droppedMessageCount, AtomicLong completedMessageCount)
    {
        this.localAddr = localAddr;
        this.remoteAddr = remoteAddr;
        this.protocolVersion = protocolVersion;
        this.bufferSize = bufferSize;
        this.callback = callback;
        this.encryptionOptions = encryptionOptions;
        this.mode = mode;
        this.maybeCoalesce = maybeCoalesce;
        this.compress = compress;
        this.droppedMessageCount = droppedMessageCount;
        this.completedMessageCount = completedMessageCount;
    }

    OutboundConnectionParams updateProtocolVersion(int protocolVersion)
    {
        return new OutboundConnectionParams(localAddr, remoteAddr, protocolVersion, bufferSize, callback,
                                            encryptionOptions, mode, maybeCoalesce, compress, droppedMessageCount, completedMessageCount);
    }
}
