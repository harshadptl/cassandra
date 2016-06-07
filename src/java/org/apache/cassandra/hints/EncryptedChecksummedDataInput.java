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
package org.apache.cassandra.hints;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContext.ChannelProxyReadChannel;

class EncryptedChecksummedDataInput extends ChecksummedDataInput
{
    private final EncryptionContext encryptionContext;
    private final ChannelProxyReadChannel readChannel;

    protected EncryptedChecksummedDataInput(ChannelProxy channel, EncryptionContext encryptionContext, long filePosition)
    {
        super(channel);
        assert encryptionContext != null;
        this.encryptionContext = encryptionContext;
        readChannel = new EncryptionContext.ChannelProxyReadChannel(channel, filePosition);
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return getSourcePosition() == channel.size() && buffer.remaining() == 0;
    }

    public long getSourcePosition()
    {
        return readChannel.getCurrentPosition();
    }

    @Override
    protected void readBuffer()
    {
        try
        {
            buffer = encryptionContext.decrypt(readChannel, buffer, true);
            buffer.flip();
        }
        catch (IOException ioe)
        {
            throw new FSReadError(ioe, getPath());
        }
    }

    @SuppressWarnings("resource") // Closing the ChecksummedDataInput will close the underlying channel.
    static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, EncryptionContext encryptionContext)
    {
        long position = input.getPosition();
        input.close();

        return new EncryptedChecksummedDataInput(new ChannelProxy(input.getPath()), encryptionContext, position);
    }

    @VisibleForTesting
    public EncryptionContext getEncryptionContext()
    {
        return encryptionContext;
    }
}
