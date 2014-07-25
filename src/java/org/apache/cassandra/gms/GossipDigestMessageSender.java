package org.apache.cassandra.gms;

import org.apache.cassandra.net.MessageOut;

import java.net.InetAddress;

public interface GossipDigestMessageSender
{
    void sendOneWay(MessageOut message, InetAddress to, Gossiper sender);
}
