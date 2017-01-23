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
package org.apache.cassandra.security;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import org.apache.cassandra.config.EncryptionOptions;

/**
 * A Factory for providing and setting up Client and Server SSL wrapped
 * Socket and ServerSocket
 */
public final class SSLFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

    @VisibleForTesting
    static volatile boolean checkedExpiry = false;

    private static AtomicReference<SslContext> clientSslContext = new AtomicReference<>();
    private static AtomicReference<SslContext> serverSslContext = new AtomicReference<>();

    public static SSLServerSocket getServerSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLServerSocket serverSocket = (SSLServerSocket)ctx.getServerSocketFactory().createServerSocket();
        try
        {
            serverSocket.setReuseAddress(true);
            prepareSocket(serverSocket, options);
            serverSocket.bind(new InetSocketAddress(address, port), 500);
            return serverSocket;
        }
        catch (IllegalArgumentException | SecurityException | IOException e)
        {
            serverSocket.close();
            throw e;
        }
    }

    /** Create a socket and connect */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port, localAddress, localPort);
        try
        {
            prepareSocket(socket, options);
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Create a socket and connect, using any local address */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port);
        try
        {
            prepareSocket(socket, options);
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Just create a socket */
    public static SSLSocket getSocket(EncryptionOptions options) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket();
        try
        {
            prepareSocket(socket, options);
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Sets relevant socket options specified in encryption settings */
    private static void prepareSocket(SSLServerSocket serverSocket, EncryptionOptions options)
    {
        String[] suites = filterCipherSuites(serverSocket.getSupportedCipherSuites(), options.cipher_suites);
        if(options.require_endpoint_verification)
        {
            SSLParameters sslParameters = serverSocket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            serverSocket.setSSLParameters(sslParameters);
        }
        serverSocket.setEnabledCipherSuites(suites);
        serverSocket.setNeedClientAuth(options.require_client_auth);
    }

    /** Sets relevant socket options specified in encryption settings */
    private static void prepareSocket(SSLSocket socket, EncryptionOptions options)
    {
        String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
        if(options.require_endpoint_verification)
        {
            SSLParameters sslParameters = socket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(sslParameters);
        }
        socket.setEnabledCipherSuites(suites);
    }
    @SuppressWarnings("resource")

    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        TrustManager[] trustManagers = null;
        if (buildTruststore)
            trustManagers = buildTrustManagerFactory(options).getTrustManagers();

        KeyManagerFactory kmf = buildKeyManagerFactory(options);

        try
        {
            SSLContext ctx = SSLContext.getInstance(options.protocol);
            ctx.init(kmf.getKeyManagers(), trustManagers, null);
            return ctx;
        }
        catch (Exception e)
        {
            throw new IOException("Error creating/initializing the SSL Context", e);
        }
    }

    static TrustManagerFactory buildTrustManagerFactory(EncryptionOptions options) throws IOException
    {
        try (FileInputStream tsf = new FileInputStream(options.truststore))
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(options.algorithm);
            KeyStore ts = KeyStore.getInstance(options.store_type);
            ts.load(tsf, options.truststore_password.toCharArray());
            tmf.init(ts);
            return tmf;
        }
        catch (Exception e)
        {
            throw new IOException("failed to build trust manager store for secure connections", e);
        }
    }

    static KeyManagerFactory buildKeyManagerFactory(EncryptionOptions options) throws IOException
    {
        try (FileInputStream ksf = new FileInputStream(options.keystore))
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(options.algorithm);
            KeyStore ks = KeyStore.getInstance(options.store_type);
            ks.load(ksf, options.keystore_password.toCharArray());
            if (!checkedExpiry)
            {
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.getCertificate(alias).getType().equals("X.509"))
                    {
                        Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                        if (expires.before(new Date()))
                            logger.warn("Certificate for {} expired on {}", alias, expires);
                    }
                }
                checkedExpiry = true;
            }
            kmf.init(ks, options.keystore_password.toCharArray());
            return kmf;
        }
        catch (Exception e)
        {
            throw new IOException("failed to build trust manager store for secure connections", e);
        }
    }

    public static String[] filterCipherSuites(String[] supported, String[] desired)
    {
        if (Arrays.equals(supported, desired))
            return desired;
        List<String> ldesired = Arrays.asList(desired);
        ImmutableSet<String> ssupported = ImmutableSet.copyOf(supported);
        String[] ret = Iterables.toArray(Iterables.filter(ldesired, Predicates.in(ssupported)), String.class);
        if (desired.length > ret.length && logger.isWarnEnabled())
        {
            Iterable<String> missing = Iterables.filter(ldesired, Predicates.not(Predicates.in(Sets.newHashSet(ret))));
            logger.warn("Filtering out {} as it isn't supported by the socket", Iterables.toString(missing));
        }
        return ret;
    }

    /** get a netty {@link SslContext} instance */
    public static SslContext getSslContext(EncryptionOptions options, boolean buildTruststore, boolean forServer) throws IOException
    {
        return getSslContext(options, buildTruststore, forServer, OpenSsl.isAvailable());
    }

    /**
     * Get a netty {@link SslContext} instance.
     */
    @VisibleForTesting
    static SslContext getSslContext(EncryptionOptions options, boolean buildTruststore, boolean forServer, boolean useOpenSsl) throws IOException
    {
        // the netty folks informed me that a) SslContext is expensive to create (as well as to destroy),
        // b) it consumes a lof of resources (especially direct memory),
        // but c) it can be reused across connection (assuming the SSL params are the same)

        if (forServer && serverSslContext.get() != null)
            return serverSslContext.get();
        if (!forServer && clientSslContext.get() != null)
            return clientSslContext.get();

        TrustManagerFactory tmf = null;
        if (buildTruststore)
            tmf = buildTrustManagerFactory(options);

        SslContextBuilder builder;
        if (forServer)
        {
            /*
                There is a case where the netty/openssl combo might not support using KeyManagerFactory. If so, we would need
                to fall back to passing in a file reference for both a x509 and PKCS#8 private key file in PEM format (see
                {@link SslContextBuilder#forServer(File, File, String)}). However, we are not supporting that now to keep
                the config/yaml API simple.
             */
            KeyManagerFactory kmf = buildKeyManagerFactory(options);
            builder = SslContextBuilder.forServer(kmf).sslProvider(useOpenSsl ? SslProvider.OPENSSL : SslProvider.JDK);
            builder.clientAuth(options.require_client_auth ? ClientAuth.REQUIRE : ClientAuth.NONE);
        }
        else
        {
            builder = SslContextBuilder.forClient();
        }

        // TODO:JEB fix this!!!!
        SslContext ctx = builder.ciphers(null)//Arrays.asList(options.cipher_suites), SupportedCipherSuiteFilter.INSTANCE)
                        .trustManager(tmf)
                        .build();

        AtomicReference<SslContext> ref = forServer ? serverSslContext : clientSslContext;
        if (ref.compareAndSet(null, ctx))
            return ctx;
        else
            return ref.get();
    }
}
