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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ExecutionException;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.TransparentDataEncryptionOptions;

/**
 * A factory for loading encryption keys from {@link KeyProvider} instances.
 * Maintains a cache of loaded keys to avoid invoking the key provider on every call.
 */
public class CipherFactory
{
    private final Logger logger = LoggerFactory.getLogger(CipherFactory.class);

    private final SecureRandom secureRandom;
    private final LoadingCache<String, Key> cache;
    private final int ivLength;
    private final KeyProvider keyProvider;

    public CipherFactory(TransparentDataEncryptionOptions options)
    {
        logger.info("initializing CipherFactory");
        ivLength = options.iv_length;

        try
        {
            secureRandom = SecureRandom.getInstance("SHA1PRNG");
            Class<KeyProvider> keyProviderClass = (Class<KeyProvider>)Class.forName(options.key_provider.class_name);
            Constructor ctor = keyProviderClass.getConstructor(TransparentDataEncryptionOptions.class);
            keyProvider = (KeyProvider)ctor.newInstance(options);
        }
        catch (Exception e)
        {
            throw new RuntimeException("couldn't load cipher factory", e);
        }

        cache = CacheBuilder.newBuilder() // by default cache is unbounded
                .maximumSize(64) // a value large enough that we should never even get close (so nothing gets evicted)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .removalListener(new RemovalListener<String, Key>()
                {
                    public void onRemoval(RemovalNotification<String, Key> notice)
                    {
                        // maybe reload the key? (to avoid the reload being on the user's dime)
                        logger.info("key {} removed from cipher key cache", notice.getKey());
                    }
                })
                .build(new CacheLoader<String, Key>()
                {
                    @Override
                    public Key load(String alias) throws Exception
                    {
                        logger.info("loading secret key for alias {}", alias);
                        return keyProvider.getSecretKey(alias);
                    }
                });
    }

    public Cipher getEncryptor(String transformation, String keyAlias) throws IOException
    {
        byte[] iv = new byte[ivLength];
        secureRandom.nextBytes(iv);
        return buildCipher(transformation, keyAlias, iv, Cipher.ENCRYPT_MODE);
    }

    public Cipher getDecryptor(String transformation, String keyAlias, byte[] iv) throws IOException
    {
        assert iv != null : "trying to decrypt, but the initialization vector is empty";
        return buildCipher(transformation, keyAlias, iv, Cipher.DECRYPT_MODE);
    }

    private Cipher buildCipher(String transformation, String keyAlias, byte[] iv, int cipherMode) throws IOException
    {
        try
        {
            Cipher cipher = Cipher.getInstance(transformation);
            Key key = retrieveKey(keyAlias);
            cipher.init(cipherMode, key, new IvParameterSpec(iv));
            return cipher;
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e)
        {
            logger.error("could not build cipher", e);
            throw new IOException("cannot load cipher", e);
        }
    }

    private Key retrieveKey(String keyAlias) throws IOException
    {
        try
        {
            return cache.get(keyAlias);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            throw new IOException("failed to load key from cache: " + keyAlias, e);
        }
    }
}
