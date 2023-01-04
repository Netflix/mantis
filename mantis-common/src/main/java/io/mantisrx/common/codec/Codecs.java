/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.common.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


public class Codecs {

    public static Codec<Integer> integer() {

        return new Codec<Integer>() {
            @Override
            public Integer decode(byte[] bytes) {
                return ByteBuffer.wrap(bytes).getInt();
            }

            @Override
            public byte[] encode(final Integer value) {
                return ByteBuffer.allocate(4).putInt(value).array();
            }
        };
    }

    public static Codec<Long> longNumber() {

        return new Codec<Long>() {
            @Override
            public Long decode(byte[] bytes) {
                return ByteBuffer.wrap(bytes).getLong();
            }

            @Override
            public byte[] encode(final Long value) {
                return ByteBuffer.allocate(8).putLong(value).array();
            }
        };
    }

    private static Codec<String> stringWithEncoding(String encoding) {
        final Charset charset = Charset.forName(encoding);

        return new Codec<String>() {
            @Override
            public String decode(byte[] bytes) {
                return new String(bytes, charset);
            }

            @Override
            public byte[] encode(final String value) {
                return value.getBytes(charset);
            }
        };
    }

    public static Codec<String> stringAscii() {

        final Charset charset = Charset.forName("US-ASCII");

        return new Codec<String>() {
            @Override
            public String decode(byte[] bytes) {
                return new String(bytes, charset);
            }

            @Override
            public byte[] encode(final String value) {
                final byte[] bytes = new byte[value.length()];
                for (int i = 0; i < value.length(); i++)
                    bytes[i] = (byte) value.charAt(i);
                return bytes;
            }
        };
    }

    public static Codec<String> stringUtf8() {
        return stringWithEncoding("UTF-8");
    }

    public static Codec<String> string() {
        return stringUtf8();
    }

    public static Codec<byte[]> bytearray() {
        return new Codec<byte[]>() {
            @Override
            public byte[] decode(byte[] bytes) {
                return bytes;
            }

            @Override
            public byte[] encode(final byte[] value) {
                return value;
            }
        };
    }

    public static <T extends Serializable> Codec<T> javaSerializer() {
        return new Codec<T>() {
            @Override
            public T decode(byte[] bytes) {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                try {
                    ObjectInput in = new ObjectInputStream(bis);
                    return (T) in.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] encode(T value) {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                    try (ObjectOutput out = new ObjectOutputStream(bos)) {
                        out.writeObject(value);
                        return bos.toByteArray();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
