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

package io.reactivex.mantis.network.push;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import rx.functions.Func1;


public class Routers {

    private Routers() {}


    public static <K, V> Router<KeyValuePair<K, V>> consistentHashingLegacyTcpProtocol(String name,
                                                                                       final Func1<K, byte[]> keyEncoder,
                                                                                       final Func1<V, byte[]> valueEncoder) {
        return new ConsistentHashingRouter<K, V>(name, new Func1<KeyValuePair<K, V>, byte[]>() {
            @Override
            public byte[] call(KeyValuePair<K, V> kvp) {
                byte[] keyBytes = kvp.getKeyBytes();
                byte[] valueBytes = valueEncoder.call(kvp.getValue());
                return
                        // length + opcode + notification type + key length
                        ByteBuffer.allocate(4 + 1 + 1 + 4 + keyBytes.length + valueBytes.length)
                                .putInt(1 + 1 + 4 + keyBytes.length + valueBytes.length) // length
                                .put((byte) 1) // opcode
                                .put((byte) 1) // notification type
                                .putInt(keyBytes.length) // key length
                                .put(keyBytes) // key bytes
                                .put(valueBytes) // value bytes
                                .array();
            }
        }, HashFunctions.xxh3());
    }

    private static byte[] dataPayload(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + data.length);
        buffer.putInt(1 + data.length); // length, plus additional byte for opcode
        buffer.put((byte) 1); // opcode for next operator
        buffer.put(data);
        return buffer.array();
    }

    public static <T> Router<T> roundRobinLegacyTcpProtocol(String name, final Func1<T, byte[]> toBytes) {
        return new RoundRobinRouter<>(name, new Func1<T, byte[]>() {
            @Override
            public byte[] call(T t1) {
                byte[] data = toBytes.call(t1);
                return dataPayload(data);
            }
        });
    }

    public static <T> Router<T> roundRobinSse(String name, final Func1<T, String> toString) {

        final byte[] prefix = "data: ".getBytes();
        final byte[] nwnw = "\n\n".getBytes();

        return new RoundRobinRouter<>(name, new Func1<T, byte[]>() {
            @Override
            public byte[] call(T data) {
                byte[] bytes = string().call(toString.call(data));
                return bytes;
                //				ByteBuffer buffer = ByteBuffer.allocate(prefix.length+bytes.length+nwnw.length);
                //				buffer.put(prefix);
                //				buffer.put(bytes);
                //				buffer.put(nwnw);
                //				return buffer.array();
            }
        });
    }

    private static Func1<String, byte[]> stringWithEncoding(String encoding) {
        final Charset charset = Charset.forName(encoding);
        return new Func1<String, byte[]>() {
            @Override
            public byte[] call(final String value) {
                return value.getBytes(charset);
            }
        };
    }

    public static Func1<String, byte[]> stringAscii() {
        return new Func1<String, byte[]>() {
            @Override
            public byte[] call(final String value) {
                final byte[] bytes = new byte[value.length()];
                for (int i = 0; i < value.length(); i++)
                    bytes[i] = (byte) value.charAt(i);
                return bytes;
            }
        };
    }

    public static Func1<String, byte[]> stringUtf8() {
        return stringWithEncoding("UTF-8");
    }

    public static Func1<String, byte[]> string() {
        return stringUtf8();
    }
}
