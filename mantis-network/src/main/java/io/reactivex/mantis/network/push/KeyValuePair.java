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

public class KeyValuePair<K, V> {

    private long keyBytesHashed;
    private byte[] keyBytes;
    private V value;

    public KeyValuePair(long keyBytesHashed, byte[] keyBytes, V value) {
        this.keyBytesHashed = keyBytesHashed;
        this.keyBytes = keyBytes;
        this.value = value;
    }

    public long getKeyBytesHashed() {
        return keyBytesHashed;
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public V getValue() {
        return value;
    }
}
