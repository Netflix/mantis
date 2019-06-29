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

package io.mantisrx.common.network;


public class HashFunctions {

    private HashFunctions() {}

    public static HashFunction crc32() {
        final HashAlgorithm algorithm = HashAlgorithm.CRC32_HASH;
        return new HashFunction() {
            @Override
            public Long call(byte[] keyBytes) {
                return algorithm.hash(keyBytes);
            }
        };
    }

    public static HashFunction ketama() {
        final HashAlgorithm algorithm = HashAlgorithm.KETAMA_HASH;
        return new HashFunction() {
            @Override
            public Long call(byte[] keyBytes) {
                return algorithm.hash(keyBytes);
            }
        };
    }
}
