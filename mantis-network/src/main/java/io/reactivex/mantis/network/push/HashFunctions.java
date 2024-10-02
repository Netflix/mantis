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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import net.openhft.hashing.LongHashFunction;


public class HashFunctions {

    private HashFunctions() {}

    public static HashFunction ketama() {
        return new HashFunction() {
            @Override
            public long computeHash(byte[] keyBytes) {
                byte[] bKey = computeMd5(keyBytes);
                return ((long) (bKey[3] & 0xFF) << 24)
                        | ((long) (bKey[2] & 0xFF) << 16)
                        | ((long) (bKey[1] & 0xFF) << 8)
                        | (bKey[0] & 0xFF);
            }
        };
    }

    public static HashFunction xxh3() {
        return bytes -> LongHashFunction.xx3().hashBytes(bytes);
    }

    public static byte[] computeMd5(byte[] keyBytes) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        md5.update(keyBytes);
        return md5.digest();
    }
}
