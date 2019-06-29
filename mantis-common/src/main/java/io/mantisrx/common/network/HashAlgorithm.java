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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;


/**
 * Known hashing algorithms for locating a server for a key.
 * Note that all hash algorithms return 64-bits of hash, but only the lower
 * 32-bits are significant.  This allows a positive 32-bit number to be
 * returned for all cases.
 */
public enum HashAlgorithm {


    CRC32_HASH,
    KETAMA_HASH;

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

    /**
     * Compute the hash for the given key.
     *
     * @return a positive integer hash
     */
    public long hash(final byte[] keyBytes) {
        long rv = 0;
        switch (this) {
        case CRC32_HASH:
            // return (crc32(shift) >> 16) & 0x7fff;
            CRC32 crc32 = new CRC32();
            crc32.update(keyBytes);
            rv = (crc32.getValue() >> 16) & 0x7fff;
            break;
        case KETAMA_HASH:
            byte[] bKey = computeMd5(keyBytes);
            rv = ((long) (bKey[3] & 0xFF) << 24)
                    | ((long) (bKey[2] & 0xFF) << 16)
                    | ((long) (bKey[1] & 0xFF) << 8)
                    | (bKey[0] & 0xFF);
            break;
        default:
            assert false;
        }
        return rv & 0xffffffffL; /* Truncate to 32-bits */
    }
}
