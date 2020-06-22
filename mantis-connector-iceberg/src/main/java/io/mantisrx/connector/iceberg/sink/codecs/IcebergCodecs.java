/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connector.iceberg.sink.codecs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import io.mantisrx.common.codec.Codec;
import org.apache.iceberg.data.Record;

public class IcebergCodecs {

    public static Codec<Record> record() {
        return new Codec<Record>() {
            @Override
            public Record decode(byte[] bytes) {
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                    return (Record) in.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException("Failed to convert bytes to DataFile", e);
                }
            }

            @Override
            public byte[] encode(Record value) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                    out.writeObject(value);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write bytes for DataFile: " + value, e);
                }

                return bytes.toByteArray();
            }
        };
    }
}
