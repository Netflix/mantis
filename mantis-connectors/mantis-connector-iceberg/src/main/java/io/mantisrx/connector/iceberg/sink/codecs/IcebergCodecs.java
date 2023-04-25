/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.connector.iceberg.sink.codecs;

import io.mantisrx.common.codec.Codec;
import io.mantisrx.connector.iceberg.sink.writer.MantisDataFile;
import io.mantisrx.connector.iceberg.sink.writer.MantisRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.IcebergDecoder;
import org.apache.iceberg.data.avro.IcebergEncoder;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;

/**
 * Encoders and decoders for working with Iceberg objects
 * such as {@link Record}s and {@link DataFile}s.
 */
public class IcebergCodecs {

    /**
     * @return a codec for encoding/decoding Iceberg Records.
     */
    public static Codec<Record> record(Schema schema) {
        return new RecordCodec<>(schema);
    }

    public static Codec<MantisRecord> mantisRecord(Schema schema) {
        return new MantisRecordCodec<>(schema);
    }

    /**
     * @return a codec for encoding/decoding DataFiles.
     */
    public static Codec<DataFile> dataFile() {
        return new ObjectCodec<>(DataFile.class);
    }

    public static Codec<MantisDataFile> mantisDataFile() {
        return new ObjectCodec<>(MantisDataFile.class);
    }

    private static class MantisRecordCodec<T extends MantisRecord> implements Codec<T> {

        private final IcebergEncoder<T> encoder;
        private final IcebergDecoder<T> decoder;

        private MantisRecordCodec(Schema schema) {
            Schema newSchema = new Schema(
                Types.NestedField.of(500, false, "record", schema.asStruct()),
                Types.NestedField.of(501, true, "timestamp", Types.LongType.get()));
            this.encoder = new IcebergEncoder<>(newSchema);
            this.decoder = new IcebergDecoder<>(newSchema);
        }

        @Override
        public T decode(byte[] bytes) {
            try {
                return decoder.decode(bytes);
            } catch (IOException e) {
                throw new RuntimeIOException("problem decoding Iceberg record", e);
            }
        }

        @Override
        public byte[] encode(T value) {
            try {
                return encoder.encode(value).array();
            } catch (IOException e) {
                throw new RuntimeIOException("problem encoding encoding Iceberg record", e);
            }
        }
    }

    private static class RecordCodec<T> implements Codec<T> {

        private final IcebergEncoder<T> encoder;
        private final IcebergDecoder<T> decoder;

        private RecordCodec(Schema schema) {
            this.encoder = new IcebergEncoder<>(schema);
            this.decoder = new IcebergDecoder<>(schema);
        }

        @Override
        public T decode(byte[] bytes) {
            try {
                return decoder.decode(bytes);
            } catch (IOException e) {
                throw new RuntimeIOException("problem decoding Iceberg record", e);
            }
        }

        @Override
        public byte[] encode(T value) {
            try {
                return encoder.encode(value).array();
            } catch (IOException e) {
                throw new RuntimeIOException("problem encoding encoding Iceberg record", e);
            }
        }
    }

    private static class ObjectCodec<T> implements Codec<T> {

        private final Class<T> tClass;

        private ObjectCodec(Class<T> tClass) {
            this.tClass = tClass;
        }

        @Override
        public T decode(byte[] bytes) {
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return tClass.cast(in.readObject());
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to convert bytes to DataFile", e);
            }
        }

        @Override
        public byte[] encode(T value) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(value);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write bytes for DataFile: " + value, e);
            }

            return bytes.toByteArray();
        }
    }
}
