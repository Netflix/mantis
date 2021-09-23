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

package io.mantisrx.connector.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class KafkaData {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] rawBytes;
    /* parsedEvent is present if the raw bytes were already decoded */
    private volatile Optional<Map<String, Object>> parsedEvent;
    private final Optional<String> key;
    private final String streamId;
    private int mantisKafkaConsumerId;


    public KafkaData(String topic,
                     int partition,
                     long offset,
                     byte[] rawBytes,
                     Optional<Map<String, Object>> parsedEvent,
                     Optional<String> key,
                     int mantisKafkaConsumerId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.rawBytes = rawBytes;
        this.parsedEvent = parsedEvent;
        this.key = key;
        this.mantisKafkaConsumerId = mantisKafkaConsumerId;
        this.streamId = new StringBuilder(topic).append('-').append(partition).toString();
    }

    public KafkaData(ConsumerRecord<String, byte[]> m,
                     Optional<Map<String, Object>> parsedEvent,
                     Optional<String> key,
                     int mantisKafkaConsumerId) {
        this(m.topic(), m.partition(), m.offset(), m.value(), parsedEvent, key, mantisKafkaConsumerId);
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getRawBytes() {
        return rawBytes;
    }

    public int getMantisKafkaConsumerId() {
        return mantisKafkaConsumerId;
    }

    public String getStreamId() {
        return this.streamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaData kafkaData = (KafkaData) o;
        return partition == kafkaData.partition &&
            offset == kafkaData.offset &&
            mantisKafkaConsumerId == kafkaData.mantisKafkaConsumerId &&
            topic.equals(kafkaData.topic) &&
            Arrays.equals(rawBytes, kafkaData.rawBytes) &&
            parsedEvent.equals(kafkaData.parsedEvent) &&
            key.equals(kafkaData.key);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, partition, offset, parsedEvent, key, mantisKafkaConsumerId);
        result = 31 * result + Arrays.hashCode(rawBytes);
        return result;
    }

    public Optional<Map<String, Object>> getParsedEvent() {
        return parsedEvent;
    }

    public void setParsedEvent(final Map<String, Object> parsedEvent) {
        this.parsedEvent = Optional.ofNullable(parsedEvent);
    }

    public Optional<String> getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "KafkaData{" +
            "topic='" + topic + '\'' +
            ", partition=" + partition +
            ", offset=" + offset +
            ", rawBytes=" + Arrays.toString(rawBytes) +
            ", parsedEvent=" + parsedEvent +
            ", key=" + key +
            '}';
    }
}
