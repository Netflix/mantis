/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.connectors.kafka;

import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.concurrent.TimeUnit;

/**
 * Ackable used to wrap the data read from kafka to allow providing feedback to the source when the payload is consumed.
 */
public class KafkaAckable {

    private final Subject<KafkaDataNotification, KafkaDataNotification> subject;
    private final KafkaData kafkaData;
    private final long createTimeNano = System.nanoTime();

    public KafkaAckable(KafkaData data, SerializedSubject<KafkaDataNotification, KafkaDataNotification> ackSubject) {
        this.kafkaData = data;
        this.subject = ackSubject;
    }

    public KafkaAckable(KafkaData data, Subject<KafkaDataNotification, KafkaDataNotification> ackSubject) {
        this.kafkaData = data;
        this.subject = ackSubject;
    }

    public void ack() {
        KafkaDataNotification n = KafkaDataNotification.ack(getKafkaData(),
            TimeUnit.MILLISECONDS.convert(System.nanoTime() - createTimeNano, TimeUnit.NANOSECONDS));
        subject.onNext(n);
    }

    /**
     * NACK indicating that the message was not processed and should be
     * returned to the source.
     *
     */
    public void nack() {
        KafkaDataNotification n = KafkaDataNotification.nack(getKafkaData(),
                                                             TimeUnit.MILLISECONDS.convert(System.nanoTime() - createTimeNano, TimeUnit.NANOSECONDS));
        subject.onNext(n);

    }

    /**
     * There was an error processing the message.  Depending on the implementation
     * of the source the message may either be,
     * 1.  Dropped
     * 2.  Replayed
     * 3.  Posted to a poison queue
     * @param t
     */
    public void error(Throwable t) {
        KafkaDataNotification n = KafkaDataNotification.error(getKafkaData(), t,
            TimeUnit.MILLISECONDS.convert(System.nanoTime() - createTimeNano, TimeUnit.NANOSECONDS));
        subject.onNext(n);

    }

    /**
     * @return Get the internal message being Ackable'd
     */
    public KafkaData getKafkaData() {
        return kafkaData;
    }



}
