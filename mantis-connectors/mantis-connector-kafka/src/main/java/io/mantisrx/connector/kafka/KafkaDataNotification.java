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

package io.mantisrx.connector.kafka;

public class KafkaDataNotification {

    public enum Kind {
        ACK,
        NACK,
        ERR
    }

    public static  KafkaDataNotification ack(KafkaData event, long elapsedMillis) {
        return new KafkaDataNotification(event, Kind.ACK, null,  elapsedMillis);
    }


    public static  KafkaDataNotification nack(KafkaData event, long elapsedMillis) {
        return new KafkaDataNotification(event, Kind.NACK, null, elapsedMillis);
    }

    public static  KafkaDataNotification error(KafkaData request, Throwable t, long elapsedMillis) {
        return new KafkaDataNotification(request, Kind.ERR, t,  elapsedMillis);
    }


    private final KafkaData value;
    private final Kind kind;
    private final Throwable error;
    private long elapsedMillis;

    protected KafkaDataNotification(KafkaData value, Kind kind, Throwable error, long elapsedMillis) {
        this.value = value;
        this.kind = kind;
        this.error = error;

        this.elapsedMillis = elapsedMillis;
    }

    public Throwable getError() {
        return error;
    }

    public boolean hasError() {
        return error != null;
    }

    /**
     * @return
     */
    public Kind getKind() {
        return kind;
    }


    public KafkaData getValue() {
        return value;
    }

    public boolean hasValue() {
        return value != null;
    }

    public boolean isError() {
        return kind.equals(Kind.ERR);
    }

    public boolean isSuccess() {
        return kind.equals(Kind.ACK);
    }

    /**
     * Time it took to execute the operation for which this notification is generated
     *
     * @return
     */
    public long getElapsed() {
        return elapsedMillis;
    }


}

