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

package io.mantisrx.server.master.client.diagnostic.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.observers.SerializedObserver;
import rx.subjects.PublishSubject;

/** mechanism for listening for diagnostic messages.  You can record a message, and callers can subscribe to the observable to take action on the messages.
 * This is a simple singleton pattern class */
public class DiagnosticPlugin {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticPlugin.class);

    private final PublishSubject<DiagnosticMessage> messagePublisher = PublishSubject.create();
    private final SerializedObserver<DiagnosticMessage> messagePublisherSerialized = new SerializedObserver<>(messagePublisher);

    private DiagnosticPlugin() {
    }

    /**
     * Record a message for diagnostic purposes.   Serialized order is enforced.
     * @param message the message to record
     */
    public void record(final DiagnosticMessage message) {
        if (message != null) {
            messagePublisherSerialized.onNext(message);
        } else {
            logger.error("RECORDING_NULL_MESSAGE_PROHBIITED");
        }
    }

    /**
     * Return an observable that calling program can process using standard rx idioms.
     * @param maxBackPressureBuffer - maximum number of messages to permit before back pressure exceeeded.
     * @return an observable for processing
     */
    public Observable<DiagnosticMessage> getDiagnosticObservable(final int maxBackPressureBuffer) {
        return messagePublisher.onBackpressureBuffer(maxBackPressureBuffer);
    }

    /** the singleton instance of the diagnostic plugin */
    public static final DiagnosticPlugin INSTANCE = new DiagnosticPlugin();
}
