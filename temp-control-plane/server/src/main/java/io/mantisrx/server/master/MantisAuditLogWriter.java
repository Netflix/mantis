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

package io.mantisrx.server.master;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.observers.SerializedObserver;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;


public class MantisAuditLogWriter {

    private static final Logger logger = LoggerFactory.getLogger(MantisAuditLogWriter.class);
    private static MantisAuditLogWriter instance;
    private final PublishSubject<MantisAuditLogEvent> subject;
    private final int backPressureBufferSize = 1000;

    private MantisAuditLogWriter(Subscriber<MantisAuditLogEvent> subscriber) {
        subject = PublishSubject.create();
        subject
                .onBackpressureBuffer(backPressureBufferSize, new Action0() {
                    @Override
                    public void call() {
                        logger.warn("Exceeded back pressure buffer of " + backPressureBufferSize);
                    }
                })

                .observeOn(Schedulers.computation())
                .subscribe(subscriber);
    }

    public static void initialize(Subscriber<MantisAuditLogEvent> subscriber) {
        instance = new MantisAuditLogWriter(subscriber);
    }

    public static MantisAuditLogWriter getInstance() {
        if (instance == null)
            throw new IllegalStateException(MantisAuditLogWriter.class.getName() + " must be initialized before use");
        return instance;
    }

    public Observer<MantisAuditLogEvent> getObserver() {
        return new SerializedObserver<>(subject);
    }
}
