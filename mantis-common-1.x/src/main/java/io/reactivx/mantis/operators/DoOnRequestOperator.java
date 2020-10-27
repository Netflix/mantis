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

package io.reactivx.mantis.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable.Operator;
import rx.Producer;
import rx.Subscriber;


public class DoOnRequestOperator<T> implements Operator<T, T> {

    private static final Logger logger = LoggerFactory.getLogger(DoOnRequestOperator.class);
    private final String name;

    public DoOnRequestOperator(String name) {
        this.name = name;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super T> child) {
        final RequestSubscriber<T> requestSubscriber = new RequestSubscriber<T>(child);
        child.setProducer(new Producer() {

            @Override
            public void request(long n) {
                if (n > 10000) {
                    logger.info("DoOnRequest + " + name + " Requested------>: " + n);
                }
                requestSubscriber.requestMore(n);
            }

        });

        return requestSubscriber;

    }

    static class RequestSubscriber<T> extends Subscriber<T> {

        final Subscriber<? super T> child;
        boolean once = false;

        public RequestSubscriber(Subscriber<? super T> child) {
            super(child);
            this.child = child;
        }

        @Override
        public void onStart() {
            if (!once) {
                // don't request anything until the child requests via requestMore
                request(0);
            }
        }

        void requestMore(long n) {
            once = true;
            request(n);
        }

        @Override
        public void onCompleted() {
            child.onCompleted();
        }

        @Override
        public void onError(Throwable e) {
            child.onError(e);
        }

        @Override
        public void onNext(T t) {
            child.onNext(t);
        }

    }
}
