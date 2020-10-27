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

package io.mantisrx.runtime.sink;

import java.util.ArrayList;
import java.util.List;

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.Metadata;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;


public class Sinks {


    public static <T> Sink<T> eagerSubscribe(final Sink<T> sink) {
        return new Sink<T>() {
            @Override
            public List<ParameterDefinition<?>> getParameters() {
                return sink.getParameters();
            }
            @Override
            public void call(Context c, PortRequest p, Observable<T> o) {
                o.subscribe();
                sink.call(c, p, o);
            }

            @Override
            public void init(Context t) {
                sink.init(t);
            }
        };
    }

    public static <T> SelfDocumentingSink<T> eagerSubscribe(final SelfDocumentingSink<T> sink) {
        return new SelfDocumentingSink<T>() {
            @Override
            public List<ParameterDefinition<?>> getParameters() {
                return sink.getParameters();
            }
            @Override
            public void call(Context c, PortRequest p, Observable<T> o) {
                o.subscribe();
                sink.call(c, p, o);
            }

            @Override
            public Metadata metadata() {
                return sink.metadata();
            }

            @Override
            public void init(Context t) {
                sink.init(t);
            }
        };
    }

    @SafeVarargs
    public static <T> Sink<T> toMany(final Sink<T>... many) {
        return new Sink<T>() {
            @Override
            public List<ParameterDefinition<?>> getParameters() {
                List<ParameterDefinition<?>> parameterDefinitions = new ArrayList<>();
                for (Sink<T> sink : many) {
                    parameterDefinitions.addAll(sink.getParameters());
                }
                return parameterDefinitions;
            }
            @Override
            public void call(Context t1, PortRequest t2, Observable<T> t3) {
                for (Sink<T> sink : many) {
                    sink.call(t1, t2, t3);
                }
            }
            @Override
            public void init(Context t) {
                for(Sink<T> sink : many) {
                    sink.init(t);
                }
            }
        };
    }

    public static <T> ServerSentEventsSink<T> sse(Func1<T, String> encoder) {
        return new ServerSentEventsSink<T>(encoder);
    }

    public static <T> Sink<T> sysout() {
        return new Sink<T>() {
            @Override
            public void call(Context t1, PortRequest p, Observable<T> t2) {
                t2.subscribe(new Observer<T>() {
                    @Override
                    public void onCompleted() {
                        System.out.println("completed");
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                    }

                    @Override
                    public void onNext(T t) {
                        System.out.println(t);
                    }
                });
            }
        };
    }
}
