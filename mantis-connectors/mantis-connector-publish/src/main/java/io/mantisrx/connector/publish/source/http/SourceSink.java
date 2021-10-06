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

package io.mantisrx.connector.publish.source.http;

import io.mantisrx.connector.publish.core.EventFilter;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.PortRequest;
import io.mantisrx.runtime.sink.ServerSentEventsSink;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.sink.predicate.Predicate;
import java.util.List;
import java.util.Map;
import rx.Observable;
import rx.functions.Func2;


public class SourceSink implements Sink<String> {

    private final String clientId;

    private Func2<Map<String, List<String>>, Context, Void> preProcessor = new NoOpProcessor();
    private Func2<Map<String, List<String>>, Context, Void> postProcessor = new NoOpProcessor();

    static class NoOpProcessor implements Func2<Map<String, List<String>>, Context, Void> {

        @Override
        public Void call(Map<String, List<String>> t1, Context t2) {
            return null;
        }
    }


    public SourceSink(Func2<Map<String, List<String>>, Context, Void> preProcessor,
                      Func2<Map<String, List<String>>, Context, Void> postProcessor, String mantisClientId) {
        this.postProcessor = postProcessor;
        this.preProcessor = preProcessor;
        this.clientId = mantisClientId;
    }

    @Override
    public void call(Context context, PortRequest portRequest,
                     Observable<String> observable) {
        observable = observable.filter(t1 -> !t1.isEmpty());

        ServerSentEventsSink<String> sink = new ServerSentEventsSink.Builder<String>()
                .withEncoder(data -> data)
                .withPredicate(new Predicate<>("description", new EventFilter(clientId)))
                .withRequestPreprocessor(preProcessor)
                .withRequestPostprocessor(postProcessor)
                .build();

        observable.subscribe();

        sink.call(context, portRequest, observable);
    }
}
