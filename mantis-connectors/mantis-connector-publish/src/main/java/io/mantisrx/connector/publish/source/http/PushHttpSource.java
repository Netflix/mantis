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

import com.mantisrx.common.utils.MantisSourceJobConstants;
import io.mantisrx.connector.publish.core.QueryRegistry;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.WorkerMap;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.type.IntParameter;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import io.reactivx.mantis.operators.DropOperator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;


public class PushHttpSource implements Source<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushHttpSource.class);

    private final Subject<String, String> eventSubject = new SerializedSubject<>(PublishSubject.create());

    private final QueryRegistry queryRegistry;

    private AtomicReference<WorkerMap> workerMapAtomicReference = new AtomicReference<>(new WorkerMap(new HashMap<>()));

    private static final String NETTY_THREAD_COUNT_PARAM_NAME = "nettyThreadCount";
    private static final String SERVER_PORT = "serverPort";

    private SourceHttpServer server;

    public PushHttpSource(QueryRegistry registry) {
        this.queryRegistry = registry;
    }

    @Override
    public Observable<Observable<String>> call(Context context, Index index) {
        return Observable.just(eventSubject
                .lift(new DropOperator<>("incoming_" + PushHttpSource.class.getCanonicalName() + "_batch"))
                .onErrorResumeNext((e) -> Observable.empty()));
    }

    @Override
    public void init(Context context, Index index) {
        LOGGER.info("Initializing PushHttpSource");
        int threadCount = (Integer) context.getParameters().get(NETTY_THREAD_COUNT_PARAM_NAME, 4);
        int serverPort = (Integer) context.getParameters().get(SERVER_PORT);

        LOGGER.info("PushHttpSource server starting at Port " + serverPort);

        server = new NettySourceHttpServer(context, threadCount);
        try {
            server.init(queryRegistry, eventSubject, serverPort);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        server.startServer();

        context.getWorkerMapObservable().subscribeOn(Schedulers.io()).subscribe((workerMap) -> {
            LOGGER.info("Got WorkerUpdate" + workerMap);
            workerMapAtomicReference.set(workerMap);
        });

        LOGGER.info("PushHttpSource server started");
    }

    @Override
    public List<ParameterDefinition<?>> getParameters() {
        List<ParameterDefinition<?>> parameters = new ArrayList<>();

        parameters.add(new IntParameter()
                .name(NETTY_THREAD_COUNT_PARAM_NAME)
                .validator(Validators.range(1, 8))
                .defaultValue(4)
                .build());

        parameters.add(new StringParameter()
                .name(MantisSourceJobConstants.ZONE_LIST_PARAMETER_NAME)
                .description("list of Zones")
                .validator(Validators.alwaysPass())
                .defaultValue("")
                .build());

        parameters.add(new StringParameter()
                .name(MantisSourceJobConstants.TARGET_APP_PARAMETER_NAME)
                .description("target app")
                .validator(Validators.alwaysPass())
                .defaultValue("")
                .build());

        parameters.add(new IntParameter()
            .name(SERVER_PORT)
            .description("port to serve the output")
            .validator(Validators.range(1000, 65535))
            .defaultValue(5054)
            .build());

        parameters.add(new StringParameter()
                .name(MantisSourceJobConstants.TARGET_ASG_CSV_PARAM)
                .description("target ASGs CSV regex")
                .validator(Validators.alwaysPass())
                .defaultValue("")
                .build());

        return parameters;
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            server.shutdownServer();
            server = null;
        }
    }
}
