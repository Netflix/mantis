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

package io.mantisrx.connector.publish.source.http;

import io.mantisrx.connector.publish.core.QueryRegistry;
import rx.subjects.Subject;


public interface SourceHttpServer {

    public static final String METRIC_GROUP = "PushServer";

    enum State {
        NOTINITED,
        INITED,
        RUNNING,
        SHUTDOWN
    }

    void init(QueryRegistry registry, Subject<String, String> eventSubject, int port) throws InterruptedException;

    void startServer();

    void shutdownServer();
}
