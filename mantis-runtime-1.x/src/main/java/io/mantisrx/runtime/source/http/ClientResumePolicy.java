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

package io.mantisrx.runtime.source.http;

import io.mantisrx.runtime.source.http.impl.ServerClientContext;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;


public interface ClientResumePolicy<R, E> {

    /**
     * Returns a new observable when the subscriber's onError() method is called. Return null should resumption will
     * not be run.
     *
     * @param clientContext The client context that offers access to the subscribed server and the logic of creating the
     *                      original stream
     * @param attempts      The number of resumptions so far
     * @param error         The error the resulted in the onError() event
     */
    Observable<HttpClientResponse<E>> onError(ServerClientContext<R, E> clientContext, int attempts, Throwable error);

    /**
     * Returns a new observable when the subscribed stream is completed. Return null should resumption will
     * not be run.
     *
     * @param clientContext The client context that offers access to the subscribed server and the logic of creating the
     *                      original stream
     * @param attempts      The number of resumptions so far
     */
    Observable<HttpClientResponse<E>> onCompleted(ServerClientContext<R, E> clientContext, int attempts);
}
