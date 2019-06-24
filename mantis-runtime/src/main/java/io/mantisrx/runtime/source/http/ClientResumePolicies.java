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

import java.util.concurrent.TimeUnit;

import io.mantisrx.runtime.source.http.impl.ServerClientContext;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func0;


/**
 * Convenient methods to create or combine {@link io.mantisrx.runtime.source.http.ClientResumePolicy}s. All the methods
 * follow the following conventions for generic types:
 * <ul>
 * <li>R: The type of a request payload </li>
 * <li>E: The type of the response payload </li>
 * </ul>
 */
public class ClientResumePolicies {

    /**
     * Creates a policy that repeats the given number of times
     *
     * @param maxRepeat The number of times to resume
     * @param <R>       The type of request payload
     * @param <E>       The type of response payload
     */
    public static <R, E> ClientResumePolicy<R, E> maxRepeat(final int maxRepeat) {
        return new ClientResumePolicy<R, E>() {
            @Override
            public Observable<HttpClientResponse<E>> onError(ServerClientContext<R, E> clientContext, int attempts, Throwable error) {
                return getNewResponse(clientContext, attempts);
            }

            @Override
            public Observable<HttpClientResponse<E>> onCompleted(ServerClientContext<R, E> clientContext, int attempts) {
                return getNewResponse(clientContext, attempts);
            }

            private Observable<HttpClientResponse<E>> getNewResponse(ServerClientContext<R, E> clientContext, int attempts) {
                if (attempts <= maxRepeat) {
                    return clientContext.newResponse();
                }

                return null;
            }
        };
    }

    public static <R, E> ClientResumePolicy<R, E> noRepeat() {
        return new ClientResumePolicy<R, E>() {
            @Override
            public Observable<HttpClientResponse<E>> onError(ServerClientContext<R, E> clientContext, int attempts, Throwable error) {
                return null;
            }

            @Override
            public Observable<HttpClientResponse<E>> onCompleted(ServerClientContext<R, E> clientContext, int attempts) {
                return null;
            }

        };
    }


    /**
     * Creates a policy that resumes after given delay.
     *
     * @param delayFunc A function that returns a delay value. The function will be called every time the resume policy is called.
     * @param unit      The unit of the delay value
     * @param <R>       The type of request payload
     * @param <E>       The type of response payload
     */
    public static <R, E> ClientResumePolicy<R, E> delayed(final Func0<Long> delayFunc, final TimeUnit unit) {
        return new ClientResumePolicy<R, E>() {
            @Override
            public Observable<HttpClientResponse<E>> onError(ServerClientContext<R, E> clientContext, int attempts, Throwable error) {
                return createDelayedResponse(clientContext);
            }

            @Override
            public Observable<HttpClientResponse<E>> onCompleted(ServerClientContext<R, E> clientContext, int attempts) {
                return createDelayedResponse(clientContext);
            }

            private Observable<HttpClientResponse<E>> createDelayedResponse(ServerClientContext<R, E> clientContext) {
                return clientContext.newResponse()
                        .delaySubscription(delayFunc.call(), unit);
            }
        };
    }

    /**
     * Returns a new policy that repeats a given policy for the specified times
     *
     * @param policy    The policy to be repeated
     * @param maxRepeat The maximum number of repeats
     * @param <R>       The type of request payload
     * @param <E>       The type of response payload
     */
    public static <R, E> ClientResumePolicy<R, E> maxRepeat(final ClientResumePolicy<R, E> policy, final int maxRepeat) {
        return new ClientResumePolicy<R, E>() {
            @Override
            public Observable<HttpClientResponse<E>> onError(ServerClientContext<R, E> clientContext, int attempts, Throwable error) {
                if (attempts <= maxRepeat) {
                    return policy.onError(clientContext, attempts, error);
                }

                return null;
            }

            @Override
            public Observable<HttpClientResponse<E>> onCompleted(ServerClientContext<R, E> clientContext, int attempts) {
                if (attempts <= maxRepeat) {
                    return policy.onCompleted(clientContext, attempts);
                }

                return null;
            }
        };
    }

}
