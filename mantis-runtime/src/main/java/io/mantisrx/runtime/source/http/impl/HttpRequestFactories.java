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

package io.mantisrx.runtime.source.http.impl;

import io.mantisrx.runtime.source.http.HttpRequestFactory;
import io.netty.buffer.ByteBuf;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;


/**
 * A collection of commonly used {@link io.mantisrx.runtime.source.http.HttpRequestFactory}
 */
public class HttpRequestFactories {

    /**
     * Creates a simple GET request factory that takes a URI and creates GET requests using the URI
     *
     * @param uri The URI used by the created request
     *
     * @return An {@link io.mantisrx.runtime.source.http.HttpRequestFactory} instance used for GET request.
     */
    public static HttpRequestFactory<ByteBuf> createGetFactory(String uri) {
        return new GetRequestFactory(uri);
    }

    /**
     * Creates a factory that produces simple HTTP request that posts to the given URI. The POST request
     * does not take any payload.
     *
     * @param uri The URI to post to
     *
     * @return An {@link io.mantisrx.runtime.source.http.HttpRequestFactory} instance used for creating the POST requests
     */
    public static HttpRequestFactory<ByteBuf> createPostFactory(String uri) {
        return new SimplePostRequestFactory(uri);
    }

    /**
     * Creates a factory that produces HTTP request that posts to a given URI. The POST request
     * will include the given payload. It's the caller's responsibility to serialize content object
     * into byte array. The byte array will not be copied, so the caller should not reuse the byte array
     * after passing it to this method.
     *
     * @param uri    The URI that a request will posts to
     * @param entity The content of a POST request
     *
     * @return A {@link io.mantisrx.runtime.source.http.HttpRequestFactory} instance that creates the specified POST requests.
     */
    public static HttpRequestFactory<ByteBuf> createPostFactory(String uri, byte[] entity) {
        return new PostRequestWithContentFactory(uri, entity);
    }

    private static class GetRequestFactory implements HttpRequestFactory<ByteBuf> {

        private final String uri;

        private GetRequestFactory(String uri) {
            this.uri = uri;
        }

        @Override
        public HttpClientRequest<ByteBuf> create() {
            return HttpClientRequest.createGet(uri);
        }
    }

    private static class SimplePostRequestFactory implements HttpRequestFactory<ByteBuf> {

        private final String uri;

        private SimplePostRequestFactory(String uri) {
            this.uri = uri;
        }

        @Override
        public HttpClientRequest<ByteBuf> create() {
            return HttpClientRequest.createPost(uri);
        }
    }

    private static class PostRequestWithContentFactory implements HttpRequestFactory<ByteBuf> {

        private final String uri;
        private final byte[] content;

        private PostRequestWithContentFactory(String uri, byte[] content) {
            this.uri = uri;
            this.content = content;
        }

        @Override
        public HttpClientRequest<ByteBuf> create() {
            return HttpClientRequest.createPost(uri).withContent(content);
        }
    }

}
