/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.filters;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.zuul.filters.http.HttpSyncEndpoint;
import com.netflix.zuul.message.http.HttpHeaderNames;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import io.mantisrx.api.proto.AppDiscoveryMap;
import io.mantisrx.api.services.AppStreamDiscoveryService;
import com.netflix.zuul.stats.status.StatusCategoryUtils;
import com.netflix.zuul.stats.status.ZuulStatusCategory;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vavr.control.Either;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.function.Function;

@Slf4j
public class AppStreamDiscovery extends HttpSyncEndpoint {

    private final AppStreamDiscoveryService appStreamDiscoveryService;
    private final ObjectMapper objectMapper;
    private static final String APPNAME_QUERY_PARAM = "app";

    @Inject
    public AppStreamDiscovery(AppStreamDiscoveryService appStreamDiscoveryService,
                              ObjectMapper objectMapper) {
        this.appStreamDiscoveryService = appStreamDiscoveryService;
        this.objectMapper = objectMapper;
    }

    @Override
    public HttpResponseMessage apply(HttpRequestMessage request) {

        List<String> apps = request.getQueryParams().get(APPNAME_QUERY_PARAM);
        Either<String, AppDiscoveryMap> result = appStreamDiscoveryService.getAppDiscoveryMap(apps);

        return result.bimap(errorMessage -> {
            HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, 500);
            resp.setBodyAsText(errorMessage);
            StatusCategoryUtils.setStatusCategory(request.getContext(), ZuulStatusCategory.FAILURE_LOCAL);
            return resp;
        }, appDiscoveryMap -> {

            Try<String> serialized = Try.of(() -> objectMapper.writeValueAsString(appDiscoveryMap));

            if (serialized.isSuccess()) {
                StatusCategoryUtils.setStatusCategory(request.getContext(), ZuulStatusCategory.SUCCESS);
                HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, 200);
                resp.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
                resp.setBodyAsText(serialized.get());
                return resp;
            } else {
                StatusCategoryUtils.setStatusCategory(request.getContext(), ZuulStatusCategory.FAILURE_LOCAL);
                HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, 500);
                resp.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString());
                resp.setBodyAsText(serialized.getOrElseGet(Throwable::getMessage));
                return resp;
            }
       }).getOrElseGet(Function.identity());
    }
}
