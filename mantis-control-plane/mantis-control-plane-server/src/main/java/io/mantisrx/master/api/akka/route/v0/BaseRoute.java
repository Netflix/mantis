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

package io.mantisrx.master.api.akka.route.v0;

import akka.actor.ActorSystem;
import akka.http.caching.LfuCache;
import akka.http.caching.javadsl.Cache;
import akka.http.caching.javadsl.CachingSettings;
import akka.http.caching.javadsl.LfuCacheSettings;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.japi.pf.PFBuilder;
import akka.pattern.AskTimeoutException;
import io.mantisrx.master.api.akka.route.MasterApiMetrics;
import io.mantisrx.master.jobcluster.proto.BaseResponse;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.duration.Duration;

@Slf4j
abstract class BaseRoute extends AllDirectives {
    protected HttpResponse toHttpResponse(final BaseResponse r) {
        switch (r.responseCode) {
            case SUCCESS:
            case SUCCESS_CREATED:
                MasterApiMetrics.getInstance().incrementResp2xx();
                return HttpResponse.create()
                    .withEntity(ContentTypes.APPLICATION_JSON, r.message)
                    .withStatus(StatusCodes.OK);
            case CLIENT_ERROR:
            case CLIENT_ERROR_NOT_FOUND:
            case CLIENT_ERROR_CONFLICT:
                MasterApiMetrics.getInstance().incrementResp4xx();
                return HttpResponse.create()
                    .withEntity(ContentTypes.APPLICATION_JSON, "{\"error\": \"" + r.message + "\"}")
                    .withStatus(StatusCodes.BAD_REQUEST);
            case OPERATION_NOT_ALLOWED:
                MasterApiMetrics.getInstance().incrementResp4xx();
                return HttpResponse.create()
                    .withEntity(ContentTypes.APPLICATION_JSON, "{\"error\": \"" + r.message + "\"}")
                    .withStatus(StatusCodes.METHOD_NOT_ALLOWED);
            case SERVER_ERROR:
            default:
                MasterApiMetrics.getInstance().incrementResp5xx();
                log.error("Non-matched response code error: {}", r.message);
                return HttpResponse.create()
                    .withEntity(ContentTypes.APPLICATION_JSON, "{\"error\": \"" + r.message + "\"}")
                    .withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
        }
    }

    protected <T extends BaseResponse> RouteAdapter completeAsync(final CompletionStage<T> stage,
                                                                final Function<T, RouteAdapter> successTransform) {
        return completeAsync(stage,
            successTransform,
            r -> complete(StatusCodes.BAD_REQUEST, "{\"error\": \"" + r.message + "\"}"));
    }

    protected <T extends BaseResponse> RouteAdapter completeAsync(final CompletionStage<T> stage,
                                                                  final Function<T, RouteAdapter> successTransform,
                                                                  final Function<T, RouteAdapter> clientFailureTransform) {
        return onComplete(
            stage,
            resp -> resp
                .map(r -> {
                    switch (r.responseCode) {
                        case SUCCESS:
                        case SUCCESS_CREATED:
                            MasterApiMetrics.getInstance().incrementResp2xx();
                            return successTransform.apply(r);
                        case CLIENT_ERROR:
                        case CLIENT_ERROR_NOT_FOUND:
                        case CLIENT_ERROR_CONFLICT:
                            return clientFailureTransform.apply(r);
                        case SERVER_ERROR:
                        case OPERATION_NOT_ALLOWED:
                        default:
                            MasterApiMetrics.getInstance().incrementResp5xx();
                            return complete(StatusCodes.INTERNAL_SERVER_ERROR, r.message);
                    }
                })
                .recover(new PFBuilder<Throwable, Route>()
                    .match(AskTimeoutException.class, te -> {
                        MasterApiMetrics.getInstance().incrementAskTimeOutCount();
                        MasterApiMetrics.getInstance().incrementResp5xx();
                        return complete(StatusCodes.INTERNAL_SERVER_ERROR,
                            "{\"error\": \"" + te.getMessage() + "\"}");
                    })
                    .matchAny(ex -> {
                        MasterApiMetrics.getInstance().incrementResp5xx();
                        log.error("Internal server error from completeAsync: ", ex);
                        return complete(StatusCodes.INTERNAL_SERVER_ERROR,
                            "{\"error\": \"" + ex.getMessage() + "\"}");
                    })
                    .build()).get());
    }

    protected Cache<Uri, RouteResult> createCache(ActorSystem actorSystem, int initialCapacity, int maxCapacity, int ttlMillis) {
        final CachingSettings defaultCachingSettings = CachingSettings.create(actorSystem);
        final LfuCacheSettings lfuCacheSettings = defaultCachingSettings.lfuCacheSettings()
                .withInitialCapacity(initialCapacity)
                .withMaxCapacity(maxCapacity)
                .withTimeToLive(Duration.create(ttlMillis, TimeUnit.MILLISECONDS));
        final CachingSettings cachingSettings = defaultCachingSettings.withLfuCacheSettings(lfuCacheSettings);
        return LfuCache.create(cachingSettings);
    }
}
