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

import com.google.common.base.Strings;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.zuul.filters.http.HttpInboundSyncFilter;
import com.netflix.zuul.message.http.HttpHeaderNames;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import io.mantisrx.api.Constants;
import io.mantisrx.api.services.JobDiscoveryService;
import io.netty.handler.codec.http.HttpHeaderValues;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobDiscoveryInfoCacheHitChecker extends HttpInboundSyncFilter {

   public static final String PATH_SPEC = "/jobClusters/discoveryInfo";
   private static DynamicBooleanProperty cacheEnabled = new DynamicBooleanProperty("mantisapi.cache.enabled", false);

   @Override
   public int filterOrder() {
      return -1;
   }

   @Override
   public boolean shouldFilter(HttpRequestMessage httpRequestMessage) {
      String jobCluster = httpRequestMessage.getPath().replaceFirst(PATH_SPEC + "/", "");
      return httpRequestMessage.getPath().startsWith(PATH_SPEC)
              && JobDiscoveryService.jobDiscoveryInfoCache.getIfPresent(jobCluster) != null;
   }

   @Override
   public HttpRequestMessage apply(HttpRequestMessage request) {
      String jobCluster = request.getPath().replaceFirst(PATH_SPEC + "/", "");
      HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, 200);
      String bodyText = JobDiscoveryService.jobDiscoveryInfoCache.getIfPresent(jobCluster) ;
      if (cacheEnabled.get() && !Strings.isNullOrEmpty(bodyText)) {
         log.info("Serving cached job discovery info for {}.", jobCluster);
         resp.setBodyAsText(bodyText);
         resp.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
         resp.getHeaders().set(Constants.MANTISAPI_CACHED_HEADER, "true");
         request.getContext().setStaticResponse(resp);
      }
      return request;
   }
}
