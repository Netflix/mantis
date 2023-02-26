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

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.zuul.filters.http.HttpOutboundSyncFilter;
import com.netflix.zuul.message.http.HttpResponseMessage;
import io.mantisrx.api.Constants;
import io.mantisrx.api.services.JobDiscoveryService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobDiscoveryCacheLoader extends HttpOutboundSyncFilter {

    private static DynamicBooleanProperty cacheEnabled = new DynamicBooleanProperty("mantisapi.cache.enabled", false);

    @Override
    public boolean needsBodyBuffered(HttpResponseMessage message) {
        return true;
    }

    @Override
    public int filterOrder() {
        return 999; // Don't really care.
    }

    @Override
    public boolean shouldFilter(HttpResponseMessage response) {
        return response.getOutboundRequest().getPath().matches("^/api/v1/jobClusters/.*/latestJobDiscoveryInfo$")
                && response.getHeaders().getAll(Constants.MANTISAPI_CACHED_HEADER).isEmpty()
                && cacheEnabled.get();
    }

    @Override
    public HttpResponseMessage apply(HttpResponseMessage response) {
        String jobCluster = response.getOutboundRequest().getPath()
                .replaceFirst("^/api/v1/jobClusters/", "")
                .replaceFirst("/latestJobDiscoveryInfo$", "");

        String responseBody = response.getBodyAsText();

        if (null != responseBody) {
            log.info("Caching latest job discovery info for {}.", jobCluster);
            JobDiscoveryService.jobDiscoveryInfoCache.put(jobCluster, response.getBodyAsText());
        }
        return response;
    }
}
