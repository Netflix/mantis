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

import com.netflix.zuul.context.SessionContext;
import com.netflix.zuul.filters.http.HttpInboundSyncFilter;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.netty.filter.ZuulEndPointRunner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Routes extends HttpInboundSyncFilter {

    @Override
    public int filterOrder() {
        return 0;
    }

    @Override
    public boolean shouldFilter(HttpRequestMessage httpRequestMessage) {
        return true;
    }

    @Override
    public HttpRequestMessage apply(HttpRequestMessage request) {
        SessionContext context = request.getContext();
        String path = request.getPath();
        String host = request.getOriginalHost();

        if (request.getMethod().toLowerCase().equals("options")) {
            context.setEndpoint(Options.class.getCanonicalName());
        } else if (path.equalsIgnoreCase("/healthcheck")) {
            context.setEndpoint(Healthcheck.class.getCanonicalName());
        } else if (path.equalsIgnoreCase("/favicon.ico")) {
            context.setEndpoint(Favicon.class.getCanonicalName());
        } else if (path.startsWith(Artifacts.PATH_SPEC)) {
            context.setEndpoint(Artifacts.class.getCanonicalName());
        } else if (path.equalsIgnoreCase("/api/v1/mantis/publish/streamDiscovery")) {
            context.setEndpoint(AppStreamDiscovery.class.getCanonicalName());
        } else if (path.startsWith("/jobClusters/discoveryInfo")) {
            String jobCluster = request.getPath().replaceFirst(JobDiscoveryInfoCacheHitChecker.PATH_SPEC + "/", "");
            String newUrl = "/api/v1/jobClusters/" + jobCluster + "/latestJobDiscoveryInfo";
            request.setPath(newUrl);
            context.setEndpoint(ZuulEndPointRunner.PROXY_ENDPOINT_FILTER_NAME);
            context.setRouteVIP("api");
        } else if (path.equalsIgnoreCase("/api/v1/mql/parse")) {
            context.setEndpoint(MQLParser.class.getCanonicalName());
        } else if (path.equals(MREAppStreamToJobClusterMapping.PATH_SPEC)) {
            context.setEndpoint(MREAppStreamToJobClusterMapping.class.getCanonicalName());
        } else {
            context.setEndpoint(ZuulEndPointRunner.PROXY_ENDPOINT_FILTER_NAME);
            context.setRouteVIP("api");
        }

        return request;
    }
}
