package io.mantisrx.api.filters;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.filters.http.HttpInboundSyncFilter;
import com.netflix.zuul.message.http.HttpHeaderNames;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.netty.handler.codec.http.HttpHeaderValues;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MasterCacheHitChecker extends HttpInboundSyncFilter {

    private static DynamicBooleanProperty cacheEnabled = new DynamicBooleanProperty("mantisapi.cache.enabled", false);
    private static final ConcurrentHashMap<String, Counter> cacheHitCounters = new ConcurrentHashMap<>(500);
    private static final ConcurrentHashMap<String, Counter> cacheMissCounters = new ConcurrentHashMap<>(500);
    private static final String CACHE_HIT_COUNTER_NAME = "mantis.api.cache.count";
    private final List<String> pushPrefixes;

    @Inject
    public MasterCacheHitChecker(@Named("push-prefixes") List<String> pushPrefixes) {
        super();
        this.pushPrefixes = pushPrefixes;
    }

    @Override
    public HttpRequestMessage apply(HttpRequestMessage request) {
        if(cacheEnabled.get()) {
            String key = request.getPathAndQuery();
            String bodyText = MasterCacheLoader.masterCache.getIfPresent(key);

            if (bodyText != null) { // Cache Hit
                HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request, 200);
                response.setBodyAsText(bodyText);
                response.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
                response.getHeaders().set(Constants.MANTISAPI_CACHED_HEADER, "true");
                request.getContext().setStaticResponse(response);

                cacheHitCounters.computeIfAbsent(key,
                        k -> SpectatorUtils.newCounter(CACHE_HIT_COUNTER_NAME, "api", "endpoint", k, "class", "hit"))
                        .increment();
            } else { // Cache Miss
                cacheMissCounters.computeIfAbsent(key,
                        k -> SpectatorUtils.newCounter(CACHE_HIT_COUNTER_NAME, "api", "endpoint", k, "class", "miss"))
                        .increment();
            }
        }

        return request;
    }

    @Override
    public int filterOrder() {
        return 0;
    }

    @Override
    public boolean shouldFilter(HttpRequestMessage msg) {
        String key = msg.getPathAndQuery();

        return msg.getMethod().equalsIgnoreCase("get")
                && key.startsWith("/api")
                && !Util.startsWithAnyOf(key, pushPrefixes);
    }
}
