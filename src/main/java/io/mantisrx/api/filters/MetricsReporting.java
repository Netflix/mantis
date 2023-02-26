package io.mantisrx.api.filters;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Timer;
import com.netflix.zuul.filters.http.HttpOutboundSyncFilter;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.netty.SpectatorUtils;
import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MetricsReporting extends HttpOutboundSyncFilter {

    private static final ConcurrentHashMap<Tuple2<String, String>, Timer> timerCache = new ConcurrentHashMap<>(500);
    private static final ConcurrentHashMap<Tuple2<String, String>, Counter> counterCache = new ConcurrentHashMap<>(500);

    @Override
    public HttpResponseMessage apply(HttpResponseMessage input) {
        String path = input.getInboundRequest().getPath();
        String status = statusCodeToStringRepresentation(input.getStatus());

        // Record Latency. Zuul no longer record total request time.
        timerCache.computeIfAbsent(Tuple.of(path, status),
                tuple -> SpectatorUtils.newTimer("latency", path,"status", status))
                .record(input.getContext().getOriginReportedDuration(), TimeUnit.NANOSECONDS);

        // Record Request
        counterCache.computeIfAbsent(Tuple.of(path, status),
                tuple -> SpectatorUtils.newCounter("requests", path, "status", status))
                .increment();

        return input;
    }

    private String statusCodeToStringRepresentation(Integer statusCode) {
        return (statusCode / 100) + "xx";
    }

    @Override
    public int filterOrder() {
        return -100;
    }

    @Override
    public boolean shouldFilter(HttpResponseMessage msg) {
        return true;
    }
}
