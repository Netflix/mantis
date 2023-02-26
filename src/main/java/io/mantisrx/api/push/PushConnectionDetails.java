package io.mantisrx.api.push;

import io.mantisrx.runtime.parameter.SinkParameter;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vavr.collection.List;
import io.vavr.control.Try;
import lombok.Value;

import java.util.stream.Collectors;

public @Value class PushConnectionDetails {

    public enum TARGET_TYPE {
        CONNECT_BY_NAME,
        CONNECT_BY_ID,
        JOB_STATUS,
        JOB_SCHEDULING_INFO,
        JOB_CLUSTER_DISCOVERY,
        METRICS
    }

    private final String uri;
    public final String target;
    public final TARGET_TYPE type;
    public final List<String> regions;


    /**
     * Determines the connection type for a given push connection.
     *
     * @param uri Request URI as returned by Netty's requestUri() methods. Expects leading slash.
     * @return The CONNECTION_TYPE requested by the URI.
     */
    public static TARGET_TYPE determineTargetType(final String uri) {
        if (uri.startsWith("/jobconnectbyname") || uri.startsWith("/api/v1/jobconnectbyname")) {
            return TARGET_TYPE.CONNECT_BY_NAME;
        } else if (uri.startsWith("/jobconnectbyid") || uri.startsWith("/api/v1/jobconnectbyid")) {
            return TARGET_TYPE.CONNECT_BY_ID;
        } else if (uri.startsWith("/jobstatus/") || uri.startsWith("/api/v1/jobstatus/")) {
            return TARGET_TYPE.JOB_STATUS;
        } else if (uri.startsWith("/api/v1/jobs/schedulingInfo/")) {
            return TARGET_TYPE.JOB_SCHEDULING_INFO;
        } else if (uri.startsWith("/jobClusters/discoveryInfoStream/")) {
            return TARGET_TYPE.JOB_CLUSTER_DISCOVERY;
        } else if (uri.startsWith("/api/v1/metrics/")) {
            return TARGET_TYPE.METRICS;
        } else {
            throw new IllegalArgumentException("Unable to determine push connection type from URI: " + uri);
        }
    }

    /**
     * Determines the target for a push connection request. Typically a job name or id.
     *
     * @param uri Request URI as returned by Netty's requestUri() methods. Expects leading slash.
     * @return The target requested by the URI.
     */
    public static String determineTarget(final String uri) {
        String sanitized = uri.replaceFirst("^/(api/v1/)?(jobconnectbyid|jobconnectbyname|jobstatus|jobs/schedulingInfo|jobClusters/discoveryInfoStream|metrics)/", "");
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(sanitized);
        return queryStringDecoder.path();
    }

    //
    // Computed Properties
    //

    public SinkParameters getSinkparameters() {
        SinkParameters.Builder builder = new SinkParameters.Builder();
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);

        builder.parameters(queryStringDecoder
                .parameters()
                .entrySet()
                .stream()
                .flatMap(entry -> entry.getValue()
                        .stream()
                        .map(val -> Try.of(() -> new SinkParameter(entry.getKey(), val)))
                        .filter(Try::isSuccess)
                        .map(Try::get))
                .collect(Collectors.toList())
                .toArray(new SinkParameter[]{}));

        return builder.build();
    }

    //
    // Static Factories
    //

    public static PushConnectionDetails from(String uri) {
        return from(uri, List.empty());
    }

    public static PushConnectionDetails from(String uri, List<String> regions) {
        return new PushConnectionDetails(uri, determineTarget(uri), determineTargetType(uri), regions);
    }

    public static PushConnectionDetails from(String uri, java.util.List<String> regions) {
        return new PushConnectionDetails(uri, determineTarget(uri), determineTargetType(uri), List.ofAll(regions));
    }
}
