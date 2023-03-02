package io.mantisrx.api.push;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.mantisrx.api.services.JobDiscoveryService;
import io.mantisrx.api.tunnel.MantisCrossRegionalClient;
import io.mantisrx.client.MantisClient;
import io.mantisrx.client.SinkConnectionFunc;
import io.mantisrx.client.SseSinkConnectionFunction;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.worker.client.MetricsClient;
import io.mantisrx.server.worker.client.SseWorkerConnectionFunction;
import io.mantisrx.server.worker.client.WorkerConnectionsStatus;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mantisrx.api.Constants.TunnelPingMessage;
import static io.mantisrx.api.Util.getLocalRegion;

@Slf4j
@Singleton
public class ConnectionBroker {

    private final MantisClient mantisClient;
    private final MantisCrossRegionalClient mantisCrossRegionalClient;
    private final WorkerMetricsClient workerMetricsClient;
    private final JobDiscoveryService jobDiscoveryService;
    private final Scheduler scheduler;
    private final ObjectMapper objectMapper;

    private final Map<PushConnectionDetails, Observable<String>> connectionCache = new WeakHashMap<>();

    @Inject
    public ConnectionBroker(MantisClient mantisClient,
                            MantisCrossRegionalClient mantisCrossRegionalClient,
                            WorkerMetricsClient workerMetricsClient,
                            @Named("io-scheduler") Scheduler scheduler,
                            ObjectMapper objectMapper) {
        this.mantisClient = mantisClient;
        this.mantisCrossRegionalClient = mantisCrossRegionalClient;
        this.workerMetricsClient = workerMetricsClient;
        this.jobDiscoveryService = JobDiscoveryService.getInstance(mantisClient, scheduler);
        this.scheduler = scheduler;
        this.objectMapper = objectMapper;
    }

    public Observable<String> connect(PushConnectionDetails details) {

        if (!connectionCache.containsKey(details)) {
            switch (details.type) {
                case CONNECT_BY_NAME:
                    return getConnectByNameFor(details)
                                    .subscribeOn(scheduler)
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .share();
                case CONNECT_BY_ID:
                    return getConnectByIdFor(details)
                            .subscribeOn(scheduler)
                            .doOnUnsubscribe(() -> {
                                log.info("Purging {} from cache.", details);
                                connectionCache.remove(details);
                            })
                            .doOnCompleted(() -> {
                                log.info("Purging {} from cache.", details);
                                connectionCache.remove(details);
                            })
                            .share();

                case METRICS:
                    return getWorkerMetrics(details)
                            .subscribeOn(scheduler)
                            .doOnUnsubscribe(() -> {
                                log.info("Purging {} from cache.", details);
                                connectionCache.remove(details);
                            })
                            .doOnCompleted(() -> {
                                log.info("Purging {} from cache.", details);
                                connectionCache.remove(details);
                            });

                case JOB_STATUS:
                    connectionCache.put(details,
                            mantisClient
                                    .getJobStatusObservable(details.target)
                                    .subscribeOn(scheduler)
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .replay(25)
                                    .autoConnect());
                    break;
                case JOB_SCHEDULING_INFO:
                    connectionCache.put(details,
                            mantisClient.getSchedulingChanges(details.target)
                                    .subscribeOn(scheduler)
                                    .map(changes -> Try.of(() -> objectMapper.writeValueAsString(changes)).getOrElse("Error"))
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                            .replay(1)
                            .autoConnect());
                    break;

                case JOB_CLUSTER_DISCOVERY:
                    connectionCache.put(details,
                            jobDiscoveryService.jobDiscoveryInfoStream(jobDiscoveryService.key(JobDiscoveryService.LookupType.JOB_CLUSTER, details.target))
                                    .subscribeOn(scheduler)
                                    .map(jdi ->Try.of(() -> objectMapper.writeValueAsString(jdi)).getOrElse("Error"))
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .replay(1)
                                    .autoConnect());
                    break;
            }
            log.info("Caching connection for: {}", details);
        }
        return connectionCache.get(details);
    }

    //
    // Helpers
    //

    private Observable<String> getConnectByNameFor(PushConnectionDetails details) {
        return details.regions.isEmpty()
                ? getResults(false, this.mantisClient, details.target, details.getSinkparameters())
                .flatMap(m -> m)
                .map(MantisServerSentEvent::getEventAsString)
                : getRemoteDataObservable(details.getUri(), details.target, details.getRegions().asJava());
    }

    private Observable<String> getConnectByIdFor(PushConnectionDetails details) {
        return details.getRegions().isEmpty()
                ? getResults(true, this.mantisClient, details.target, details.getSinkparameters())
                .flatMap(m -> m)
                .map(MantisServerSentEvent::getEventAsString)
                : getRemoteDataObservable(details.getUri(), details.target, details.getRegions().asJava());
    }


    private static SinkConnectionFunc<MantisServerSentEvent> getSseConnFunc(final String target, SinkParameters sinkParameters) {
        return new SseSinkConnectionFunction(true,
                t -> log.warn("Reconnecting to sink of job " + target + " after error: " + t.getMessage()),
                sinkParameters);
    }

    private static Observable<Observable<MantisServerSentEvent>> getResults(boolean isJobId, MantisClient mantisClient,
                                                                            final String target, SinkParameters sinkParameters) {
        final AtomicBoolean hasError = new AtomicBoolean();
        return  isJobId ?
                mantisClient.getSinkClientByJobId(target, getSseConnFunc(target, sinkParameters), null).getResults() :
                mantisClient.getSinkClientByJobName(target, getSseConnFunc(target, sinkParameters), null)
                        .switchMap(serverSentEventSinkClient -> {
                            if (serverSentEventSinkClient.hasError()) {
                                hasError.set(true);
                                return Observable.error(new Exception(serverSentEventSinkClient.getError()));
                            }
                            return serverSentEventSinkClient.getResults();
                        })
                        .takeWhile(o -> !hasError.get());
    }

    //
    // Tunnel
    //

    private Observable<String> getRemoteDataObservable(String uri, String target, List<String> regions) {
        return Observable.from(regions)
                .flatMap(region -> {
                    final String originReplacement = "\\{\"" + Constants.metaOriginName + "\": \"" + region + "\", ";
                    if (region.equalsIgnoreCase(getLocalRegion())) {
                        return this.connect(PushConnectionDetails.from(uri))
                                .map(datum -> datum.replaceFirst("^\\{", originReplacement));
                    } else {
                        log.info("Connecting to remote region {} at {}.", region, uri);
                        return mantisCrossRegionalClient.getSecureSseClient(region)
                                .submit(HttpClientRequest.createGet(uri))
                                .retryWhen(Util.getRetryFunc(log, uri + " in " + region))
                                .doOnError(throwable -> log.warn(
                                        "Error getting response from remote SSE server for uri {} in region {}: {}",
                                        uri, region, throwable.getMessage(), throwable)
                                ).flatMap(remoteResponse -> {
                                    if (!remoteResponse.getStatus().reasonPhrase().equals("OK")) {
                                        log.warn("Unexpected response from remote sink for uri {} region {}: {}", uri, region, remoteResponse.getStatus().reasonPhrase());
                                        String err = remoteResponse.getHeaders().get(Constants.metaErrorMsgHeader);
                                        if (err == null || err.isEmpty())
                                            err = remoteResponse.getStatus().reasonPhrase();
                                        return Observable.<MantisServerSentEvent>error(new Exception(err))
                                                .map(datum -> datum.getEventAsString());
                                    }
                                    return clientResponseToObservable(remoteResponse, target, region, uri)
                                            .map(datum -> datum.replaceFirst("^\\{", originReplacement))
                                            .doOnError(t -> log.error(t.getMessage()));
                                })
                                .subscribeOn(scheduler)
                                .observeOn(scheduler)
                                .doOnError(t -> log.warn("Error streaming in remote data ({}). Will retry: {}", region, t.getMessage(), t))
                                .doOnCompleted(() -> log.info(String.format("remote sink connection complete for uri %s, region=%s", uri, region)));
                    }
                })
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .doOnError(t -> log.error("Error in flatMapped cross-regional observable for {}", uri, t));
    }

    private Observable<String> clientResponseToObservable(HttpClientResponse<ServerSentEvent> response, String target, String
            region, String uri) {

        Counter numRemoteBytes = SpectatorUtils.newCounter(Constants.numRemoteBytesCounterName, target, "region", region);
        Counter numRemoteMessages = SpectatorUtils.newCounter(Constants.numRemoteMessagesCounterName, target, "region", region);
        Counter numSseErrors = SpectatorUtils.newCounter(Constants.numSseErrorsCounterName, target, "region", region);

        return response.getContent()
                .doOnError(t -> log.warn(t.getMessage()))
                .timeout(3 * Constants.TunnelPingIntervalSecs, TimeUnit.SECONDS)
                .doOnError(t -> log.warn("Timeout getting data from remote {} connection for {}", region, uri))
                .filter(sse -> !(!sse.hasEventType() || !sse.getEventTypeAsString().startsWith("error:")) ||
                        !TunnelPingMessage.equals(sse.contentAsString()))
                .map(t1 -> {
                    String data = "";
                    if (t1.hasEventType() && t1.getEventTypeAsString().startsWith("error:")) {
                        log.error("SSE has error, type=" + t1.getEventTypeAsString() + ", content=" + t1.contentAsString());
                        numSseErrors.increment();
                        throw new RuntimeException("Got error SSE event: " + t1.contentAsString());
                    }
                    try {
                        data = t1.contentAsString();
                        if (data != null) {
                            numRemoteBytes.increment(data.length());
                            numRemoteMessages.increment();
                        }
                    } catch (Exception e) {
                        log.error("Could not extract data from SSE " + e.getMessage(), e);
                    }
                    return data;
                });
    }

    private Observable<String> getWorkerMetrics(PushConnectionDetails details) {

        final String jobId = details.target;

        SinkParameters metricNamesFilter = details.getSinkparameters();

        final MetricsClient<MantisServerSentEvent> metricsClient = workerMetricsClient.getMetricsClientByJobId(jobId,
                new SseWorkerConnectionFunction(true, new Action1<Throwable>() {
                    @Override
                    public void call(Throwable throwable) {
                        log.error("Metric connection error: " + throwable.getMessage());
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException ie) {
                            log.error("Interrupted waiting for retrying connection");
                        }
                    }
                }, metricNamesFilter),
                new Observer<WorkerConnectionsStatus>() {
                    @Override
                    public void onCompleted() {
                        log.info("got onCompleted in WorkerConnStatus obs");
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.info("got onError in WorkerConnStatus obs");
                    }

                    @Override
                    public void onNext(WorkerConnectionsStatus workerConnectionsStatus) {
                        log.info("got WorkerConnStatus {}", workerConnectionsStatus);
                    }
                });

        return metricsClient
                .getResults()
                .flatMap(metrics -> metrics
                        .map(MantisServerSentEvent::getEventAsString));

    }
}
