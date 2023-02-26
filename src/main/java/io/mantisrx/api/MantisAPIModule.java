/*
 * Copyright 2018 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package io.mantisrx.api;

import com.netflix.discovery.guice.EurekaModule;
import com.netflix.zuul.*;
import com.netflix.zuul.filters.FilterRegistry;
import com.netflix.zuul.filters.MutableFilterRegistry;
import com.netflix.zuul.groovy.GroovyCompiler;
import com.netflix.zuul.groovy.GroovyFileFilter;
import io.mantisrx.server.core.Configurations;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.HighAvailabilityServicesUtil;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.AbstractDiscoveryClientOptionalArgs;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.netty.common.accesslog.AccessLogPublisher;
import com.netflix.netty.common.status.ServerStatusManager;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.ThreadPoolMonitor;
import com.netflix.zuul.context.SessionContextDecorator;
import com.netflix.zuul.context.ZuulSessionContextDecorator;
import com.netflix.zuul.init.ZuulFiltersModule;
import com.netflix.zuul.netty.server.BaseServerStartup;
import com.netflix.zuul.netty.server.ClientRequestReceiver;
import com.netflix.zuul.origins.BasicNettyOriginManager;
import com.netflix.zuul.origins.OriginManager;
import io.mantisrx.api.services.artifacts.ArtifactManager;
import io.mantisrx.api.services.artifacts.InMemoryArtifactManager;
import com.netflix.zuul.stats.BasicRequestMetricsPublisher;
import com.netflix.zuul.stats.RequestMetricsPublisher;
import io.mantisrx.api.tunnel.MantisCrossRegionalClient;
import io.mantisrx.api.tunnel.NoOpCrossRegionalClient;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import org.apache.commons.configuration.AbstractConfiguration;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class MantisAPIModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AbstractConfiguration.class).toInstance(ConfigurationManager.getConfigInstance());

        bind(BaseServerStartup.class).to(MantisServerStartup.class);

        // use provided basic netty origin manager
        bind(OriginManager.class).to(BasicNettyOriginManager.class);

        // zuul filter loading
        bind(DynamicCodeCompiler.class).to(GroovyCompiler.class);
        bind(FilenameFilter.class).to(GroovyFileFilter.class);
        install(new EurekaModule());
        install(new ZuulFiltersModule());
        bind(FilterLoader.class).to(DynamicFilterLoader.class);
        bind(FilterRegistry.class).to(MutableFilterRegistry.class);
        bind(FilterFileManager.class).asEagerSingleton();

        // general server bindings
        bind(ServerStatusManager.class); // health/discovery status
        bind(SessionContextDecorator.class).to(ZuulSessionContextDecorator.class); // decorate new sessions when requests come in
        bind(Registry.class).to(DefaultRegistry.class); // atlas metrics registry
        bind(RequestCompleteHandler.class).to(BasicRequestCompleteHandler.class); // metrics post-request completion
        bind(RequestMetricsPublisher.class).to(BasicRequestMetricsPublisher.class); // timings publisher

        // access logger, including request ID generator
        bind(AccessLogPublisher.class).toInstance(new AccessLogPublisher("ACCESS",
                (channel, httpRequest) -> ClientRequestReceiver.getRequestFromChannel(channel).getContext().getUUID()));

        bind(ArtifactManager.class).to(InMemoryArtifactManager.class);
        bind(MantisCrossRegionalClient.class).to(NoOpCrossRegionalClient.class);

        bind(ObjectMapper.class).toInstance(new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false));
    }

    @Provides
    @Singleton
    MasterClientWrapper provideMantisClientWrapper(AbstractConfiguration configuration) {
        Properties props = new Properties();
        configuration.getKeys("mantis").forEachRemaining(key -> {
            props.put(key, configuration.getString(key));
        });

        HighAvailabilityServices haServices = HighAvailabilityServicesUtil.createHAServices(
            Configurations.frmProperties(props, CoreConfiguration.class));
        return new MasterClientWrapper(haServices.getMasterClientApi());
    }

    @Provides @Singleton MantisClient provideMantisClient(AbstractConfiguration configuration) {
        Properties props = new Properties();
        configuration.getKeys("mantis").forEachRemaining(key -> {
            props.put(key, configuration.getString(key));
        });

        return new MantisClient(props);
    }

    @Provides
    @Singleton
    @Named("io-scheduler")
    Scheduler provideIoScheduler(Registry registry) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 128, 60,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        ThreadPoolMonitor.attach(registry, executor, "io-thread-pool");
        return Schedulers.from(executor);
    }

    @Provides @Singleton
    WorkerMetricsClient provideWorkerMetricsClient(AbstractConfiguration configuration) {
        Properties props = new Properties();
        configuration.getKeys("mantis").forEachRemaining(key -> {
            props.put(key, configuration.getString(key));
        });
        return new WorkerMetricsClient(props);
    }

    @Provides
    @Singleton
    @Named("push-prefixes")
    List<String> providePushPrefixes() {
        List<String> pushPrefixes = new ArrayList<>(20);
        pushPrefixes.add("/jobconnectbyid");
        pushPrefixes.add("/api/v1/jobconnectbyid");
        pushPrefixes.add("/jobconnectbyname");
        pushPrefixes.add("/api/v1/jobconnectbyname");
        pushPrefixes.add("/jobsubmitandconnect");
        pushPrefixes.add("/api/v1/jobsubmitandconnect");
        pushPrefixes.add("/jobClusters/discoveryInfoStream");
        pushPrefixes.add("/jobstatus");
        pushPrefixes.add("/api/v1/jobstatus");
        pushPrefixes.add("/api/v1/jobs/schedulingInfo/");
        pushPrefixes.add("/api/v1/metrics");

        return pushPrefixes;
    }

}
