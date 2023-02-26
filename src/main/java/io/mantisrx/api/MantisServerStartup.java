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

import com.google.inject.name.Named;
import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.config.DynamicIntProperty;
import com.netflix.discovery.EurekaClient;
import com.netflix.netty.common.accesslog.AccessLogPublisher;
import com.netflix.netty.common.channel.config.ChannelConfig;
import com.netflix.netty.common.channel.config.CommonChannelConfigKeys;
import com.netflix.netty.common.metrics.EventLoopGroupMetrics;
import com.netflix.netty.common.proxyprotocol.StripUntrustedProxyHeadersHandler;
import com.netflix.netty.common.status.ServerStatusManager;
import com.netflix.spectator.api.Registry;
import com.netflix.zuul.FilterLoader;
import com.netflix.zuul.FilterUsageNotifier;
import com.netflix.zuul.RequestCompleteHandler;
import com.netflix.zuul.context.SessionContextDecorator;
import com.netflix.zuul.netty.server.BaseServerStartup;
import com.netflix.zuul.netty.server.DirectMemoryMonitor;
import com.netflix.zuul.netty.server.NamedSocketAddress;

import io.mantisrx.api.initializers.MantisApiServerChannelInitializer;
import io.mantisrx.api.push.ConnectionBroker;
import io.mantisrx.api.tunnel.MantisCrossRegionalClient;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.HighAvailabilityServices;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import org.apache.commons.configuration.AbstractConfiguration;
import rx.Scheduler;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetSocketAddress;
import java.util.*;

@Singleton
public class MantisServerStartup extends BaseServerStartup {

    private final HighAvailabilityServices highAvailabilityServices;
    private final MantisCrossRegionalClient mantisCrossRegionalClient;
    private final ConnectionBroker connectionBroker;
    private final Scheduler scheduler;
    private final List<String> pushPrefixes;

    @Inject
    public MantisServerStartup(ServerStatusManager serverStatusManager, FilterLoader filterLoader,
                               SessionContextDecorator sessionCtxDecorator, FilterUsageNotifier usageNotifier,
                               RequestCompleteHandler reqCompleteHandler, Registry registry,
                               DirectMemoryMonitor directMemoryMonitor, EventLoopGroupMetrics eventLoopGroupMetrics,
                               EurekaClient discoveryClient, ApplicationInfoManager applicationInfoManager,
                               AccessLogPublisher accessLogPublisher,
                               AbstractConfiguration configurationManager,
                               HighAvailabilityServices highAvailabilityServices,
                               MantisCrossRegionalClient mantisCrossRegionalClient,
                               ConnectionBroker connectionBroker,
                               @Named("io-scheduler") Scheduler scheduler,
                               @Named("push-prefixes") List<String> pushPrefixes
                               ) {
        super(serverStatusManager, filterLoader, sessionCtxDecorator, usageNotifier, reqCompleteHandler, registry,
                directMemoryMonitor, eventLoopGroupMetrics, discoveryClient, applicationInfoManager,
                accessLogPublisher);
        this.highAvailabilityServices = highAvailabilityServices;
        this.mantisCrossRegionalClient = mantisCrossRegionalClient;
        this.connectionBroker = connectionBroker;
        this.scheduler = scheduler;
        this.pushPrefixes = pushPrefixes;

        // Mantis Master Listener
        highAvailabilityServices
                .getMasterMonitor()
                .getMasterObservable()
                .filter(x -> x != null)
                .forEach(masterDescription -> {
                    LOG.info("Received new Mantis Master: " + masterDescription);
                    configurationManager.setProperty("api.ribbon.listOfServers",
                            masterDescription.getHostIP() + ":" + masterDescription.getApiPort());
                });
    }

    @Override
    protected Map<NamedSocketAddress, ChannelInitializer<?>> chooseAddrsAndChannels(ChannelGroup clientChannels) {
        Map<NamedSocketAddress, ChannelInitializer<?>> addrsToChannels = new HashMap<>();

        String mainPortName = "main";
        int port = new DynamicIntProperty("zuul.server.port.main", 7001).get();
        NamedSocketAddress sockAddr = new NamedSocketAddress(mainPortName, new InetSocketAddress(port));

        ChannelConfig channelConfig = defaultChannelConfig(mainPortName);
        ChannelConfig channelDependencies = defaultChannelDependencies(mainPortName);

        /* These settings may need to be tweaked depending if you're running behind an ELB HTTP listener, TCP listener,
         * or directly on the internet.
         */
        channelConfig.set(CommonChannelConfigKeys.allowProxyHeadersWhen,
                StripUntrustedProxyHeadersHandler.AllowWhen.ALWAYS);
        channelConfig.set(CommonChannelConfigKeys.preferProxyProtocolForClientIp, false);
        channelConfig.set(CommonChannelConfigKeys.isSSlFromIntermediary, false);
        channelConfig.set(CommonChannelConfigKeys.withProxyProtocol, false);

        addrsToChannels.put(
                sockAddr,
                new MantisApiServerChannelInitializer(
                        String.valueOf(port), channelConfig, channelDependencies, clientChannels, pushPrefixes,
                    highAvailabilityServices, mantisCrossRegionalClient, connectionBroker,
                        scheduler, false));
        logAddrConfigured(sockAddr);

        return Collections.unmodifiableMap(addrsToChannels);
    }
}
