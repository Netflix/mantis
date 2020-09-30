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

package com.netflix.mantis.examples.mantispublishsample.web.config;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.mantis.examples.mantispublishsample.web.filter.CaptureRequestEventFilter;
import com.netflix.mantis.examples.mantispublishsample.web.service.MyService;
import com.netflix.mantis.examples.mantispublishsample.web.service.MyServiceImpl;
import com.netflix.mantis.examples.mantispublishsample.web.servlet.HelloServlet;
import com.netflix.spectator.nflx.SpectatorModule;
import io.mantisrx.publish.netty.guice.MantisRealtimeEventsPublishModule;


/**
 * Wire up the servlets, filters and other modules.
 */
public class DefaultGuiceServletConfig extends GuiceServletContextListener {
    @Override
    protected Injector getInjector() {
        return Guice.createInjector(
                new ArchaiusModule(), new MantisRealtimeEventsPublishModule(), new SpectatorModule(),
                new ServletModule() {
                    @Override
                    protected void configureServlets() {
                        filter("/*").through(CaptureRequestEventFilter.class);
                        serve("/hello").with(HelloServlet.class);
                        bind(MyService.class).to(MyServiceImpl.class);
                    }
                }
                );
    }
}