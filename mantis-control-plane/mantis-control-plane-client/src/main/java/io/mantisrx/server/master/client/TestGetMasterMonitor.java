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

package io.mantisrx.server.master.client;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.master.ZookeeperLeaderMonitorFactory;
import io.mantisrx.server.master.client.config.StaticPropertiesConfigurationFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import rx.functions.Action1;
import rx.functions.Func1;


public class TestGetMasterMonitor {

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";

    public static void main(String[] args) {
        try {
            Args.parse(TestGetMasterMonitor.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(TestGetMasterMonitor.class);
            System.exit(1);
        }
        Properties properties = new Properties();
        System.out.println("propfile=" + propFile);
        try (InputStream inputStream = new FileInputStream(propFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(5);
        StaticPropertiesConfigurationFactory configurationFactory = new StaticPropertiesConfigurationFactory(properties);
        CoreConfiguration config = configurationFactory.getConfig();
        MasterMonitor masterMonitor = new ZookeeperLeaderMonitorFactory().createLeaderMonitor(config);
        masterMonitor.getMasterObservable()
                .filter(new Func1<MasterDescription, Boolean>() {
                    @Override
                    public Boolean call(MasterDescription masterDescription) {
                        return masterDescription != null;
                    }
                })
                .doOnNext(new Action1<MasterDescription>() {
                    @Override
                    public void call(MasterDescription masterDescription) {
                        System.out.println(counter.incrementAndGet() + ": Got new master: " + masterDescription.toString());
                        latch.countDown();
                    }
                })
                .subscribe();
        masterMonitor.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
