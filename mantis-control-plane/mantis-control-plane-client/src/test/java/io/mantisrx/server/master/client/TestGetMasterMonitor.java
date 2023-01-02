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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mantisrx.server.core.master.MasterMonitor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestGetMasterMonitor {

    @Argument(alias = "p", description = "Specify a configuration file", required = true)
    private static String propFile = "";

    public static void main(String[] args) throws IOException {
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
        final Config config = ConfigFactory.parseProperties(properties);
        MasterMonitor masterMonitor;
        try (ClientServices clientServices = ClientServicesUtil.createClientServices(config)) {
            masterMonitor = clientServices.getMasterMonitor();
            masterMonitor.getMasterObservable()
                .filter(masterDescription -> masterDescription != null)
                .doOnNext(masterDescription -> {
                    log.info(counter.incrementAndGet() + ": Got new master: " + masterDescription.toString());
                    latch.countDown();
                })
                .subscribe();

            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("Failed to finish", e);
            }
        }
    }
}
