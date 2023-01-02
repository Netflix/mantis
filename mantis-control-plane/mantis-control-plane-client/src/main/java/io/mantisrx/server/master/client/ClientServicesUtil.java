/*
 * Copyright 2022 Netflix, Inc.
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

import com.typesafe.config.Config;
import io.mantisrx.server.core.highavailability.HighAvailabilityServices;
import io.mantisrx.server.core.highavailability.LeaderRetrievalService;
import io.mantisrx.server.core.zookeeper.HighAvailabilityServicesUtil;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * HighAvailabilityServicesUtil helps you create HighAvailabilityServices instance based on the core configuration.
 */
@Slf4j
public class ClientServicesUtil {
    private final static AtomicReference<ClientServices> ref = new AtomicReference<>();

    public static ClientServices createClientServices(Config config) {
        if (ref.get() == null) {
            return ref.updateAndGet(val -> {
                if (val == null) {
                    try {
                        HighAvailabilityServices highAvailabilityServices =
                            HighAvailabilityServicesUtil.createHighAvailabilityServices(config);
                        highAvailabilityServices.startAsync().awaitRunning();

                        LeaderRetrievalService retrievalService =
                            highAvailabilityServices.getLeaderRetrievalService();
                        retrievalService.startAsync().awaitRunning();
                        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                            retrievalService.stopAsync().awaitTerminated();
                            highAvailabilityServices.stopAsync().awaitTerminated();
                        }));

                        return new ClientServicesImpl(retrievalService);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    return val;
                }
            });
        } else {
            return ref.get();
        }
//    String mode = config.getString("mantis.highAvailability.mode");
//    if (mode.equals("local")) {
//      throw new UnsupportedOperationException();
//    } else if (mode.equalsIgnoreCase("zookeeper")) {
//      if (HAServiceInstanceRef.get() == null) {
//          HAServiceInstanceRef.compareAndSet(null, new ZookeeperHigh(config.getConfig("mantis.highAvailability.zookeeper")));
//      }
//
//      return HAServiceInstanceRef.get();
//    } else {
//        throw new UnsupportedOperationException();
//    }
    }

}
