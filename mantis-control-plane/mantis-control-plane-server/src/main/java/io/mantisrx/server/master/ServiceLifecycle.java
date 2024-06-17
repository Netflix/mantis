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

package io.mantisrx.server.master;

import io.mantisrx.server.core.BaseService;
import java.util.Iterator;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manage lifecycle of services. Services added in an order are started in the same order and shutdown in the reverse
 * order. Each service being added is implicitly given the previously added service as its predecessor. This class
 * currently represents a model of a linear list of services, each depending on only the previous service in the list.
 */
public class ServiceLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ServiceLifecycle.class);
    private LinkedList<BaseService> servicesList = new LinkedList<BaseService>();

    public void addService(BaseService service) {
        if (!servicesList.isEmpty()) {
            service.addPredecessor(servicesList.getLast());
        }
        servicesList.add(service);
    }

    public void start() {
        for (BaseService service : servicesList) {
            try {
                logger.info("Starting service " + service.getMyServiceCount() + ": " + service);
                service.start();
                logger.info("Successfully started service " + service.getMyServiceCount() + ": " + service);
            } catch (Exception e) {
                logger.error(String.format("Failed to start service %d: %s: %s", service.getMyServiceCount(), service, e.getMessage()), e);
                throw e;
            }
        }
    }

    public void becomeLeader() {
        for (BaseService service : servicesList) {
            service.enterActiveMode();
        }
    }

    public void shutdown() {
        if (!servicesList.isEmpty()) {
            Iterator<BaseService> iterator = servicesList.descendingIterator();
            while (iterator.hasNext()) {
                BaseService service = iterator.next();
                logger.info("Shutting down service " + service.getMyServiceCount() + ": " + service);
                service.shutdown();
                logger.info("Successfully shut down service " + service.getMyServiceCount() + ": " + service);
            }
        }
    }
}