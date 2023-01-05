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

package io.mantisrx.server.core.zookeeper;

import com.typesafe.config.Config;
import io.mantisrx.server.core.highavailability.HighAvailabilityServices;
import io.mantisrx.server.core.highavailability.HighAvailabilityServicesFactory;
import io.mantisrx.shaded.com.google.common.base.Preconditions;

public class HighAvailabilityServicesUtil {
    public static HighAvailabilityServices createHighAvailabilityServices(Config config) throws Exception {
        String mode = config.getString("mantis.highAvailability.mode");
        if (mode.equals("local")) {
            throw new UnsupportedOperationException();
        } else {
            final HighAvailabilityServicesFactory factory;
            if (mode.equals("class")) {
                String clazz = config.getString("mantis.highAvailability.class");
                factory = (HighAvailabilityServicesFactory) Class.forName(clazz).newInstance();
            } else {
                Preconditions.checkArgument(mode.equals("zookeeper"), "unknown mode");
                factory = new ZookeeperHighAvailabilityServicesFactory();
            }
            return factory.getHighAvailabilityServices(config);
        }
    }
}
