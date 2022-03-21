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

import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.core.master.MasterMonitor;
import io.mantisrx.server.core.zookeeper.CuratorService;
import io.mantisrx.shaded.com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

@Slf4j
public class HighAvailabilityServicesUtil {

  public static HighAvailabilityServices createHAServices(CoreConfiguration configuration) {
    if (configuration.isLocalMode()) {
      throw new UnsupportedOperationException();
    } else {
      return new ZkHighAvailabilityServices(configuration);
    }
  }

  private static class ZkHighAvailabilityServices extends AbstractIdleService implements
      HighAvailabilityServices {

    private final CuratorService curatorService;

    public ZkHighAvailabilityServices(CoreConfiguration configuration) {
      curatorService = new CuratorService(configuration, null);
    }

    @Override
    protected void startUp() throws Exception {
      curatorService.start();
    }

    @Override
    protected void shutDown() throws Exception {
      curatorService.shutdown();
    }

    @Override
    public MantisMasterGateway getMasterClientApi() {
      return new MantisMasterClientApi(curatorService.getMasterMonitor());
    }

    @Override
    public Observable<MasterDescription> getMasterDescription() {
      return curatorService.getMasterMonitor().getMasterObservable();
    }

    @Override
    public MasterMonitor getMasterMonitor() {
      return curatorService.getMasterMonitor();
    }
  }
}
