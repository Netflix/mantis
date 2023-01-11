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

package io.mantisrx.server.master.config;

import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.server.core.CoreConfiguration;
import io.mantisrx.server.master.store.KeyValueStore;
import org.skife.config.Config;
import org.skife.config.Default;


public interface MasterConfiguration extends CoreConfiguration {

    @Config("mantis.master.storageProvider")
    KeyValueStore getStorageProvider();

    @Config("mantis.master.resourceClusterStorageProvider")
    ResourceClusterStorageProvider getResourceClusterStorageProvider();

    @Config("mantis.master.resourceClusterProvider")
    String getResourceClusterProvider();


    // ------------------------------------------------------------------------
    //  Apache Mesos related configurations
    // ------------------------------------------------------------------------


    @Config("mantis.interval.move.workers.disabled.vms.millis")
    @Default("60000")
    long getIntervalMoveWorkersOnDisabledVMsMillis();


}
