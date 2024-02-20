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

package io.mantisrx.server.worker.config;

import static org.junit.Assert.assertEquals;

import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Properties;
import org.junit.Test;

public class WorkerConfigurationTest {

    @Test
    public void testTaskExecutorAttributesWhenEmptyStringIsPassed() {
        final Properties props = new Properties();
        props.setProperty("mantis.zookeeper.root", "");
        props.setProperty("mantis.taskexecutor.attributes", "");
        final WorkerConfiguration workerConfiguration = new StaticPropertiesConfigurationFactory(props).getConfig();
        assertEquals(ImmutableMap.of(), workerConfiguration.getTaskExecutorAttributes());
    }

    @Test
    public void testTaskExecutorAttributesWhenASingleValueKVPairIsPassed() {
        final Properties props = new Properties();
        props.setProperty("mantis.taskexecutor.attributes", "key1:val1");
        props.setProperty("mantis.zookeeper.root", "");
        final WorkerConfiguration workerConfiguration = new StaticPropertiesConfigurationFactory(props).getConfig();
        assertEquals(ImmutableMap.of("key1", "val1"), workerConfiguration.getTaskExecutorAttributes());
    }

    @Test
    public void testMoreThanOneKeyValuePair() {
        final Properties props = new Properties();
        props.setProperty("mantis.taskexecutor.attributes", "key1:val1,key2:val2,key3:val3");
        props.setProperty("mantis.zookeeper.root", "");
        final WorkerConfiguration workerConfiguration = new StaticPropertiesConfigurationFactory(props).getConfig();
        assertEquals(ImmutableMap.of("key1", "val1", "key2", "val2", "key3", "val3"), workerConfiguration.getTaskExecutorAttributes());
    }

    @Test
    public void testExcludeUnresolvedAttribute() {
        final Properties props = new Properties();
        props.setProperty("mantis.taskexecutor.attributes", "key1:val1,key2:${val2},key3:val3");
        props.setProperty("mantis.zookeeper.root", "");
        final WorkerConfiguration workerConfiguration = new StaticPropertiesConfigurationFactory(props).getConfig();
        assertEquals(ImmutableMap.of("key1", "val1", "key3", "val3"), workerConfiguration.getTaskExecutorAttributes());
    }
}
