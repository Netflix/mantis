/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.server.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.mantisrx.common.JsonSerializer;
import io.mantisrx.runtime.loader.config.WorkerConfiguration;
import io.mantisrx.runtime.loader.config.WorkerConfigurationUtils;
import io.mantisrx.runtime.loader.config.WorkerConfigurationWritable;
import io.mantisrx.server.worker.config.StaticPropertiesConfigurationFactory;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

public class WorkerConfigurationWritableTest {
    static String ConfigWritableToString(WorkerConfigurationWritable configurationWritable) throws IOException {
        JsonSerializer ser = new JsonSerializer();
        return ser.toJson(configurationWritable);
    }

    @Test
    public void testWorkerConfigurationConversion() throws IOException {
        final Properties props = new Properties();
        props.setProperty("mantis.zookeeper.root", "");

        props.setProperty("mantis.taskexecutor.cluster.storage-dir", "");
        props.setProperty("mantis.taskexecutor.local.storage-dir", "");
        props.setProperty("mantis.taskexecutor.cluster-id", "default");
        props.setProperty("mantis.taskexecutor.heartbeats.interval", "100");
        props.setProperty("mantis.taskexecutor.metrics.collector", "io.mantisrx.server.agent.DummyMetricsCollector");

        props.setProperty("mantis.taskexecutor.id", "testId1");
        props.setProperty("mantis.taskexecutor.heartbeats.interval", "999");

        WorkerConfiguration configSource = new StaticPropertiesConfigurationFactory(props).getConfig();

        WorkerConfigurationWritable configurationWritable = WorkerConfigurationUtils.toWritable(configSource);
        assertNotNull(configurationWritable);
        assertEquals("testId1", configurationWritable.getTaskExecutorId());
        assertEquals(999, configurationWritable.getHeartbeatInternalInMs());
        assertNotNull(configurationWritable.getUsageSupplier());

        String configStr = ConfigWritableToString(configurationWritable);
        WorkerConfigurationWritable configurationWritable2 =
            WorkerConfigurationUtils.stringToWorkerConfiguration(configStr);
        assertNotNull(configurationWritable);
        assertEquals("testId1", configurationWritable2.getTaskExecutorId());
        assertEquals(999, configurationWritable2.getHeartbeatInternalInMs());
        assertNull(configurationWritable2.getUsageSupplier());
    }
}
