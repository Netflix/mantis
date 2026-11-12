/*
 * Copyright 2026 Netflix, Inc.
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

package io.mantisrx.runtime.sink;

import static io.mantisrx.common.SystemParameters.SSE_CONNECTION_BUFFER_INTERVAL_MS;
import static io.mantisrx.common.SystemParameters.SSE_CONNECTION_BUFFER_MODE;
import static org.junit.Assert.assertEquals;

import io.mantisrx.common.properties.DefaultMantisPropertiesLoader;
import io.mantisrx.runtime.parameter.Parameter;
import io.mantisrx.runtime.parameter.ParameterUtils;
import io.mantisrx.runtime.parameter.Parameters;
import io.reactivex.mantis.network.push.ServerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class ConnectionBufferConfigurationTest {
    @Test
    public void registeredDefaultsPreservePeriodic200Ms() {
        Parameters parameters = ParameterUtils.createContextParameters(new HashMap<>());
        assertEquals("periodic", parameters.get(SSE_CONNECTION_BUFFER_MODE));
        assertEquals(200, parameters.get(SSE_CONNECTION_BUFFER_INTERVAL_MS));
        ServerConfig.Builder<String> builder = new ServerConfig.Builder<>();
        ServerSentEventsSink.configureConnectionBuffer(builder, (name, fallback) -> fallback);
        assertEquals(ServerConfig.ConnectionBufferMode.PERIODIC, builder.build().getConnectionBufferMode());
        assertEquals(200, builder.build().getConnectionBufferIntervalMs());
    }

    @Test
    public void jobOverridesReachSinkThroughDefaultPropertiesLoader() {
        Map<String, String> previous = new HashMap<>();
        String[] keys = {SSE_CONNECTION_BUFFER_MODE, SSE_CONNECTION_BUFFER_INTERVAL_MS,
                "JOB_PARAM_" + SSE_CONNECTION_BUFFER_MODE, "JOB_PARAM_" + SSE_CONNECTION_BUFFER_INTERVAL_MS};
        for (String key : keys) { previous.put(key, System.getProperty(key)); }
        try {
            // The worker exports job parameters under JOB_PARAM_; they override process defaults.
            System.setProperty(SSE_CONNECTION_BUFFER_MODE, "periodic");
            System.setProperty(SSE_CONNECTION_BUFFER_INTERVAL_MS, "200");
            for (int interval : new int[] {100, 150}) {
                Parameters parameters = ParameterUtils.createContextParameters(new HashMap<>(),
                        new Parameter(SSE_CONNECTION_BUFFER_MODE, "activity"),
                        new Parameter(SSE_CONNECTION_BUFFER_INTERVAL_MS, Integer.toString(interval)));
                System.setProperty("JOB_PARAM_" + SSE_CONNECTION_BUFFER_MODE,
                        parameters.get(SSE_CONNECTION_BUFFER_MODE).toString());
                System.setProperty("JOB_PARAM_" + SSE_CONNECTION_BUFFER_INTERVAL_MS,
                        parameters.get(SSE_CONNECTION_BUFFER_INTERVAL_MS).toString());
                ServerConfig.Builder<String> builder = new ServerConfig.Builder<>();
                ServerSentEventsSink.configureConnectionBuffer(builder,
                        new DefaultMantisPropertiesLoader(new Properties()));
                ServerConfig<String> config = builder.build();
                assertEquals(ServerConfig.ConnectionBufferMode.ACTIVITY, config.getConnectionBufferMode());
                assertEquals(interval, config.getConnectionBufferIntervalMs());
            }
        } finally {
            for (String key : keys) {
                if (previous.get(key) == null) { System.clearProperty(key); }
                else { System.setProperty(key, previous.get(key)); }
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnknownMode() {
        ParameterUtils.createContextParameters(new HashMap<>(),
                new Parameter(SSE_CONNECTION_BUFFER_MODE, "adaptive"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsZeroInterval() {
        ParameterUtils.createContextParameters(new HashMap<>(),
                new Parameter(SSE_CONNECTION_BUFFER_INTERVAL_MS, "0"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeInterval() {
        ParameterUtils.createContextParameters(new HashMap<>(),
                new Parameter(SSE_CONNECTION_BUFFER_INTERVAL_MS, "-1"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonNumericInterval() {
        ParameterUtils.createContextParameters(new HashMap<>(),
                new Parameter(SSE_CONNECTION_BUFFER_INTERVAL_MS, "invalid"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void sinkRejectsInvalidPropertyWithoutParameterValidation() {
        ServerSentEventsSink.configureConnectionBuffer(new ServerConfig.Builder<>(),
                (name, fallback) -> name.equals(SSE_CONNECTION_BUFFER_INTERVAL_MS) ? "0" : fallback);
    }
}
