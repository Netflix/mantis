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

package io.mantisrx.common;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Worker Ports has the following semantics:
 *   1. capture the individual ports for each of metrics, debugging, console, custom, sink
 *   2. ports[0] should capture the sink port because of legacy reasons.
 */
@EqualsAndHashCode
@ToString
public class WorkerPorts implements Serializable {

    private final int metricsPort;
    private final int debugPort;
    private final int consolePort;
    private final int customPort;
    private final int sinkPort;
    private final List<Integer> ports;

    public WorkerPorts(final List<Integer> assignedPorts) {
        if (assignedPorts.size() < 5) {
            throw new IllegalArgumentException("assignedPorts should have at least 5 ports");
        }

        this.metricsPort = assignedPorts.get(0);
        this.debugPort = assignedPorts.get(1);
        this.consolePort = assignedPorts.get(2);
        this.customPort = assignedPorts.get(3);
        this.sinkPort = assignedPorts.get(4);
        this.ports = ImmutableList.of(assignedPorts.get(4));
        if (!isValid()) {
            throw new IllegalStateException("worker validation failed on port allocation");
        }
    }

    public WorkerPorts(int metricsPort, int debugPort, int consolePort, int customPort, int sinkPort) {
        this(ImmutableList.of(metricsPort, debugPort, consolePort, customPort, sinkPort));
    }

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public WorkerPorts(@JsonProperty("metricsPort") int metricsPort,
                       @JsonProperty("debugPort") int debugPort,
                       @JsonProperty("consolePort") int consolePort,
                       @JsonProperty("customPort") int customPort,
                       @JsonProperty("ports") List<Integer> ports) {
        this(ImmutableList.<Integer>builder()
                .add(metricsPort)
                .add(debugPort)
                .add(consolePort)
                .add(customPort)
                .addAll(ports)
                .build());
    }

    public int getMetricsPort() {
        return metricsPort;
    }

    public int getDebugPort() {
        return debugPort;
    }

    public int getConsolePort() {
        return consolePort;
    }

    public int getCustomPort() {
        return customPort;
    }

    public int getSinkPort() { return sinkPort; }

    @JsonIgnore
    public List<Integer> getAllPorts() {
        final List<Integer> allPorts = new ArrayList<>();
        allPorts.add(metricsPort);
        allPorts.add(debugPort);
        allPorts.add(consolePort);
        allPorts.add(customPort);
        allPorts.addAll(ports);
        return allPorts;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    /**
     * Validates that this object has at least 5 valid ports and all of them are unique.
     */
    private boolean isValid() {
        Set<Integer> uniquePorts = new HashSet<>();
        uniquePorts.add(metricsPort);
        uniquePorts.add(consolePort);
        uniquePorts.add(debugPort);
        uniquePorts.add(customPort);
        uniquePorts.add(sinkPort);

        return uniquePorts.size() >= 5
                && isValidPort(metricsPort)
                && isValidPort(consolePort)
                && isValidPort(debugPort)
                && isValidPort(customPort)
                && isValidPort(sinkPort);
    }

    /**
     * A port with 0 is technically correct, but we disallow it because there would be an inconsistency between
     * what unused port the OS selects (some port number) and what this object's metadata holds (0).
     */
    private boolean isValidPort(int port) {
        return port > 0 && port <= 65535;
    }
}
