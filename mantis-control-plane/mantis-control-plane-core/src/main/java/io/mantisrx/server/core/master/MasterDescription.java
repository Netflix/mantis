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

package io.mantisrx.server.core.master;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;


/**
 * A JSON-serializable data transfer object for Mantis master descriptions. It's used to transfer
 * metadata between master and workers.
 */
@EqualsAndHashCode
@ToString
public class MasterDescription {

    public static final String JSON_PROP_HOSTNAME = "hostname";
    public static final String JSON_PROP_HOST_IP = "hostIP";
    public static final String JSON_PROP_API_PORT = "apiPort";
    public static final String JSON_PROP_SCHED_INFO_PORT = "schedInfoPort";
    public static final String JSON_PROP_API_PORT_V2 = "apiPortV2";
    public static final String JSON_PROP_API_STATUS_URI = "apiStatusUri";
    public static final String JSON_PROP_CONSOLE_PORT = "consolePort";
    public static final String JSON_PROP_CREATE_TIME = "createTime";

    private final String hostname;
    private final String hostIP;
    private final int apiPort;
    private final int schedInfoPort;
    private final int apiPortV2;
    private final String apiStatusUri;
    private final long createTime;
    private final int consolePort;

    public static final MasterDescription MASTER_NULL =
        new MasterDescription("NONE", "localhost", -1, -1, -1, "uri://", -1, -1L);

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public MasterDescription(
            @JsonProperty(JSON_PROP_HOSTNAME) String hostname,
            @JsonProperty(JSON_PROP_HOST_IP) String hostIP,
            @JsonProperty(JSON_PROP_API_PORT) int apiPort,
            @JsonProperty(JSON_PROP_SCHED_INFO_PORT) int schedInfoPort,
            @JsonProperty(JSON_PROP_API_PORT_V2) int apiPortV2,
            @JsonProperty(JSON_PROP_API_STATUS_URI) String apiStatusUri,
            @JsonProperty(JSON_PROP_CONSOLE_PORT) int consolePort,
            @JsonProperty(JSON_PROP_CREATE_TIME) long createTime
    ) {
        this.hostname = hostname;
        this.hostIP = hostIP;
        this.apiPort = apiPort;
        this.schedInfoPort = schedInfoPort;
        this.apiPortV2 = apiPortV2;
        this.apiStatusUri = apiStatusUri;
        this.consolePort = consolePort;
        this.createTime = createTime;
    }

    @JsonProperty(JSON_PROP_HOSTNAME)
    public String getHostname() {
        return hostname;
    }

    @JsonProperty(JSON_PROP_HOST_IP)
    public String getHostIP() {
        return hostIP;
    }

    @JsonProperty(JSON_PROP_API_PORT)
    public int getApiPort() {
        return apiPort;
    }

    @JsonProperty(JSON_PROP_SCHED_INFO_PORT)
    public int getSchedInfoPort() {
        return schedInfoPort;
    }

    @JsonProperty(JSON_PROP_API_PORT_V2)
    public int getApiPortV2() {
        return apiPortV2;
    }

    @JsonProperty(JSON_PROP_API_STATUS_URI)
    public String getApiStatusUri() {
        return apiStatusUri;
    }

    @JsonProperty(JSON_PROP_CREATE_TIME)
    public long getCreateTime() {
        return createTime;
    }

    public String getFullApiStatusUri() {
        String uri = getApiStatusUri().trim();
        if (uri.startsWith("/")) {
            uri = uri.substring(1);
        }

        return String.format("http://%s:%d/%s", getHostname(), getApiPort(), uri);
    }

    @JsonProperty(JSON_PROP_CONSOLE_PORT)
    public int getConsolePort() {
        return consolePort;
    }
}
