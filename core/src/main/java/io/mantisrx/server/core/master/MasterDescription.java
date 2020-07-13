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
import io.mantisrx.shaded.com.google.common.base.Objects;


/**
 * A JSON-serializable data transfer object for Mantis master descriptions. It's used to transfer
 * metadata between master and workers.
 */
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


    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("hostname", hostname)
                .add("hostIP", hostIP)
                .add("apiPort", apiPort)
                .add("schedInfoPort", schedInfoPort)
                .add("apiPortV2", apiPortV2)
                .add("apiStatusUri", apiStatusUri)
                .add("createTime", createTime)
                .add("consolePort", consolePort)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MasterDescription that = (MasterDescription) o;

        if (apiPort != that.apiPort) return false;
        if (schedInfoPort != that.schedInfoPort) return false;
        if (apiPortV2 != that.apiPortV2) return false;
        if (consolePort != that.consolePort) return false;
        if (createTime != that.createTime) return false;
        if (apiStatusUri != null ? !apiStatusUri.equals(that.apiStatusUri) : that.apiStatusUri != null) return false;
        if (hostIP != null ? !hostIP.equals(that.hostIP) : that.hostIP != null) return false;
        return hostname != null ? hostname.equals(that.hostname) : that.hostname == null;
    }

    @Override
    public int hashCode() {
        int result = hostname != null ? hostname.hashCode() : 0;
        result = 31 * result + (hostIP != null ? hostIP.hashCode() : 0);
        result = 31 * result + apiPort;
        result = 31 * result + apiPortV2;
        result = 31 * result + (apiStatusUri != null ? apiStatusUri.hashCode() : 0);
        result = 31 * result + (int) (createTime ^ (createTime >>> 32));
        result = 31 * result + consolePort;
        return result;
    }
}
