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

package io.mantisrx.server.master.domain;

import java.net.URL;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.runtime.descriptor.SchedulingInfo;


public class Jar {

    private final URL url;
    private final String version;
    private final long uploadedAt;
    private final SchedulingInfo schedulingInfo;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public Jar(@JsonProperty("url") URL url,
               @JsonProperty("uploadedAt") long uploadedAt,
               @JsonProperty("version") String version,
               @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo) {
        this.url = url;
        this.uploadedAt = uploadedAt;
        this.version = (version == null || version.isEmpty()) ?
                "" + System.currentTimeMillis() :
                version;
        this.schedulingInfo = schedulingInfo;
    }

    public URL getUrl() {
        return url;
    }

    public long getUploadedAt() {
        return uploadedAt;
    }

    public String getVersion() {
        return version;
    }

    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }
}
