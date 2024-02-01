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

package io.mantisrx.server.core.domain;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobArtifact {
    // Job artifact unique identifier
    @JsonProperty("artifactID")
    ArtifactID artifactID;
    // Job artifact name
    @JsonProperty("name")
    String name;
    // Job artifact version
    @JsonProperty("version")
    String version;
    // Job artifact creation timestamp
    @JsonProperty("createdAt")
    Instant createdAt;
    // Runtime Type Identification. For java potential values: spring-boot, guice
    @JsonProperty("runtimeType")
    String runtimeType;
    // Job artifact external dependencies
    // Example: {"mantis-runtime": "1.0.0"}
    @JsonProperty("dependencies")
    Map<String, String> dependencies;
    // Job entrypoint clas.
    @JsonProperty("entrypoint")
    String entrypoint;

    /**
     * tags for the current job artifact. E.g. jdk version, SBN version etc.
     */
    @JsonProperty("tags")
    Map<String, String> tags;
}
