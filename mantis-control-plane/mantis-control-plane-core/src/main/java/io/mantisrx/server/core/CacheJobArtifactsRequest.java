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

package io.mantisrx.server.core;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import lombok.NonNull;
import lombok.Value;

@Value
public class CacheJobArtifactsRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    @NonNull
    final List<URI> artifacts;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public CacheJobArtifactsRequest(@JsonProperty("artifacts") List<URI> artifacts) {
        this.artifacts = artifacts;
    }
}
