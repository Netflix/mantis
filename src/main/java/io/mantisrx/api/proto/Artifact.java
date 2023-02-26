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

package io.mantisrx.api.proto;

import java.util.Objects;


public class Artifact {
    private long sizeInBytes;
    private String fileName;
    private byte[] content;

    public Artifact(String fileName, long sizeInBytes, byte[] content) {
        Objects.requireNonNull(fileName, "File name cannot be null");
        Objects.requireNonNull(content, "Content cannot be null");
        this.fileName = fileName;
        this.sizeInBytes = sizeInBytes;
        this.content = content;
    }

    public long getSizeInBytes() {
        return this.sizeInBytes;
    }

    public byte[] getContent() {
        return content;
    }

    public String getFileName() {
        return  fileName;
    }
}
