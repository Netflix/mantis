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

package io.mantisrx.server.master.client.diagnostic.plugin;

import java.util.Map;

/** struct for recording messages */
public class DiagnosticMessage {

    public static class Builder {
        private final DiagnosticMessageType messageType;
        private Throwable error;
        private String description;
        private Map<String, String> tags;

        private Builder(final DiagnosticMessageType messageType) {
            this.messageType = messageType;
        }

        public Builder withError(final Throwable error) {
            this.error = error;
            return this;
        }

        public Builder withDescription(final String description) {
            this.description = description;
            return this;
        }

        public Builder withTags(final Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public DiagnosticMessage build() {
            return new DiagnosticMessage(this);
        }
    }

    private DiagnosticMessage(final Builder builder) {
        this.messageType = builder.messageType;
        this.error = builder.error;
        this.description = builder.description;
        this.tags = builder.tags;
    }

    /** used to construct a diagnostic message with the standard Java builder pattern */
    public static Builder builder(final DiagnosticMessageType messageType) {
        return new Builder(messageType);
    }

    /** the message type of the diagnostic message */
    public DiagnosticMessageType getMessageType() {
        return messageType;
    }

    /** if an exception is part of the diagnostic message, the exception.  Otherwise null */
    public Throwable getError() {
        return error;
    }

    /** if the message has descriptive tags, the tags.  Otherwise null */
    public Map<String, String> getTags() {
        return tags;
    }

    /** if there is descriptive text, like a log message associated with the log message, the text.  Otherwise null */
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "DiagnosticMessage [messageType=" + messageType + ", error="
                + error + ", tags=" + tags + ", description=" + description
                + "]";
    }

    private final DiagnosticMessageType messageType;
    private final Throwable error;
    private final Map<String, String> tags;
    private final String description;
}
