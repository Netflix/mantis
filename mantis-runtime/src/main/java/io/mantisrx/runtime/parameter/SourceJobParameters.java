/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.runtime.parameter;

import com.mantisrx.common.utils.MantisSSEConstants;
import io.mantisrx.runtime.Context;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import io.mantisrx.shaded.com.fasterxml.jackson.core.type.TypeReference;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceJobParameters {
    public static final String MANTIS_SOURCEJOB_TARGET_KEY = "target";
    public static final String MANTIS_SOURCEJOB_NAME_PARAM = "sourceJobName";
    public static final String MANTIS_SOURCEJOB_CRITERION = "criterion";
    public static final String MANTIS_SOURCEJOB_CLIENT_ID = "clientId";
    public static final String MANTIS_SOURCEJOB_IS_BROADCAST_MODE = "isBroadcastMode";
    public static final String MANTIS_SOURCEJOB_ADDITIONAL_PARAMS = "additionalParams";
    private static final Logger log = LoggerFactory.getLogger(SourceJobParameters.class);
    private static final ObjectMapper mapper = new ObjectMapper();


    public static List<TargetInfo> parseInputParameters(Context ctx) {
        String targetListStr = (String) ctx.getParameters()
                .get(MANTIS_SOURCEJOB_TARGET_KEY, "{}");
        return parseTargetInfo(targetListStr);
    }

    public static List<TargetInfo> parseTargetInfo(String targetListStr) {
        List<TargetInfo> targetList = new ArrayList<>();

        try {
            Map<String, List<TargetInfo>> targets = mapper.readValue(targetListStr, new TypeReference<Map<String, List<TargetInfo>>>() {});
            if (targets.get("targets") != null) {
                return targets.get("targets");
            }
        } catch (Exception ex) {
            log.error("Failed to parse target list: {}", targetListStr, ex);
        }

        return targetList;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TargetInfo {

        @JsonProperty(MANTIS_SOURCEJOB_NAME_PARAM) public String sourceJobName;
        @JsonProperty(MANTIS_SOURCEJOB_CRITERION) public String criterion;
        @JsonProperty(MANTIS_SOURCEJOB_CLIENT_ID) public String clientId;
        @JsonProperty(MantisSSEConstants.SAMPLE) public int samplePerSec = -1;
        @JsonProperty(MANTIS_SOURCEJOB_IS_BROADCAST_MODE) public boolean isBroadcastMode;
        @JsonProperty(MantisSSEConstants.ENABLE_META_MESSAGES) public boolean enableMetaMessages;
        @JsonProperty(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION) public boolean enableCompressedBinary;
        @JsonProperty(MantisSSEConstants.MANTIS_COMPRESSION_DELIMITER) public String delimiter;
        @JsonProperty(MANTIS_SOURCEJOB_ADDITIONAL_PARAMS) public Map<String, String> additionalParams;

        public TargetInfo(String jobName,
                          String criterion,
                          String clientId,
                          int samplePerSec,
                          boolean isBroadcastMode,
                          boolean enableMetaMessages,
                          boolean enableCompressedBinary,
                          String delimiter) {
            this(jobName, criterion, clientId, samplePerSec, isBroadcastMode, enableMetaMessages, enableCompressedBinary, delimiter, null);
        }

        public TargetInfo(String jobName,
                          String criterion,
                          String clientId,
                          int samplePerSec,
                          boolean isBroadcastMode,
                          boolean enableMetaMessages,
                          boolean enableCompressedBinary,
                          String delimiter,
                          Map<String, String> additionalParams) {
            this.sourceJobName = jobName;
            this.criterion = criterion;
            this.clientId = clientId;
            this.samplePerSec = samplePerSec;
            this.isBroadcastMode = isBroadcastMode;
            this.enableMetaMessages = enableMetaMessages;
            this.enableCompressedBinary = enableCompressedBinary;
            this.delimiter = delimiter;
            this.additionalParams = additionalParams;
        }

        public TargetInfo() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TargetInfo that = (TargetInfo) o;
            return Objects.equals(sourceJobName, that.sourceJobName) &&
                    Objects.equals(criterion, that.criterion) &&
                    Objects.equals(clientId, that.clientId) &&
                    Objects.equals(samplePerSec, that.samplePerSec) &&
                    Objects.equals(isBroadcastMode, that.isBroadcastMode) &&
                    Objects.equals(enableMetaMessages, that.enableMetaMessages) &&
                    Objects.equals(enableCompressedBinary, that.enableCompressedBinary) &&
                    Objects.equals(delimiter, that.delimiter) &&
                    Objects.equals(additionalParams, that.additionalParams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceJobName, criterion, clientId, samplePerSec, isBroadcastMode, enableMetaMessages, enableCompressedBinary, delimiter, additionalParams);
        }

        @Override
        public String toString() {
            return "TargetInfo{" +
                    "sourceJobName=" + sourceJobName + "," +
                    "criterion=" + criterion + "," +
                    "clientId=" + clientId + "," +
                    "samplePerSec=" + samplePerSec + "," +
                    "isBroadcastMode=" + isBroadcastMode + "," +
                    "enableMetaMessages=" + enableMetaMessages + "," +
                    "enableCompressedBinary=" + enableCompressedBinary + "," +
                    "delimiter=" + delimiter + "," +
                    "additionalParams=" + additionalParams +
                    "}";
        }
    }

    public static class TargetInfoBuilder {

        private String sourceJobName;
        private String criterion;
        private String clientId;
        private int samplePerSec = -1;
        private boolean isBroadcastMode = false;
        private boolean enableMetaMessages = false;
        private boolean enableCompressedBinary = false;
        private String delimiter = null;
        private Map<String, String> additionalParams;

        public TargetInfoBuilder() {
        }

        public TargetInfoBuilder withSourceJobName(String srcJobName) {
            this.sourceJobName = srcJobName;
            return this;
        }

        public TargetInfoBuilder withQuery(String query) {
            this.criterion = query;
            return this;
        }

        public TargetInfoBuilder withSamplePerSec(int samplePerSec) {
            this.samplePerSec = samplePerSec;
            return this;
        }

        public TargetInfoBuilder withBroadCastMode() {
            this.isBroadcastMode = true;
            return this;
        }

        public TargetInfoBuilder withMetaMessagesEnabled() {
            this.enableMetaMessages = true;
            return this;
        }


        public TargetInfoBuilder withBinaryCompressionEnabled() {
            this.enableCompressedBinary = true;
            return this;
        }

        public TargetInfoBuilder withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public TargetInfoBuilder withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public TargetInfoBuilder withAdditionalParams(Map<String, String> additionalParams) {
            this.additionalParams = additionalParams;
            return this;
        }

        public TargetInfo build() {
            return new TargetInfo(
                    sourceJobName,
                    criterion,
                    clientId,
                    samplePerSec,
                    isBroadcastMode,
                    enableMetaMessages,
                    enableCompressedBinary,
                    delimiter,
                    additionalParams);
        }
    }

    /**
     * Ensures that a list of TargetInfo contains a sane set of sourceJobName, ClientId pairs.
     * TODO: Currently mutates the list, which isn't problematic here, but it would be prudent to clean this up.
     *
     * @param targets A List of TargetInfo for which to validate and correct clientId inconsistencies.
     *
     * @return The original List modified to have consistent clientIds.
     */
    public static List<TargetInfo> enforceClientIdConsistency(List<TargetInfo> targets, String defaultClientId) {

        targets.sort(Comparator.comparing(t -> t.criterion));
        HashSet<Map.Entry<String, String>> connectionPairs = new HashSet<>(targets.size());

        for (TargetInfo target : targets) {
            if (target.clientId == null) {
                target.clientId = defaultClientId;
            }

            Map.Entry<String, String> connectionPair = new AbstractMap.SimpleEntry<>(target.sourceJobName, target.clientId);
            int attempts = 0;

            while (connectionPairs.contains(connectionPair)) {
                connectionPair = new AbstractMap.SimpleEntry<>(target.sourceJobName, target.clientId + "_" + ++attempts);
            }

            target.clientId = connectionPair.getValue();
            connectionPairs.add(connectionPair);
        }

        return targets;
    }
}
