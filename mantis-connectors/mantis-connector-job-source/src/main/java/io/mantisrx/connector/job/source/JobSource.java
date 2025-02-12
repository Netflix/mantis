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

package io.mantisrx.connector.job.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mantisrx.common.utils.Closeables;
import com.mantisrx.common.utils.MantisSSEConstants;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.connector.job.core.AbstractSourceJobSource;
import io.mantisrx.connector.job.core.DefaultSinkConnectionStatusObserver;
import io.mantisrx.connector.job.core.MantisSourceJobConnector;
import io.mantisrx.connector.job.core.MultiSinkConnectionStatusObserver;
import io.mantisrx.connector.job.core.SinkConnectionStatusObserver;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.parameter.ParameterDefinition;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.runtime.parameter.type.StringParameter;
import io.mantisrx.runtime.parameter.validator.Validators;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import io.mantisrx.shaded.com.google.common.collect.Lists;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.io.IOException;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Slf4j
public class JobSource extends AbstractSourceJobSource implements Source<MantisServerSentEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobSource.class);
    private static JsonParser parser = new JsonParser();
    protected List<TargetInfo> targets;
    private final List<MantisSSEJob> jobs = new ArrayList<>();

    public JobSource(List<TargetInfo> targets) {
        this.targets = targets;
    }

    // For backwards compatibility.
    public JobSource() {
        this(new ArrayList<>());
    }

    public JobSource(String targetInfoStr) {
        this.targets = parseTargetInfo(targetInfoStr);
    }

    @Override
    public List<ParameterDefinition<?>> getParameters() {

        List<ParameterDefinition<?>> params = Lists.newArrayList();

        params.add(new StringParameter()
                .name(MantisSourceJobConnector.MANTIS_SOURCEJOB_TARGET_KEY)
                .validator(Validators.notNullOrEmpty())
                .defaultValue("{}")
                .build());

        return params;
    }

    @Override
    public Observable<Observable<MantisServerSentEvent>> call(Context context, Index index) {
        if (targets.isEmpty()) {
            targets = parseInputParameters(context);
        }

        Observable<Observable<MantisServerSentEvent>> sourceObs = null;
        int workerNo = context.getWorkerInfo().getWorkerNumber();

        targets = enforceClientIdConsistency(targets, context.getJobId());

        for (TargetInfo targetInfo : targets) {
            MantisSSEJob job;
            String sourceJobName = targetInfo.sourceJobName;
            String criterion = targetInfo.criterion;
            int samplePerSec = targetInfo.samplePerSec;
            boolean enableMetaMessages = targetInfo.enableMetaMessages;
            LOGGER.info("Processing job " + sourceJobName);
            boolean singleton = false;
            SinkConnectionStatusObserver obs = DefaultSinkConnectionStatusObserver.getInstance(singleton);
            MultiSinkConnectionStatusObserver.INSTANCE.addSinkConnectionObserver(sourceJobName, obs);

            String clientId = targetInfo.clientId;
            if (targetInfo.isBroadcastMode) {
                clientId = clientId + "_" + workerNo;
            }
            boolean enableCompressedBinary = targetInfo.enableCompressedBinary;
            Map<String, String> additionalParams = targetInfo.additionalParams;

            job = getSourceJob(sourceJobName, criterion, clientId, samplePerSec, enableMetaMessages, enableCompressedBinary, obs, additionalParams, Optional.<SinkParameters>empty());
            jobs.add(job);

            if (sourceObs == null) {
                sourceObs = job.connectAndGet();
            } else {
                if (job != null) {
                    Observable<Observable<MantisServerSentEvent>> clientObs = job.connectAndGet();
                    if (clientObs != null) {
                        sourceObs = sourceObs.mergeWith(clientObs);
                    } else {
                        LOGGER.error("Could not connect to job " + sourceJobName);
                    }
                } else {
                    LOGGER.error("Could not connect to job " + sourceJobName);
                }
            }
        }
        return sourceObs;
    }

    @Override
    public void close() throws IOException {
        try {
            Closeables.combine(jobs).close();
        } finally {
            jobs.clear();
        }
    }

    /**
     * Use {@link io.mantisrx.runtime.parameter.SourceJobParameters.TargetInfo} instead.
     */
    @Deprecated
    public static class TargetInfo {

        public String sourceJobName;
        public String criterion;
        public int samplePerSec;
        public boolean isBroadcastMode;
        public boolean enableMetaMessages;
        public boolean enableCompressedBinary;
        public String clientId;
        public Map<String, String> additionalParams;

        public TargetInfo(String jobName,
                          String criterion,
                          String clientId,
                          int samplePerSec,
                          boolean isBroadcastMode,
                          boolean enableMetaMessages,
                          boolean enableCompressedBinary) {
            this(jobName, criterion, clientId, samplePerSec, isBroadcastMode, enableMetaMessages, enableCompressedBinary, null);
        }

        public TargetInfo(String jobName,
                          String criterion,
                          String clientId,
                          int samplePerSec,
                          boolean isBroadcastMode,
                          boolean enableMetaMessages,
                          boolean enableCompressedBinary,
                          Map<String, String> additionalParams) {
            this.sourceJobName = jobName;
            this.criterion = criterion;
            this.clientId = clientId;
            this.samplePerSec = samplePerSec;
            this.isBroadcastMode = isBroadcastMode;
            this.enableMetaMessages = enableMetaMessages;
            this.enableCompressedBinary = enableCompressedBinary;
            this.additionalParams = additionalParams;
        }
    }

    protected static List<TargetInfo> parseInputParameters(Context ctx) {
        String targetListStr = (String) ctx.getParameters()
                .get(MantisSourceJobConnector.MANTIS_SOURCEJOB_TARGET_KEY, "{}");
        return parseTargetInfo(targetListStr);
    }

    /**
     * Use {@link io.mantisrx.runtime.parameter.SourceJobParameters#parseTargetInfo(String)} instead.
     */
    @Deprecated
    protected static List<TargetInfo> parseTargetInfo(String targetListStr) {
        List<TargetInfo> targetList = new ArrayList<TargetInfo>();
        JsonObject requestObj = (JsonObject) parser.parse(targetListStr);
        JsonArray arr = requestObj.get("targets").getAsJsonArray();

        for (int i = 0; i < arr.size(); i++) {
            int sample = -1;
            boolean isBroadCastMode = false;
            JsonObject srcObj = arr.get(i).getAsJsonObject();
            String sName = srcObj.get(MantisSourceJobConnector.MANTIS_SOURCEJOB_NAME_PARAM).getAsString();
            String criterion = srcObj.get(MantisSourceJobConnector.MANTIS_SOURCEJOB_CRITERION).getAsString();

            String clientId = null;
            if (srcObj.get(MantisSourceJobConnector.MANTIS_SOURCEJOB_CLIENT_ID) != null) {
                clientId = srcObj.get(MantisSourceJobConnector.MANTIS_SOURCEJOB_CLIENT_ID).getAsString();
            }

            if (srcObj.get(MantisSSEConstants.SAMPLE) != null) {
                sample = srcObj.get(MantisSSEConstants.SAMPLE).getAsInt();
            }
            if (srcObj.get(MantisSourceJobConnector.MANTIS_SOURCEJOB_IS_BROADCAST_MODE) != null) {
                isBroadCastMode =
                        srcObj.get(MantisSourceJobConnector.MANTIS_SOURCEJOB_IS_BROADCAST_MODE).getAsBoolean();
            }
            boolean enableMetaMessages = false;
            if (srcObj.get(MantisSSEConstants.ENABLE_META_MESSAGES) != null) {
                enableMetaMessages = srcObj.get(MantisSSEConstants.ENABLE_META_MESSAGES).getAsBoolean();
            }
            boolean enableCompressedBinary = false;
            if (srcObj.get(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION) != null) {
                enableCompressedBinary = true;
            }

            // Extract additionalParams
            Map<String, String> additionalParams = new HashMap<>();
            for (Map.Entry<String, JsonElement> entry : srcObj.entrySet()) {
                String key = entry.getKey();
                if (key.equals(MantisSourceJobConnector.MANTIS_SOURCEJOB_NAME_PARAM) ||
                    key.equals(MantisSourceJobConnector.MANTIS_SOURCEJOB_CRITERION) ||
                    key.equals(MantisSourceJobConnector.MANTIS_SOURCEJOB_CLIENT_ID) ||
                    key.equals(MantisSSEConstants.SAMPLE) ||
                    key.equals(MantisSourceJobConnector.MANTIS_SOURCEJOB_IS_BROADCAST_MODE) ||
                    key.equals(MantisSSEConstants.ENABLE_META_MESSAGES) ||
                    key.equals(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION)) {
                    LOGGER.warn("Overwriting key " + key + " in additionalParams");
                }
                additionalParams.put(key, entry.getValue().getAsString());
            }

            TargetInfo ti = new TargetInfo(
                    sName,
                    criterion,
                    clientId,
                    sample,
                    isBroadCastMode,
                    enableMetaMessages,
                    enableCompressedBinary,
                    additionalParams);
            targetList.add(ti);
            LOGGER.info("sname: " + sName + " criterion: " + criterion + " isBroadcastMode " + isBroadCastMode);
        }

        return targetList;
    }

    /**
     * Use {@link io.mantisrx.runtime.parameter.SourceJobParameters.TargetInfoBuilder} instead.
     */
    @Deprecated
    public static class TargetInfoBuilder {

        private String sourceJobName;
        private String criterion;
        private String clientId;
        private int samplePerSec = -1;
        private boolean isBroadcastMode = false;
        private boolean enableMetaMessages = false;
        private boolean enableCompressedBinary = false;
        private Map<String, String> additionalParams = new HashMap<>();

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
                    additionalParams);
        }
    }

    /**
     * Use {@link io.mantisrx.runtime.parameter.SourceJobParameters#enforceClientIdConsistency(List, String)} instead.
     *
     * Ensures that a list of TargetInfo contains a sane set of sourceJobName, ClientId pairs.
     * TODO: Currently mutates the list, which isn't problematic here, but it would be prudent to clean this up.
     *
     * @param targets A List of TargetInfo for which to validate and correct clientId inconsistencies.
     *
     * @return The original List modified to have consistent clientIds.
     */
    @Deprecated
    public static List<TargetInfo> enforceClientIdConsistency(List<TargetInfo> targets, String defaultClientId) {

        targets.sort(Comparator.comparing(t -> t.criterion));
        HashSet<Tuple2<String, String>> connectionPairs = new HashSet<>(targets.size());

        for (TargetInfo target : targets) {
            if (target.clientId == null) {
                target.clientId = defaultClientId;
            }

            Tuple2<String, String> connectionPair = Tuple.of(target.sourceJobName, target.clientId);
            int attempts = 0;

            while (connectionPairs.contains(connectionPair)) {
                connectionPair = Tuple.of(target.sourceJobName, target.clientId + "_" + ++attempts);
            }

            target.clientId = connectionPair._2;
            connectionPairs.add(connectionPair);
        }

        return targets;
    }
}
