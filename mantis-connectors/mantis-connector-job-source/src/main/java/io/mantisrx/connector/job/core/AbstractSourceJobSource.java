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

package io.mantisrx.connector.job.core;

import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.client.SinkConnectionsStatus;
import io.mantisrx.runtime.parameter.SinkParameters;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;


public abstract class AbstractSourceJobSource extends AbstractJobSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSourceJobSource.class);

    /**
     * @deprecated use {@link #getSourceJob(String, String, String, int, Optional)}, forPartition & toPartition params are not used and will be removed in next release
     */
    @Deprecated
    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId, int forPartition, int totalPartitions, int samplePerSec) {
        LOGGER.info("Connecting to source job " + sourceJobName);
        return getSourceJob(sourceJobName, criterion, clientId, samplePerSec, Optional.empty());
    }

    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId, int samplePerSec, Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to source job " + sourceJobName);
        return getSourceJob(sourceJobName, criterion, clientId, samplePerSec, new MantisSourceJobConnector.NoOpSinkConnectionsStatusObserver(), sinkParamsO);
    }

    /**
     * @deprecated use {@link #getSourceJob(String, String, String, int, Observer, Optional)}
     */
    @Deprecated
    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId, int forPartition, int totalPartitions, int samplePerSec, Observer<SinkConnectionsStatus> sinkConnObs) {
        LOGGER.info("Connecting to source job " + sourceJobName + " obs " + sinkConnObs);
        boolean enableMetaMessages = false;
        boolean enableCompressedBinaryInput = false;
        return connectToQueryBasedJob(MantisSourceJobConnectorFactory.getConnector(), criterion, sourceJobName, clientId, samplePerSec, enableMetaMessages, enableCompressedBinaryInput, sinkConnObs, null, Optional.<SinkParameters>empty());

    }

    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId,
                                     int samplePerSec, Observer<SinkConnectionsStatus> sinkConnObs, Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to source job " + sourceJobName + " obs " + sinkConnObs);
        boolean enableMetaMessages = false;
        return getSourceJob(sourceJobName, criterion, clientId, samplePerSec, enableMetaMessages, sinkConnObs, sinkParamsO);
    }

    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId,
                                     int samplePerSec, boolean enableMetaMessages, Observer<SinkConnectionsStatus> sinkConnObs, Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to source job " + sourceJobName + " obs " + sinkConnObs);
        boolean enableCompressedBinary = false;
        return getSourceJob(sourceJobName, criterion, clientId, samplePerSec, enableMetaMessages, enableCompressedBinary, sinkConnObs, sinkParamsO);
    }

    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId,
                                     int samplePerSec, boolean enableMetaMessages, boolean enableCompressedBinaryInput, Observer<SinkConnectionsStatus> sinkConnObs, Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to source job " + sourceJobName + " obs " + sinkConnObs);
        return connectToQueryBasedJob(MantisSourceJobConnectorFactory.getConnector(), criterion, sourceJobName, clientId, samplePerSec, enableMetaMessages, enableCompressedBinaryInput, sinkConnObs, null, sinkParamsO);

    }

    public MantisSSEJob getSourceJob(String sourceJobName, String criterion, String clientId,
                                     int samplePerSec, boolean enableMetaMessages, boolean enableCompressedBinaryInput, Observer<SinkConnectionsStatus> sinkConnObs, Map<String, String> additionalParams, Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to source job " + sourceJobName + " obs " + sinkConnObs);
        return connectToQueryBasedJob(MantisSourceJobConnectorFactory.getConnector(), criterion, sourceJobName, clientId, samplePerSec, enableMetaMessages, enableCompressedBinaryInput, sinkConnObs, additionalParams, sinkParamsO);

    }

    private MantisSSEJob connectToQueryBasedJob(MantisSourceJobConnector connector, String criterion,
                                                String jobName, String clientId, int samplePerSec, boolean enableMetaMessages, boolean enableCompressedBinaryInput,
                                                Observer<SinkConnectionsStatus> sinkConnObs,
                                                Map<String, String> additionalParams,
                                                Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to " + jobName);
        if (criterion == null || criterion.isEmpty()) {
            throw new RuntimeException("Criterion cannot be empty");
        }
        String subId = Integer.toString(criterion.hashCode());
        SinkParameters defaultParams = getDefaultSinkParams(clientId, samplePerSec,
                Optional.of(criterion), Optional.of(subId), enableMetaMessages, enableCompressedBinaryInput, 500, additionalParams);

        return connector.connectToJob(jobName, sinkParamsO.orElse(defaultParams), sinkConnObs);
    }
}
