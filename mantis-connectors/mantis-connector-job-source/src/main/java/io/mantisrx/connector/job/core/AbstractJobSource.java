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

import com.mantisrx.common.utils.MantisSSEConstants;
import io.mantisrx.client.MantisSSEJob;
import io.mantisrx.client.SinkConnectionsStatus;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.runtime.source.Source;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;


public abstract class AbstractJobSource implements Source<MantisServerSentEvent> {

    private static final int DEFAULT_META_MSG_INTERVAL_MSEC = 500;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJobSource.class);

    public SinkParameters getDefaultSinkParams(final String clientId,
                                               final int samplePerSec,
                                               final Optional<String> criterion,
                                               final Optional<String> subscriptionId,
                                               final boolean enableMetaMessages,
                                               boolean enableCompressedBinaryInput, final long metaMessageInterval) {
        return this.getDefaultSinkParams(clientId, samplePerSec, criterion, subscriptionId, enableMetaMessages, enableCompressedBinaryInput, metaMessageInterval, null);
    }

    public SinkParameters getDefaultSinkParams(final String clientId,
                                               final int samplePerSec,
                                               final Optional<String> criterion,
                                               final Optional<String> subscriptionId,
                                               final boolean enableMetaMessages,
                                               boolean enableCompressedBinaryInput, final long metaMessageInterval, Map<String, String> additionalParams) {
        SinkParameters.Builder defaultParamBuilder = new SinkParameters.Builder();

        try {
            defaultParamBuilder = defaultParamBuilder
                    .withParameter(MantisSourceJobConnector.MANTIS_SOURCEJOB_CLIENT_ID_PARAM, clientId)
                    .withParameter(MantisSSEConstants.ENABLE_PINGS, "true");
            if (samplePerSec >= 1) {
                defaultParamBuilder = defaultParamBuilder.withParameter("sample", Integer.toString(samplePerSec));
            }
            if (criterion.isPresent()) {
                defaultParamBuilder =
                        defaultParamBuilder.withParameter(MantisSourceJobConnector.MANTIS_SOURCEJOB_CRITERION, criterion.get());
            }
            if (subscriptionId.isPresent()) {
                defaultParamBuilder = defaultParamBuilder.withParameter(MantisSourceJobConnector.MANTIS_SOURCEJOB_SUBSCRIPTION_ID, subscriptionId.get());
            }
            if (enableMetaMessages) {
                defaultParamBuilder = defaultParamBuilder.withParameter(MantisSSEConstants.ENABLE_META_MESSAGES, Boolean.toString(true));

                defaultParamBuilder = defaultParamBuilder.withParameter(MantisSSEConstants.META_MESSAGES_SEC, Long.toString(metaMessageInterval));
            }
            if (enableCompressedBinaryInput) {
                defaultParamBuilder = defaultParamBuilder.withParameter(MantisSSEConstants.MANTIS_ENABLE_COMPRESSION, Boolean.toString(true));
            }
            if (additionalParams != null) {
                for (Map.Entry<String, String> entry : additionalParams.entrySet()) {
                    defaultParamBuilder = defaultParamBuilder.withParameter(entry.getKey(), entry.getValue());
                }
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }


        return defaultParamBuilder.build();
    }

    public MantisSSEJob getJob(String jobName, String clientId, int samplePerSec,
                               Observer<SinkConnectionsStatus> sinkConnObs, Optional<SinkParameters> sinkParamsO) {
        LOGGER.info("Connecting to job " + jobName + " obs " + sinkConnObs);
        boolean enableMetaMessages = false;
        boolean enableCompressedBinaryInput = false;
        MantisSourceJobConnector connector = MantisSourceJobConnectorFactory.getConnector();
        SinkParameters defaultParams = getDefaultSinkParams(clientId,
                samplePerSec, Optional.<String>empty(), Optional.<String>empty(), enableMetaMessages, enableCompressedBinaryInput, DEFAULT_META_MSG_INTERVAL_MSEC);
        return connector.connectToJob(jobName, sinkParamsO.orElse(defaultParams), sinkConnObs);
    }

}
