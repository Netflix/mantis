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

package io.mantisrx.sourcejob.kafka;

import static io.mantisrx.connector.kafka.source.MantisKafkaSourceConfig.DEFAULT_PARSE_MSG_IN_SOURCE;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import io.mantisrx.connector.kafka.KafkaAckable;
import io.mantisrx.connector.kafka.KafkaSourceParameters;
import io.mantisrx.connector.kafka.source.serde.ParseException;
import io.mantisrx.connector.kafka.source.serde.Parser;
import io.mantisrx.connector.kafka.source.serde.ParserType;
import io.mantisrx.runtime.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutoAckTaggingStage extends AbstractAckableTaggingStage {

    private static final Logger LOG = LoggerFactory.getLogger(AutoAckTaggingStage.class);

    public AutoAckTaggingStage() {
    }

    private static final Logger logger = LoggerFactory.getLogger(AutoAckTaggingStage.class);

    /**
     * default impl to ack the received data and returned parse kafka data
     *
     * @param ackable
     *
     * @return
     */
    @Override
    protected Map<String, Object> processAndAck(final Context context, KafkaAckable ackable) {
        try {

            Boolean messageParsedInSource = (Boolean) context.getParameters().get(KafkaSourceParameters.PARSE_MSG_IN_SOURCE, DEFAULT_PARSE_MSG_IN_SOURCE);
            String messageParserType = (String) context.getParameters().get(KafkaSourceParameters.PARSER_TYPE, ParserType.SIMPLE_JSON.getPropName());
            if (messageParsedInSource) {
                final Optional<Map<String, Object>> parsedEventO = ackable.getKafkaData().getParsedEvent();
                return parsedEventO.orElse(Collections.emptyMap());
            } else {
                final Parser parser = ParserType.parser(messageParserType).getParser();
                if (parser.canParse(ackable.getKafkaData().getRawBytes())) {
                    return parser.parseMessage(ackable.getKafkaData().getRawBytes());
                } else {
                    LOG.warn("cannot parse message {}", ackable.getKafkaData().getRawBytes().toString());
                    throw new ParseException("cannot parse message");
                }
            }

        } catch (Throwable t) {
            if (t instanceof ParseException) {
                logger.warn("failed to parse message", t);
            } else {
                logger.error("caught unexpected exception", t);
            }
        } finally {
            ackable.ack();
        }
        return Collections.emptyMap();
    }

    // no op
    @Override
    protected Map<String, Object> preProcess(Map<String, Object> rawData) {
        return rawData;
    }
}

