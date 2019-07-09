/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.connectors.kafka.source.serde;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;


public interface Parser {

    /**
     * Determine if the payload byte array is parsable.
     *
     * @param message
     *
     * @return boolean indicate if payload is parsable
     */
    boolean canParse(byte[] message);

    /**
     * parse a payload byte array into a map.
     *
     * @param message
     *
     * @return map
     *
     * @throws ParseException
     */
    Map<String, Object> parseMessage(byte[] message) throws ParseException;

    /**
     * Returns partial readable payload information, if encoding is not supported fallback to Base64.
     *
     * @param payload
     *
     * @return string message
     *
     * @throws UnsupportedEncodingException
     */
    default String getPartialPayLoadForLogging(byte[] payload) {
        String msg = new String(payload, StandardCharsets.UTF_8);
        return msg.length() <= 128 ? msg : msg.substring(0, 127);
    }
}
