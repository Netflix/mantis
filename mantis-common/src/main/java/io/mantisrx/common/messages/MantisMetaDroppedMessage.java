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

package io.mantisrx.common.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;


public class MantisMetaDroppedMessage extends MantisMetaMessage {

    private final long time;
    @JsonProperty("mantis.meta")
    private final String type = "droppedMessageCount";
    @JsonProperty("value")
    private final String value;

    public MantisMetaDroppedMessage(long dropCount, long time) {
        this.time = time;
        this.value = String.valueOf(dropCount);
    }

    public static void main(String[] args) {
        MantisMetaDroppedMessage m = new MantisMetaDroppedMessage(2, System.currentTimeMillis());
        System.out.println("M " + m.toString());

    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public long getTime() {
        return this.time;

    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        String str;
        try {
            str = mapper.writeValueAsString(this);
            return str;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{\"mantis.meta\" : \"error\"" + e.getMessage() + "}";
        }
    }

}
