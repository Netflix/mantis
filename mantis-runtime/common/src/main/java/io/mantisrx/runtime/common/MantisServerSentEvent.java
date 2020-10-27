/*
 *
 * Copyright 2020 Netflix, Inc.
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
 *
 */

package io.mantisrx.runtime.common;

public class MantisServerSentEvent {

    private final String eventData;

    public MantisServerSentEvent(String data) {
        this.eventData = data;
    }

    public String getEventAsString() {
        return eventData;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((eventData == null) ? 0 : eventData.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MantisServerSentEvent other = (MantisServerSentEvent) obj;
        if (eventData == null) {
            if (other.eventData != null)
                return false;
        } else if (!eventData.equals(other.eventData))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MantisServerSentEvent [eventData=" + eventData + "]";
    }


}
