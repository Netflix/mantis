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

package io.mantisrx.common.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class DateTimeExt {

    private static final DateTimeFormatter ISO_UTC_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC"));

    private DateTimeExt() {
    }

    /**
     * The time given in the argument is scoped to a local (system default) time zone. The result
     * is adjusted to UTC time zone.
     */
    public static String toUtcDateTimeString(long msSinceEpoch) {
        if (msSinceEpoch == 0L) {
            return null;
        }
        return ISO_UTC_DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(msSinceEpoch)) + 'Z';
    }
}
