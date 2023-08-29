/*
 * Copyright 2023 Netflix, Inc.
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

package io.mantisrx.connector.iceberg.sink.committer.watermarks;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class PropertyAwareWatermarkExtractor implements WatermarkExtractor {

    private final String propertyKey;

    @Nullable
    @Override
    public Long getWatermark(Table table) {
        try {
            return Long.parseLong(table.properties().get(propertyKey));
        } catch (Exception e) {
            log.error("Failed to extract watermark from the table", e);
            return null;
        }
    }

    @Override
    public void setWatermark(Transaction transaction, Long watermark) {
        UpdateProperties updateProperties = transaction.updateProperties();
        updateProperties.set(propertyKey, Long.toString(watermark));
        updateProperties.commit();
        log.info("Iceberg committer for table={} set VTTS watermark to {}", transaction.table(),
            watermark);
    }
}
