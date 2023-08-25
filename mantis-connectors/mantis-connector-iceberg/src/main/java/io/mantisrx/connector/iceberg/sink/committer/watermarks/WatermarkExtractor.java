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
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

public interface WatermarkExtractor {

    @Nullable
    Long getWatermark(Table table);

    @Nullable
    default Long getWatermark(Transaction transaction) {
        return getWatermark(transaction.table());
    }

    void setWatermark(Transaction transaction, Long watermark);

    default void setWatermark(Table table, Long watermark) {
        setWatermark(table.newTransaction(), watermark);
    }

    static WatermarkExtractor noop() {
        return new WatermarkExtractor() {
            @Nullable
            @Override
            public Long getWatermark(Table table) {
                return null;
            }

            @Override
            public void setWatermark(Transaction transaction, Long watermark) {
            }
        };
    }

    static WatermarkExtractor propertiesAware(final String propertyKey) {
        return new PropertyAwareWatermarkExtractor(propertyKey);
    }
}
