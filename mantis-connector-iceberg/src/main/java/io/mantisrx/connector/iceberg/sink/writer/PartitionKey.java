/*
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
 */

package io.mantisrx.connector.iceberg.sink.writer;

/*
public class PartitionKey implements Record, StructLike {


    private final StructType struct;
    private final PartitionSpec spec;
    private final int size;
    private final Map<String, Integer> nameToPos;
    private final Object[] values;

    @Override
    public StructType struct() {
        return struct;
    }

    @Override
    public Object getField(String name) {
        Integer pos = nameToPos.get(name);
        if (pos != null) {
            return values[pos];
        }

        return null;
    }

    @Override
    public void setField(String name, Object value) {

    }

    @Override
    public Object get(int pos) {
        return null;
    }

    @Override
    public Record copy() {
        return null;
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return null;
    }

    @Override
    public <T> void set(int pos, T value) {

    }
}


 */
