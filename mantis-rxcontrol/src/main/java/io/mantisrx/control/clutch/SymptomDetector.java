/*
 * Copyright 2024 Netflix, Inc.
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

package io.mantisrx.control.clutch;

import com.yahoo.sketches.quantiles.DoublesSketch;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import java.util.List;
import java.util.function.Predicate;

/**
 * TODO: This is completely experimental, not currently in use.
 */
public class SymptomDetector {

    private final Set<Tuple2<String, Predicate<List<? extends DoublesSketch>>>> symptoms = HashSet.empty();

    public void registerDector(String symptom, Predicate<List<? extends DoublesSketch>> pred) {
        symptoms.add(Tuple.of(symptom, pred));
    }

    public Set<String> getSymptoms(List<? extends DoublesSketch> observations) {
        return symptoms
                .filter(tup -> tup._2.test(observations))
                .map(tup -> tup._1);
    }
}
