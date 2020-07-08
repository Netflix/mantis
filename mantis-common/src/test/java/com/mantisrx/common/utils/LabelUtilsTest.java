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

package com.mantisrx.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import io.mantisrx.common.Label;
import io.mantisrx.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;


public class LabelUtilsTest {

    @Test
    public void testGenerate3Pairs() {

        String tagQuery = "k1=v1,k2=v2,k3=v3";

        List<Label> pairs = LabelUtils.generatePairs(tagQuery);

        assertEquals(3, pairs.size());

        assertTrue(pairs.contains(new Label("k1", "v1")));
        assertTrue(pairs.contains(new Label("k2", "v2")));
        assertTrue(pairs.contains(new Label("k3", "v3")));
    }

    @Test
    public void testGenerate1Pair() {

        String tagQuery = "k1=v1";

        List<Label> pairs = LabelUtils.generatePairs(tagQuery);

        assertEquals(1, pairs.size());
        assertTrue(pairs.contains(new Label("k1", "v1")));

    }

    @Test
    public void testGeneratePairNull() {

        String tagQuery = null;

        List<Label> pairs = LabelUtils.generatePairs(tagQuery);

        assertEquals(0, pairs.size());

    }

    @Test
    public void testGeneratePairMalformed() {

        String tagQuery = "k1=";

        List<Label> pairs = LabelUtils.generatePairs(tagQuery);

        assertEquals(0, pairs.size());

    }

    @Test
    public void testGeneratePairPartialMalformed() {

        String tagQuery = "k1=,k2=v2";

        List<Label> pairs = LabelUtils.generatePairs(tagQuery);

        assertEquals(1, pairs.size());

        assertEquals(new Label("k2", "v2"), pairs.get(0));

    }

    @Test
    public void testAllPairsPresent() {
        String tagQuery = "k1=v1,k2=v2,k3=v3";

        List<Label> expectedPairs = LabelUtils.generatePairs(tagQuery);

        List<Label> actualPairs = new ArrayList<>();

        actualPairs.add(new Label("k1", "v1"));
        actualPairs.add(new Label("k2", "v2"));
        actualPairs.add(new Label("k3", "v3"));

        assertTrue(LabelUtils.allPairsPresent(expectedPairs, actualPairs));


    }

    @Test
    public void testAllPairsPresent2() {
        String tagQuery = "k1=v1,k2=v2,k3=v3";

        List<Label> expectedPairs = LabelUtils.generatePairs(tagQuery);

        List<Label> actualPairs = new ArrayList<>();

        actualPairs.add(new Label("k1", "v1"));
        actualPairs.add(new Label("k2", "v2"));


        assertFalse(LabelUtils.allPairsPresent(expectedPairs, actualPairs));


    }

    @Test
    public void testSomePairsPresent() {
        String tagQuery = "k1=v1,k2=v2,k3=v3";

        List<Label> expectedPairs = LabelUtils.generatePairs(tagQuery);

        List<Label> actualPairs = new ArrayList<>();

        actualPairs.add(new Label("k1", "v1"));
        actualPairs.add(new Label("k2", "v2"));


        assertTrue(LabelUtils.somePairsPresent(expectedPairs, actualPairs));


    }

    @Test
    public void testSomePairsPresent2() {
        String tagQuery = "k1=v1,k2=v2,k3=v3";

        List<Label> expectedPairs = LabelUtils.generatePairs(tagQuery);

        List<Label> actualPairs = new ArrayList<>();

        actualPairs.add(new Label("k4", "v1"));
        actualPairs.add(new Label("k5", "v2"));


        assertFalse(LabelUtils.somePairsPresent(expectedPairs, actualPairs));


    }

    @Test
    public void testSomePairsPresent3() {
        String tagQuery = "";

        List<Label> expectedPairs = LabelUtils.generatePairs(tagQuery);

        List<Label> actualPairs = new ArrayList<>();

        actualPairs.add(new Label("k4", "v1"));
        actualPairs.add(new Label("k5", "v2"));


        assertFalse(LabelUtils.somePairsPresent(expectedPairs, actualPairs));


    }

    @Test
    public void testSerDe() {
        Label l = new Label("k1", "v1");
        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println(mapper.writeValueAsString(l));
        } catch (JsonProcessingException e) {
            fail();
        }
    }


}