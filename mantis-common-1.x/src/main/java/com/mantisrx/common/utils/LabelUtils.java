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

import java.util.ArrayList;
import java.util.List;

import io.mantisrx.common.Label;


public class LabelUtils {

    public static final String OR_OPERAND = "or";
    public static final String AND_OPERAND = "and";
    public static final String LABEL_QUERY_PARAM = "labels";
    public static final String LABEL_OP_QUERY_PARAM = "labels.op";

    public static List<Label> generatePairs(String tagQuery) {
        //name1=value1,name2=value2
        List<Label> pairList = new ArrayList<>();
        if (tagQuery != null) {
            String[] toks = tagQuery.split(",");
            if (toks != null && toks.length > 0) {
                for (int i = 0; i < toks.length; i++) {
                    String token = toks[i];
                    String[] pairToks = token.split("=");
                    if (pairToks != null && pairToks.length == 2) {
                        pairList.add(new Label(pairToks[0], pairToks[1]));
                    }
                }
            }
        }
        return pairList;
    }

    public static boolean allPairsPresent(List<Label> expectedTags, List<Label> actualTags) {
        for (Label expectedTag : expectedTags) {
            if (!actualTags.contains(expectedTag)) {
                return false;
            }
        }
        return true;

    }

    public static boolean somePairsPresent(List<Label> expectedTags, List<Label> actualTags) {
        for (Label expectedTag : expectedTags) {
            if (actualTags.contains(expectedTag)) {
                return true;
            }
        }
        return false;
    }


}