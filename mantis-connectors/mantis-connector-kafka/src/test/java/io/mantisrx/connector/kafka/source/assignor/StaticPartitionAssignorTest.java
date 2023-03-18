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

package io.mantisrx.connector.kafka.source.assignor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StaticPartitionAssignorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticPartitionAssignorTest.class);

    @Test
    public void testStaticAssign1() {
        Map<String, Integer> topicPartitionCounts = generateTopicPartitionCounts(15, 2);
        LOGGER.info("TopicPartitionMap {}", topicPartitionCounts);
        int totalNumConsumers = 20;

        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();
        for (int i = 0; i < totalNumConsumers; i++) {
            List<TopicPartition> assignedPartitions = partitionAssigner.assignPartitionsToConsumer(i, topicPartitionCounts, totalNumConsumers);
            assertTrue(assignedPartitions.size() >= 1 && assignedPartitions.size() <= 2);
            LOGGER.info("Consumer[{}] -> {}", i, assignedPartitions);
        }

    }

    @Test
    public void testStaticAssignMoreConsumersThanPartitions() {
        Map<String, Integer> topicPartitionCounts = generateTopicPartitionCounts(15, 2);

        LOGGER.info("TopicPartitionMap {}", topicPartitionCounts);

        int totalNumConsumers = 40;

        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();
        for (int i = 0; i < totalNumConsumers; i++) {
            List<TopicPartition> assignedPartitions = partitionAssigner.assignPartitionsToConsumer(i, topicPartitionCounts, totalNumConsumers);
            //  assertTrue(assignedPartitions.size() >= 1 && assignedPartitions.size() <= 2);
            LOGGER.info("Consumer[{}] -> {}", i, assignedPartitions);
        }

    }

    private Map<String, Integer> generateTopicPartitionCounts(int numTopics, int partitionRange) {

        Map<String, Integer> topicPartitionMap = new HashMap<>();
        int partitionCnt = 1;
        for (int i = 0; i < numTopics; i++) {
            topicPartitionMap.put("topic_" + i, partitionCnt++);
            if (partitionCnt == partitionRange + 1) {
                partitionCnt = 1;
            }
        }
        return topicPartitionMap;
    }


    @Test
    public void testStaticAssign2() {
        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put("topic_0", 1400);

        LOGGER.info("TopicPartitionMap {}", topicPartitionCounts);

        int totalNumConsumers = 20;

        Map<String, List<TopicPartition>> assignmentMap = new HashMap<>();
        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();
        for (int i = 0; i < totalNumConsumers; i++) {
            List<TopicPartition> assignedPartitions = partitionAssigner.assignPartitionsToConsumer(i, topicPartitionCounts, totalNumConsumers);
            assertEquals(70, assignedPartitions.size());
            assignmentMap.put("" + i, assignedPartitions);
            LOGGER.info("Consumer[{}] -> {}", i, assignedPartitions);
        }
    }

    @Test
    public void testStaticAssign3() {
        String tpList = "testtopic:1,testtopic2:7,testtopic3:1,testtopic4:46";
        Map<String, Integer> tpMap = new HashMap<>();
        String[] topicPartitionTuples = tpList.split(",");
        for (int i = 0; i < topicPartitionTuples.length; i++) {
            String[] topPart = topicPartitionTuples[i].split(":");
            tpMap.put(topPart[0], Integer.valueOf(topPart[1]));

        }

        int totalNumConsumers = 12;
        Map<String, List<TopicPartition>> assignmentMap = new HashMap<>();
        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();
        for (int i = 0; i < totalNumConsumers; i++) {
            List<TopicPartition> assignedPartitions = partitionAssigner.assignPartitionsToConsumer(i, tpMap, totalNumConsumers);
            //  assertEquals(70, assignedPartitions.size());
            assignmentMap.put("" + i, assignedPartitions);
            LOGGER.info("Consumer[{}] -> {}", i, assignedPartitions);
        }
    }

    @Test
    public void invalidConsumerIndexTest() {
        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put("topic_0", 1400);

        LOGGER.info("TopicPartitionMap {}", topicPartitionCounts);

        int totalNumConsumers = 20;

        Map<String, List<TopicPartition>> assignmentMap = new HashMap<>();
        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();

        try {
            partitionAssigner.assignPartitionsToConsumer(-1, topicPartitionCounts, totalNumConsumers);
            fail();
        } catch (IllegalArgumentException e) {

        }


        try {
            partitionAssigner.assignPartitionsToConsumer(100, topicPartitionCounts, totalNumConsumers);
            fail();
        } catch (IllegalArgumentException e) {

        }

    }

    @Test
    public void invalidTotalConsumersTest() {
        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put("topic_0", 1400);

        LOGGER.info("TopicPartitionMap {}", topicPartitionCounts);

        Map<String, List<TopicPartition>> assignmentMap = new HashMap<>();
        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();

        try {
            int totalNumConsumers = -1;
            partitionAssigner.assignPartitionsToConsumer(1, topicPartitionCounts, totalNumConsumers);
            fail();
        } catch (IllegalArgumentException e) {

        }

    }

    @Test
    public void invalidTopicPartitionMapTest() {
        Map<String, Integer> topicPartitionCounts = null;

        StaticPartitionAssignor partitionAssigner = new StaticPartitionAssignorImpl();

        try {
            partitionAssigner.assignPartitionsToConsumer(1, topicPartitionCounts, 20);
            fail();
        } catch (NullPointerException e) {

        }
    }

}
