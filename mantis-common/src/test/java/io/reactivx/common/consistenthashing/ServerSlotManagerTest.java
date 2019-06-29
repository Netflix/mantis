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

package io.reactivx.common.consistenthashing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.mantisrx.common.network.HashFunctions;
import io.mantisrx.common.network.ServerSlotManager;
import io.mantisrx.common.network.ServerSlotManager.SlotAssignmentManager;
import io.mantisrx.common.network.WritableEndpoint;
import org.junit.Test;


public class ServerSlotManagerTest {

    @Test
    public void oneNodeTest() {
        WritableEndpoint<Void> n1 = new WritableEndpoint<Void>("host1", 7001);
        Map<String, List<String>> params = new HashMap<String, List<String>>();

        ServerSlotManager<Void> ssm = new ServerSlotManager<Void>(HashFunctions.ketama());
        SlotAssignmentManager<Void> sam = ssm.registerServer(n1, params);

        int hostHitCountNode1 = 0;
        int nonHitCount = 0;

        int MSG_COUNT = 100000;
        for (int i = 0; i < MSG_COUNT; i++) {

            if (sam.filter(n1, ("msg:" + i).getBytes())) {
                hostHitCountNode1++;
            } else {
                nonHitCount++;
            }
        }
        assertTrue(nonHitCount == 0);
        assertEquals(MSG_COUNT, hostHitCountNode1);

        //cleanup
        ssm.deregisterServer(n1, params);
    }

    @Test
    public void twoNodeSameClientIdTest() {
        WritableEndpoint<Void> n1 = new WritableEndpoint<Void>("host1", 7001);
        WritableEndpoint<Void> n2 = new WritableEndpoint<Void>("host2", 7001);

        Map<String, List<String>> params = new HashMap<String, List<String>>();
        List<String> vals = new ArrayList<String>();
        vals.add("client1");
        params.put("clientId", vals);

        ServerSlotManager<Void> ssm = new ServerSlotManager<Void>(HashFunctions.ketama());
        SlotAssignmentManager<Void> sm = ssm.registerServer(n1, params);

        SlotAssignmentManager<Void> sm2 = ssm.registerServer(n2, params);

        assertEquals(sm, sm2);


        int hostHitCountNode1 = 0;
        int hostHitCountNode2 = 0;
        int nonHitCount = 0;

        int MSG_COUNT = 1000000;
        for (int i = 0; i < MSG_COUNT; i++) {
            String msg = "msg:" + i;
            if (sm.filter(n1, msg.getBytes())) {
                hostHitCountNode1++;
            } else if (sm2.filter(n2, msg.getBytes())) {
                hostHitCountNode2++;
            } else {
                nonHitCount++;
            }
        }

        double host1HitPercentage = (double) hostHitCountNode1 / (double) MSG_COUNT;
        System.out.println("host1 hit % " + host1HitPercentage);
        assertTrue(host1HitPercentage > 0.4 && host1HitPercentage < 0.6);

        assertTrue(nonHitCount == 0);
        assertEquals(MSG_COUNT, hostHitCountNode1 + hostHitCountNode2);


        ssm.deregisterServer(n1, params);
        ssm.deregisterServer(n2, params);

    }

    @Test
    public void threeNodeSameClientIdTest() {
        WritableEndpoint<Void> n1 = new WritableEndpoint<Void>("host1", 7001);
        WritableEndpoint<Void> n2 = new WritableEndpoint<Void>("host2", 7001);

        Map<String, List<String>> params = new HashMap<String, List<String>>();
        List<String> vals = new ArrayList<String>();
        vals.add("client1");
        params.put("clientId", vals);

        ServerSlotManager<Void> ssm = new ServerSlotManager<Void>(HashFunctions.ketama());

        SlotAssignmentManager<Void> sm = ssm.registerServer(n1, params);

        SlotAssignmentManager<Void> sm2 = ssm.registerServer(n2, params);

        assertEquals(sm, sm2);


        int hostHitCountNode1 = 0;
        int hostHitCountNode2 = 0;
        int nonHitCount = 0;

        int MSG_COUNT = 1000000;
        for (int i = 0; i < MSG_COUNT; i++) {
            String msg = "msg:" + i;
            if (sm.filter(n1, msg.getBytes())) {
                hostHitCountNode1++;
            } else if (sm2.filter(n2, msg.getBytes())) {
                hostHitCountNode2++;
            } else {
                nonHitCount++;
            }
        }

        double host1HitPercentage = (double) hostHitCountNode1 / (double) MSG_COUNT;
        System.out.println("host1 hit % " + host1HitPercentage);
        assertTrue(host1HitPercentage > 0.4 && host1HitPercentage < 0.6);

        assertTrue(nonHitCount == 0);
        assertEquals(MSG_COUNT, hostHitCountNode1 + hostHitCountNode2);


        WritableEndpoint<Void> n3 = new WritableEndpoint<Void>("host3", 7001);
        // add another node
        SlotAssignmentManager<Void> sm3 = ssm.registerServer(n3, params);

        hostHitCountNode1 = 0;
        hostHitCountNode2 = 0;
        int hostHitCountNode3 = 0;
        nonHitCount = 0;

        MSG_COUNT = 1000000;
        for (int i = 0; i < MSG_COUNT; i++) {
            String msg = "msg:" + i;
            if (sm.filter(n1, msg.getBytes())) {
                hostHitCountNode1++;
            }
            if (sm2.filter(n2, msg.getBytes())) {
                hostHitCountNode2++;
            }
            if (sm3.filter(n3, msg.getBytes())) {
                hostHitCountNode3++;
            } else {
                nonHitCount++;
            }
        }

        assertEquals(MSG_COUNT, hostHitCountNode1 + hostHitCountNode2 + hostHitCountNode3);

        ssm.deregisterServer(n1, params);
        ssm.deregisterServer(n2, params);
        ssm.deregisterServer(n3, params);
    }

    @Test
    public void twoNodeDifferentClientIdTest() {
        WritableEndpoint<Void> n1 = new WritableEndpoint<Void>("host1", 7001);
        WritableEndpoint<Void> n2 = new WritableEndpoint<Void>("host2", 7001);

        Map<String, List<String>> params = new HashMap<String, List<String>>();
        List<String> vals = new ArrayList<String>();
        vals.add("client1");
        params.put("clientId", vals);

        Map<String, List<String>> params2 = new HashMap<String, List<String>>();
        List<String> vals2 = new ArrayList<String>();
        vals2.add("client2");
        params2.put("clientId", vals2);

        ServerSlotManager<Void> ssm = new ServerSlotManager<Void>(HashFunctions.ketama());

        SlotAssignmentManager<Void> sm = ssm.registerServer(n1, params);

        SlotAssignmentManager<Void> sm2 = ssm.registerServer(n2, params2);

        assertFalse(sm.equals(sm2));

        int hostHitCountNode1 = 0;
        int hostHitCountNode2 = 0;
        int nonHitCount = 0;
        int MSG_COUNT = 1000000;

        for (int i = 0; i < MSG_COUNT; i++) {

            String msg = "msg:" + i;
            boolean atleastOneMatch = false;
            if (sm.filter(n1, msg.getBytes())) {
                hostHitCountNode1++;
                atleastOneMatch = true;
            }
            if (sm2.filter(n2, msg.getBytes())) {
                hostHitCountNode2++;
                atleastOneMatch = true;
            }

            if (!atleastOneMatch) {
                nonHitCount++;
            }
        }

        assertTrue(nonHitCount == 0);
        assertEquals(MSG_COUNT, hostHitCountNode1);
        assertEquals(MSG_COUNT, hostHitCountNode2);

        ssm.deregisterServer(n1, params);
        ssm.deregisterServer(n2, params2);

    }

    @Test
    public void twoSameOneDifferentClientIdTest() {
        WritableEndpoint<Void> n1 = new WritableEndpoint<Void>("host1", 7001);
        WritableEndpoint<Void> n2 = new WritableEndpoint<Void>("host2", 7001);
        WritableEndpoint<Void> n3 = new WritableEndpoint<Void>("host3", 7001);

        Map<String, List<String>> params = new HashMap<String, List<String>>();
        List<String> vals = new ArrayList<String>();
        vals.add("client1");
        params.put("clientId", vals);

        Map<String, List<String>> params3 = new HashMap<String, List<String>>();
        List<String> vals3 = new ArrayList<String>();
        vals3.add("client3");
        params3.put("clientId", vals3);

        ServerSlotManager<Void> ssm = new ServerSlotManager<Void>(HashFunctions.ketama());

        SlotAssignmentManager<Void> sm = ssm.registerServer(n1, params);

        SlotAssignmentManager<Void> sm2 = ssm.registerServer(n2, params);

        SlotAssignmentManager<Void> sm3 = ssm.registerServer(n3, params3);

        assertFalse(sm.equals(sm3));

        assertTrue(sm.equals(sm2));

        int hostHitCountNode1 = 0;
        int hostHitCountNode2 = 0;
        int hostHitCountNode3 = 0;
        int nonHitCount = 0;
        int MSG_COUNT = 1000000;

        for (int i = 0; i < MSG_COUNT; i++) {
            boolean atleastOneMatch = false;
            String msg = "msg:" + i;

            if (sm.filter(n1, msg.getBytes())) {
                hostHitCountNode1++;
                atleastOneMatch = true;
            }
            if (sm2.filter(n2, msg.getBytes())) {
                hostHitCountNode2++;
                atleastOneMatch = true;
            }

            if (sm3.filter(n3, msg.getBytes())) {
                hostHitCountNode3++;
                atleastOneMatch = true;
            }

            if (!atleastOneMatch) {
                nonHitCount++;
            }
        }

        assertTrue(nonHitCount == 0);
        assertEquals(MSG_COUNT, hostHitCountNode1 + hostHitCountNode2);
        assertEquals(MSG_COUNT, hostHitCountNode3);

        ssm.deregisterServer(n1, params);
        ssm.deregisterServer(n2, params);
        ssm.deregisterServer(n3, params3);

    }

}
