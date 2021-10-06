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

package io.mantisrx.server.master.mesos;

import com.netflix.fenzo.VirtualMachineLease;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value;


public class VirtualMachineLeaseMesosImpl implements VirtualMachineLease {

    private Offer offer;
    private double cpuCores;
    private double memoryMB;
    private double networkMbps = 0.0;
    private double diskMB;
    private String hostname;
    private String vmID;
    private List<Range> portRanges;
    private Map<String, Protos.Attribute> attributeMap;

    private long offeredTime;

    public VirtualMachineLeaseMesosImpl(Offer offer) {
        this.offer = offer;
        hostname = offer.getHostname();
        this.vmID = offer.getSlaveId().getValue();
        offeredTime = System.currentTimeMillis();
        // parse out resources from offer
        // We expect network bandwidth to be coming in as a consumable scalar resource with the name "network"
        for (Resource resource : offer.getResourcesList()) {
            if ("cpus".equals(resource.getName())) {
                cpuCores = resource.getScalar().getValue();
            } else if ("mem".equals(resource.getName())) {
                memoryMB = resource.getScalar().getValue();
            } else if ("disk".equals(resource.getName())) {
                diskMB = resource.getScalar().getValue();
            } else if ("network".equals(resource.getName())) {
                networkMbps = resource.getScalar().getValue();
            } else if ("ports".equals(resource.getName())) {
                portRanges = new ArrayList<>();
                for (Value.Range range : resource.getRanges().getRangeList()) {
                    portRanges.add(new Range((int) range.getBegin(), (int) range.getEnd()));
                }
            }
        }
        attributeMap = new HashMap<>();
        if (offer.getAttributesCount() > 0) {
            for (Protos.Attribute attribute : offer.getAttributesList()) {
                attributeMap.put(attribute.getName(), attribute);
            }
        }
    }

    @Override
    public String hostname() {
        return hostname;
    }

    @Override
    public String getVMID() {
        return vmID;
    }

    @Override
    public double cpuCores() {
        return cpuCores;
    }

    @Override
    public double memoryMB() {
        return memoryMB;
    }

    @Override
    public double networkMbps() {
        return networkMbps;
    }

    @Override
    public double diskMB() {
        return diskMB;
    }

    public Offer getOffer() {
        return offer;
    }

    @Override
    public String getId() {
        return offer.getId().getValue();
    }

    @Override
    public long getOfferedTime() {
        return offeredTime;
    }

    @Override
    public List<Range> portRanges() {
        return portRanges;
    }

    @Override
    public Map<String, Protos.Attribute> getAttributeMap() {
        return attributeMap;
    }

    @Override
    public Double getScalarValue(String name) {
        return null;
    }

    @Override
    public Map<String, Double> getScalarValues() {
        return Collections.emptyMap();
    }
}
