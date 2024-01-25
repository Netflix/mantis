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

package io.mantisrx.runtime.loader.cgroups;

import io.mantisrx.runtime.loader.config.Usage.UsageBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * Implementation uses ideas from <a href="https://github.com/python-diamond/Diamond/blob/master/src/collectors/network/network.py">the diamond project</a>'s network metrics collector.
 */
@RequiredArgsConstructor
class NetworkSubsystemProcess implements SubsystemProcess {

    private final String fileName;
    private final String device;

    private Map<String, NetworkStats> getDeviceLevelStats() throws IOException {
        Map<String, NetworkStats> result = new HashMap<>();
        try (ProcFileReader reader = new ProcFileReader(Files.newInputStream(Paths.get(fileName)))) {

            // consume header
            reader.finishLine();
            reader.finishLine();
            while (reader.hasMoreData()) {
                String iface = reader.nextString();
                if (iface.isEmpty()) {
                    break;
                }

                // always include snapshot values
                long rxBytes = reader.nextLong();
                long rxPackets = reader.nextLong();
                reader.nextLong(); // errs skip
                reader.nextLong(); // drop skip
                reader.nextLong(); // fifo skip
                reader.nextLong(); // frame skip
                reader.nextLong(); // compressed skip
                reader.nextLong(); // multicast skip
                long txBytes = reader.nextLong();
                long txPackets = reader.nextLong();

                result.put(iface, new NetworkStats(rxBytes, rxPackets, txBytes, txPackets));
                reader.finishLine();
            }
        } catch (IOException e) {
            throw e;
        } catch (NullPointerException | NumberFormatException e) {
            throw new IllegalStateException("problem parsing stats: " + e);
        }

        return result;
    }

    @Override
    public void getUsage(UsageBuilder usageBuilder) throws IOException {
        Map<String, NetworkStats> result = getDeviceLevelStats();
        NetworkStats stats = result.get(device);
        usageBuilder.networkReadBytes(stats.getRxBytes());
        usageBuilder.networkWriteBytes(stats.getTxBytes());
    }

    @Value
    private class NetworkStats {
        long rxBytes;
        long rxPackets;
        long txBytes;
        long txPackets;
    }
}
