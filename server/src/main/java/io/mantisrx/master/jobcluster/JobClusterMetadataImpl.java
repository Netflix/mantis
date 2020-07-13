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

package io.mantisrx.master.jobcluster;

import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonCreator;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mantisrx.shaded.com.fasterxml.jackson.annotation.JsonProperty;


import io.mantisrx.runtime.JobOwner;
import io.mantisrx.runtime.WorkerMigrationConfig;
import io.mantisrx.server.master.domain.IJobClusterDefinition;
import io.mantisrx.server.master.domain.JobClusterDefinitionImpl;
import io.mantisrx.server.master.domain.JobId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * JobCluster
-------------

String name,
JobOwner owner,
SLA {
	int slaMin,
	int slaMax,
	String cronSpec,
	CronPolicy policy,
}
WorkerMigrationConfig config,
boolea readyForJobMaster,
long lastJobCount
boolean isdisabled

jobDefinitions [{
	
	String artifactName,
	String version,
	long uploadedAt,


	SchedulingInfo: [{
                        "1": {
                            "numberOfInstances": 15,
                            "machineDefinition": {
                                "cpuCores": 4,
                                "memoryMB": 12024,
                                "networkMbps": 512,
                                "diskMB": 10024,
                                "numPorts": 1
                            },
                            "hardConstraints": [],
                            "softConstraints": [],
                            "scalingPolicy": null,
                            "scalable": false
                        }, {
                        "2" : {
							"numberOfInstances": 15,
                            "machineDefinition": {
                                "cpuCores": 4,
                                "memoryMB": 12024,
                                "networkMbps": 512,
                                "diskMB": 10024,
                                "numPorts": 1
                            },
                            "hardConstraints": [],
                            "softConstraints": [],
                            "scalingPolicy": null,
                            "scalable": false
                        	}
                        }
                    }],
    "parameters": [
            {
                "name": "enableCompressedBinaryInput",
                "value": "True"
            },
            {
                "name": "targetApp",
                "value": "^apiproxy.*"
            }
        ],
    }]
    

}

 * @author njoshi
 *
 */
public class JobClusterMetadataImpl implements IJobClusterMetadata {

	final private IJobClusterDefinition jobClusterDefinition;
	final private long lastJobCount;
	final private boolean disabled;

	@JsonCreator
	@JsonIgnoreProperties(ignoreUnknown=true)
	public JobClusterMetadataImpl(@JsonProperty("jobClusterDefinition") JobClusterDefinitionImpl jcDefn,
			@JsonProperty("lastJobCount") long lastJobCount,
			@JsonProperty("disabled") boolean disabled) {
		this.jobClusterDefinition = jcDefn;
		this.lastJobCount = lastJobCount;
		this.disabled = disabled;
	}

	/* (non-Javadoc)
	 * @see io.mantisrx.master.jobcluster.IJobClusterMetadata#getJobClusterDefinition()
	 */
	@Override
	public IJobClusterDefinition getJobClusterDefinition() {
		return jobClusterDefinition;
	}

	/* (non-Javadoc)
	 * @see io.mantisrx.master.jobcluster.IJobClusterMetadata#getLastJobCount()
	 */
	@Override
	public long getLastJobCount() {
		return lastJobCount;
	}

	/* (non-Javadoc)
	 * @see io.mantisrx.master.jobcluster.IJobClusterMetadata#isDisabled()
	 */
	@Override
	public boolean isDisabled() {
		return disabled;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		JobClusterMetadataImpl that = (JobClusterMetadataImpl) o;
		return lastJobCount == that.lastJobCount &&
				disabled == that.disabled &&
				Objects.equals(jobClusterDefinition, that.jobClusterDefinition);
	}

	@Override
	public int hashCode() {

		return Objects.hash(jobClusterDefinition, lastJobCount, disabled);
	}


	@Override
	public String toString() {
		return "JobClusterMetadataImpl [jobClusterDefinition=" + jobClusterDefinition + ", lastJobCount=" + lastJobCount
				+ ", disabled=" + disabled + "]";
	}

	public static class Builder {
		private JobClusterDefinitionImpl jobClusterDefinition;
		private long lastJobCount = 0;
		private boolean disabled;

		public Builder() {}
		
		public Builder withJobClusterDefinition(JobClusterDefinitionImpl jobClusterDef) {
			this.jobClusterDefinition = jobClusterDef;
			return this;
		}
		
		public Builder withLastJobCount(long lastJobCnt) {
			this.lastJobCount = lastJobCnt;
			return this;
		}
		
		public Builder withIsDisabled(boolean disabled) {
			this.disabled = disabled;
			return this;
		}

		public IJobClusterMetadata build() {
			return new JobClusterMetadataImpl(this.jobClusterDefinition, this.lastJobCount, this.disabled);
		}
		
		public IJobClusterMetadata build(JobClusterDefinitionImpl def, long lastJobCnt, boolean isDisabled) {
			return new JobClusterMetadataImpl(def, lastJobCount, isDisabled);
		}
	}
	
	
}
