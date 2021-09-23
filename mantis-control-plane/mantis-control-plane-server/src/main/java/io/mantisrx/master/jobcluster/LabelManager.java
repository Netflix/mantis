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

import io.mantisrx.common.Label;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.server.master.domain.JobDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LabelManager {

    public enum SystemLabels {
        MANTIS_IS_RESUBMIT_LABEL("_mantis.isResubmit"),
        MANTIS_ARTIFACT_LABEL("_mantis.artifact"),
        MANTIS_VERSION_LABEL("_mantis.version"),
        MANTIS_SUBMITTER_LABEL("_mantis.submitter"),
        MANTIS_OWNER_EMAIL_LABEL("_mantis.ownerEmail"),
        MANTIS_CRITIALITY_LABEL("_mantis.criticality"),
        MANTIS_DATA_ORIGIN_LABEL("_mantis.dataOrigin"),
        MANTIS_JOB_TYPE_LABEL("_mantis.jobType");
        public final String label;
        SystemLabels(String s) {
            this.label = s;
        }
    };


    private static final Logger logger = LoggerFactory.getLogger(LabelManager.class);


    static int numberOfMandatoryLabels() {
        return 2;
    }
    static JobDefinition insertSystemLabels(JobDefinition resolvedJobDefn, boolean autoResubmit) {
        JobDefinition updatedJobDefn = resolvedJobDefn;
        if(autoResubmit) {
            updatedJobDefn = insertAutoResubmitLabel(resolvedJobDefn);
        }
        String artifactName = updatedJobDefn.getArtifactName();
        String version = updatedJobDefn.getVersion();

        List<Label> labels = updatedJobDefn.getLabels();

        // remove old artifact & version label if present.
        List<Label> updatedLabels = labels.stream()
                .filter(label -> !(label.getName().equals(SystemLabels.MANTIS_ARTIFACT_LABEL.label)))
                .filter(label -> !label.getName().equals(SystemLabels.MANTIS_VERSION_LABEL.label))
                .collect(Collectors.toList());

        updatedLabels.add(new Label(SystemLabels.MANTIS_ARTIFACT_LABEL.label, artifactName));

        updatedLabels.add(new Label(SystemLabels.MANTIS_VERSION_LABEL.label, version));

        try {
            updatedJobDefn = new JobDefinition.Builder().from(updatedJobDefn)
                    .withLabels(updatedLabels).build();

            return updatedJobDefn;
        } catch (InvalidJobException e) {
            logger.error(e.getMessage());
            return resolvedJobDefn;
        }
    }

     static JobDefinition insertAutoResubmitLabel(JobDefinition resolvedJobDefn) {
        List<Label> labels = resolvedJobDefn.getLabels();

        boolean alreadyHasResubmitLabel = labels.stream().anyMatch(
                label -> label.getName().equals(SystemLabels.MANTIS_IS_RESUBMIT_LABEL.label));

        if(!alreadyHasResubmitLabel) {
            List<Label> updatedLabels = new ArrayList<>(labels);
            updatedLabels.add(new Label(SystemLabels.MANTIS_IS_RESUBMIT_LABEL.label, "true"));
            try {
                JobDefinition updatedJobDefn = new JobDefinition.Builder().from(resolvedJobDefn)
                        .withLabels(updatedLabels).build();
                logger.debug("Added isResubmit label");
                return updatedJobDefn;
            } catch (InvalidJobException e) {
                logger.error(e.getMessage());
                return resolvedJobDefn;
            }
        } else {
            logger.debug("Job " + resolvedJobDefn.getName() + " already has isResubmit label. Don't add new");
            return resolvedJobDefn;
        }

    }

}
