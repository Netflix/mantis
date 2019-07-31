package io.mantisrx.master.jobcluster;

import static io.mantisrx.master.jobcluster.JobClusterHelper.SystemLabels.*;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.mantisrx.common.Label;
import io.mantisrx.master.jobcluster.JobClusterHelper.SystemLabels;
import io.mantisrx.runtime.JobSla;
import io.mantisrx.runtime.MantisJobDurationType;
import io.mantisrx.runtime.command.InvalidJobException;
import io.mantisrx.server.master.domain.JobDefinition;
import org.junit.Test;


public class JobClusterHelperTest {
    @Test
    public void insertResubmitLabelTest() throws InvalidJobException {

        JobDefinition jobDefinition = generateJobDefinition("insertResubmitLabelTest", new ArrayList<>(),
                "art.zip", "1.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertAutoResubmitLabel(jobDefinition);
        assertEquals(1, updatedJobDefn.getLabels().size());
        Label label = updatedJobDefn.getLabels().get(0);
        assertEquals(MANTIS_IS_RESUBMIT_LABEL.name(), label.getName());
    }

    @Test
    public void doNotinsertResubmitLabelIfAlreadyExistsTest() throws InvalidJobException {
        List<Label> labels = new ArrayList<>();
        labels.add(new Label(MANTIS_IS_RESUBMIT_LABEL.name(), "true"));
        JobDefinition jobDefinition = generateJobDefinition("DoNotinsertResubmitLabelIfAlreadyExistsTest",
                labels, "art.zip", "1.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertAutoResubmitLabel(jobDefinition);
        assertEquals(1, updatedJobDefn.getLabels().size());
        Label label = updatedJobDefn.getLabels().get(0);
        assertEquals(MANTIS_IS_RESUBMIT_LABEL.name(), label.getName());
    }

    @Test
    public void insertArtifactLabelTest() throws InvalidJobException {
        String artifactName = "art.zip";
        JobDefinition jobDefinition = generateJobDefinition("insertResubmitLabelTest", new ArrayList<>(),
                artifactName, "1.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertSystemLabels(jobDefinition, false);
        assertEquals(2, updatedJobDefn.getLabels().size());

        List<Label> labels = updatedJobDefn.getLabels().stream().filter(
                label -> label.getName().equals(MANTIS_ARTIFACT_LABEL.name()))
                .collect(Collectors.toList());

        Label label = labels.get(0);
        assertEquals(MANTIS_ARTIFACT_LABEL.name(), label.getName());
        assertEquals(artifactName, label.getValue());
    }

    @Test
    public void replaceArtifactLabelTest() throws InvalidJobException {
        String artifactName = "art1.zip";
        List<Label> labels = new ArrayList<>();
        labels.add(new Label(MANTIS_ARTIFACT_LABEL.name(), "art0.zip"));
        JobDefinition jobDefinition = generateJobDefinition("replaceArtifactLabelTest", labels,
                artifactName, "1.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertSystemLabels(jobDefinition, false);
        assertEquals(2, updatedJobDefn.getLabels().size());

        labels = updatedJobDefn.getLabels().stream().filter(
                label -> label.getName().equals(MANTIS_ARTIFACT_LABEL.name()))
                .collect(Collectors.toList());

        Label label = labels.get(0);

        assertEquals(MANTIS_ARTIFACT_LABEL.name(), label.getName());
        assertEquals(artifactName, label.getValue());
    }

    @Test
    public void insertVersionLabelTest() throws InvalidJobException {
        String artifactName = "art.zip";
        JobDefinition jobDefinition = generateJobDefinition("insertVersionLabelTest", new ArrayList<>(),
                artifactName, "1.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertSystemLabels(jobDefinition, false);
        assertEquals(2, updatedJobDefn.getLabels().size());

        List<Label> labels = updatedJobDefn.getLabels().stream().filter(
                label -> label.getName().equals(MANTIS_VERSION_LABEL.name()))
                .collect(Collectors.toList());

        Label label = labels.get(0);
        assertEquals(MANTIS_VERSION_LABEL.name(), label.getName());
        assertEquals("1.0", label.getValue());
    }

    @Test
    public void replaceVersionLabelTest() throws InvalidJobException {
        String artifactName = "art1.zip";
        String v0 = "1.0";
        String v1 = "2.0";
        List<Label> labels = new ArrayList<>();
        labels.add(new Label(MANTIS_VERSION_LABEL.name(), v0));
        JobDefinition jobDefinition = generateJobDefinition("replaceVersionLabelTest", labels,
                artifactName, "2.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertSystemLabels(jobDefinition, false);
        assertEquals(2, updatedJobDefn.getLabels().size());

        labels = updatedJobDefn.getLabels().stream().filter(
                label -> label.getName().equals(MANTIS_VERSION_LABEL.name()))
                .collect(Collectors.toList());

        Label label = labels.get(0);

        assertEquals(MANTIS_VERSION_LABEL.name(), label.getName());
        assertEquals(v1, label.getValue());
    }

    @Test
    public void systemLabelTest() throws InvalidJobException {
        String artifactName = "art1.zip";
        List<Label> labels = new ArrayList<>();
        labels.add(new Label(MANTIS_ARTIFACT_LABEL.name(), "art0.zip"));
        JobDefinition jobDefinition = generateJobDefinition("systemLabelTest", labels,
                artifactName,"1.0");
        JobDefinition updatedJobDefn = JobClusterHelper.insertSystemLabels(jobDefinition, true);
        assertEquals(3, updatedJobDefn.getLabels().size());
        for(Label l : updatedJobDefn.getLabels()) {
            if(l.getName().equals(MANTIS_ARTIFACT_LABEL.name())) {
                assertEquals(artifactName, l.getValue());
            } else if (l.getName().equals(MANTIS_IS_RESUBMIT_LABEL.name())){
                assertEquals("true", l.getValue());
            } else {
                assertEquals("1.0", l.getValue());
            }
        }

    }

    JobDefinition generateJobDefinition(String name, List<Label> labelList, String artifactName, String version)
            throws InvalidJobException {
        return new JobDefinition.Builder()
                .withName(name)
                .withParameters(Lists.newArrayList())
                .withLabels(labelList)
                .withSchedulingInfo(JobClusterTest.SINGLE_WORKER_SCHED_INFO)
                .withArtifactName(artifactName)
                .withVersion(version)
                .withSubscriptionTimeoutSecs(1)
                .withUser("njoshi")
                .withJobSla(new JobSla(0, 0,
                        JobSla.StreamSLAType.Lossy, MantisJobDurationType.Transient, "userType"))

                .build();
    }
}
