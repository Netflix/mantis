package io.mantisrx.api.services.artifacts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.mantisrx.api.proto.Artifact;


public class InMemoryArtifactManager implements ArtifactManager {
    private Map<String, Artifact> artifacts = new HashMap<>();

    @Override
    public List<String> getArtifacts() {
        return artifacts
                .values()
                .stream()
                .map(Artifact::getFileName)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Artifact> getArtifact(String name) {
        return artifacts
                .values()
                .stream()
                .filter(artifact -> artifact.getFileName().equals(name))
                .findFirst();
    }

    @Override
    public void deleteArtifact(String name) {
        this.artifacts.remove(name);
    }

    @Override
    public void putArtifact(Artifact artifact) {
        this.artifacts.put(artifact.getFileName(), artifact);
    }
}
