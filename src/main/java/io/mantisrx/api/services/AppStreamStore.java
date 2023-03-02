package io.mantisrx.api.services;

import com.google.common.collect.ImmutableList;
import io.mantisrx.discovery.proto.AppJobClustersMap;
import java.io.IOException;
import java.util.Collection;

/**
 * Interface to get streams associated with a given app or set of apps
 */
public interface AppStreamStore {
  default AppJobClustersMap getJobClusterMappings(String app) throws IOException {
    return getJobClusterMappings(ImmutableList.of(app));
  }

  AppJobClustersMap getJobClusterMappings(Collection<String> apps) throws IOException;
}
