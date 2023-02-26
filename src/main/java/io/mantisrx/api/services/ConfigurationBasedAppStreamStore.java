package io.mantisrx.api.services;

import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.common.JsonSerializer;
import io.mantisrx.discovery.proto.AppJobClustersMap;
import io.mantisrx.shaded.org.apache.curator.framework.listen.Listenable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("unused")
@Slf4j
public class ConfigurationBasedAppStreamStore implements AppStreamStore {

  private final JsonSerializer jsonSerializer;

  private final AtomicReference<AppJobClustersMap> appJobClusterMappings = new AtomicReference<>();

  private final Counter appJobClusterMappingNullCount;
  private final Counter appJobClusterMappingFailCount;
  private final Counter appJobClusterMappingRequestCount;

  public ConfigurationBasedAppStreamStore(ConfigSource configSource) {
    configSource.getListenable()
        .addListener((newConfig) -> updateAppJobClustersMapping(newConfig));
    this.jsonSerializer = new JsonSerializer();
    updateAppJobClustersMapping(configSource.get());

    this.appJobClusterMappingNullCount = SpectatorUtils.newCounter(
        "appJobClusterMappingNull", "mantisapi");
    this.appJobClusterMappingRequestCount = SpectatorUtils.newCounter(
        "appJobClusterMappingRequest", "mantisapi", "app", "unknown");
    this.appJobClusterMappingFailCount = SpectatorUtils.newCounter(
        "appJobClusterMappingFail", "mantisapi");
  }

  @Override
  public AppJobClustersMap getJobClusterMappings(Collection<String> apps) throws IOException {
    return getAppJobClustersMap(apps, this.appJobClusterMappings.get());
  }

  private AppJobClustersMap getAppJobClustersMap(Collection<String> appNames,
      @Nullable AppJobClustersMap appJobClustersMap) throws IOException {

    if (appJobClustersMap != null) {
      final AppJobClustersMap appJobClusters;
      if (appNames.size() > 0) {
        appJobClusters = appJobClustersMap.getFilteredAppJobClustersMap(new ArrayList<>(appNames));
      } else {
        appJobClusterMappingRequestCount.increment();
        appJobClusters = appJobClustersMap;
      }
      return appJobClusters;
    } else {
      appJobClusterMappingNullCount.increment();
      throw new IOException("AppJobClustersMap is null");
    }
  }

  private void updateAppJobClustersMapping(String appJobClusterStr) {
    try {
      AppJobClustersMap appJobClustersMap =
          jsonSerializer.fromJSON(appJobClusterStr, AppJobClustersMap.class);
      log.info("appJobClustersMap updated to {}", appJobClustersMap);
      appJobClusterMappings.set(appJobClustersMap);
    } catch (Exception ioe) {
      log.error("failed to update appJobClustersMap on Property update {}", appJobClusterStr, ioe);
      appJobClusterMappingFailCount.increment();
    }
  }

  public interface ConfigSource extends Supplier<String> {

    Listenable<ConfigurationChangeListener> getListenable();
  }

  public interface ConfigurationChangeListener {

    void onConfigChange(String config);
  }
}
