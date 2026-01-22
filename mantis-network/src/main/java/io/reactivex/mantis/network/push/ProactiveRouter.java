package io.reactivex.mantis.network.push;

import io.mantisrx.common.metrics.Metrics;

import java.util.List;

public interface ProactiveRouter<T> {
    void route(List<T> chunks);

    void addConnection(AsyncConnection<T> connection);

    void removeConnection(AsyncConnection<T> connection);

    Metrics getMetrics();
}
