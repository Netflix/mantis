package io.reactivex.mantis.network.push;

import rx.functions.Func1;

public interface RouterFactory {
    <T> Router<T> scalarStageToStageRouter(String name, final Func1<T, byte[]> toBytes);

    <T> ProactiveRouter<T> scalarStageToStageProactiveRouter(String name, final Func1<T, byte[]> toBytes);

    <K, V> ProactiveRouter<KeyValuePair<K, V>> keyedStageToStageProactiveRouter(String name, Func1<K, byte[]> keyEncoder, Func1<V, byte[]> valueEncoder);
}
