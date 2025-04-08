package io.reactivex.mantis.network.push;

import rx.functions.Func1;

public interface RouterFactory {
    public <T> Router<T> scalarStageToStageRouter(String name, final Func1<T, byte[]> toBytes);
}
