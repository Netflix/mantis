package io.mantisrx.api;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
    public static final String numMessagesCounterName = "numSinkMessages";
    public static final String numDroppedMessagesCounterName = "numDroppedSinkMessages";
    public static final String numBytesCounterName = "numSinkBytes";
    public static final String numDroppedBytesCounterName = "numDroppedSinkBytes";

    public static final String SSE_DATA_SUFFIX = "\r\n\r\n";
    public static final String SSE_DATA_PREFIX = "data: ";

    public static final long TunnelPingIntervalSecs = 12;
    public static final String TunnelPingMessage = "MantisApiTunnelPing";
    public static final String TunnelPingParamName = "MantisApiTunnelPingEnabled";

    public static final String OriginRegionTagName = "originRegion";
    public static final String ClientIdTagName = "clientId";
    public static final String TagsParamName = "MantisApiTag";
    public static final String TagNameValDelimiter = ":";

    public static final String metaErrorMsgHeader = "mantis.meta.error.message";
    public static final String metaOriginName = "mantis.meta.origin";

    public static final String numRemoteBytesCounterName = "numRemoteSinkBytes";
    public static final String numRemoteMessagesCounterName = "numRemoteMessages";
    public static final String numSseErrorsCounterName = "numSseErrors";

    public static final String DUMMY_TIMER_DATA = "DUMMY_TIMER_DATA";

    public static final String MANTISAPI_CACHED_HEADER = "x-nflx-mantisapi-cached";
}
