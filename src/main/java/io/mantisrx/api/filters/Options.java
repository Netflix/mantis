package io.mantisrx.api.filters;

import com.netflix.zuul.filters.http.HttpSyncEndpoint;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import com.netflix.zuul.stats.status.StatusCategoryUtils;
import com.netflix.zuul.stats.status.ZuulStatusCategory;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public class Options extends HttpSyncEndpoint {

    @Override
    public HttpResponseMessage apply(HttpRequestMessage request) {
        HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, HttpResponseStatus.OK.code());
        resp.setBodyAsText("");
        StatusCategoryUtils.setStatusCategory(request.getContext(), ZuulStatusCategory.SUCCESS);
        return resp;
    }
}
