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

package com.netflix.mantis.examples.mantispublishsample.web.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import io.mantisrx.publish.api.Event;
import io.mantisrx.publish.api.EventPublisher;
import io.mantisrx.publish.api.PublishStatus;
import lombok.extern.slf4j.Slf4j;


/**
 * A sample filter that captures Request and Response headers and sends them to
 * Mantis using the mantis-publish library.
 */

@Slf4j
@Singleton
public class CaptureRequestEventFilter implements Filter {

    private static final String RESPONSE_HEADER_PREFIX = "response.header.";
    private static final String REQUEST_HEADER_PREFIX = "request.header.";
    private static final String VALUE_SEPARATOR = ",";

    @Inject
    private EventPublisher publisher;


    @Override
    public void init(FilterConfig filterConfig) {
        log.info("Capture Request data filter inited");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        final HttpServletRequest req = (HttpServletRequest) servletRequest;
        final HttpServletResponse res = (HttpServletResponse)servletResponse;
        log.debug("In do filter");
        final long startMillis = System.currentTimeMillis();
        // Add a wrapper around the Response object to capture headers.
        final ResponseSpy responseSpy = new ResponseSpy(res);
        // Send request down the filter chain
        filterChain.doFilter(servletRequest,responseSpy);

        // request is complete now gather all the request data and send to mantis.
        processPostFilter(startMillis, req, responseSpy);

    }

    /**
     * Invoked after the request has been completed. Used to gather all the request and response headers
     * associated with this request and publish to mantis.
     * @param startMillis The time processing began for this request.
     * @param req The servlet request object
     * @param responseSpy The spy servlet response.
     */
    private void processPostFilter(long startMillis, HttpServletRequest req, ResponseSpy responseSpy) {
        try {
            Map<String, Object> event = new HashMap<>();
            postProcess(req, responseSpy,event);
            Event rEvent = new Event(event);
            final long duration = System.currentTimeMillis() - startMillis;
            rEvent.set("duration", duration);
            log.info("sending event {} to stream {}", rEvent);
            CompletionStage<PublishStatus> sendResult = publisher.publish(rEvent);
            sendResult.whenCompleteAsync((status,throwable) -> {
                log.info("Filter send event status=> {}", status);
            });
        } catch (Exception e) {
            log.error("failed to process event", e);
        }
    }

    /**
     * Captures the request and response headers associated with this request.
     * @param httpServletRequest
     * @param responseSpy
     * @param event
     */
    private void postProcess(HttpServletRequest httpServletRequest, ResponseSpy responseSpy, Map<String,Object> event) {

        try {

            int rdm = ThreadLocalRandom.current().nextInt();
            if(rdm < 0) {
                rdm = rdm * (-1);
            }
            event.put("request.uuid", rdm);

            captureRequestData(event, httpServletRequest);

            captureResponseData(event, responseSpy);

        } catch (Exception e) {
            event.put("exception", e.toString());
            log.error("Error capturing data in api.RequestEventInfoCollector filter! uri=" +
                    httpServletRequest.getRequestURI(), e);
        }

    }

    /**
     * Captures response headers.
     * @param event
     * @param res
     */
    private void captureResponseData(Map<String, Object> event, ResponseSpy res ) {
        log.debug("Capturing response data");
        // response headers

        for (String name : res.headers.keySet()) {
            final StringBuilder valBuilder = new StringBuilder();
            boolean firstValue = true;
            for (String s : res.headers.get(name)) {
                // only prepends separator for non-first header values
                if (firstValue) firstValue = false;
                else {
                    valBuilder.append(VALUE_SEPARATOR);
                }
                valBuilder.append(s);
            }
            event.put(RESPONSE_HEADER_PREFIX + name, valBuilder.toString());
        }

        // Set Cookies
        if (!res.cookies.isEmpty()) {
            Iterator<Cookie> cookies = res.cookies.iterator();
            StringBuilder setCookies = new StringBuilder();
            while (cookies.hasNext()) {
                Cookie cookie = cookies.next();
                setCookies.append(cookie.getName()).append("=").append(cookie.getValue());
                String domain = cookie.getDomain();
                if (domain != null) {
                    setCookies.append("; Domain=").append(domain);
                }

                int maxAge = cookie.getMaxAge();
                if (maxAge >= 0) {
                    setCookies.append("; Max-Age=").append(maxAge);
                }

                String path = cookie.getPath();
                if (path != null) {
                    setCookies.append("; Path=").append(path);
                }

                if (cookie.getSecure()) {
                    setCookies.append("; Secure");
                }

                if (cookie.isHttpOnly()) {
                    setCookies.append("; HttpOnly");
                }

                if (cookies.hasNext()) {
                    setCookies.append(VALUE_SEPARATOR);
                }
            }

            event.put(RESPONSE_HEADER_PREFIX + "set-cookie", setCookies.toString());
        }

        // status of the request
        int status = res.statusCode;
        event.put("status", status);

    }

    /**
     * Captures request headers.
     * @param event
     * @param req
     */
    private void captureRequestData(Map<String, Object> event, HttpServletRequest req) {
        // basic request properties
        String path = req.getRequestURI();
        if (path == null) path = "/";
        event.put("path", path);
        event.put("host", req.getHeader("host"));
        event.put("query", req.getQueryString());
        event.put("method", req.getMethod());
        event.put("currentTime", System.currentTimeMillis());

        // request headers
        for (final Enumeration<String> names = req.getHeaderNames(); names.hasMoreElements();) {
            final String name = (String)names.nextElement();
            final StringBuilder valBuilder = new StringBuilder();
            boolean firstValue = true;
            for (final Enumeration<String> vals = req.getHeaders(name); vals.hasMoreElements();) {
                // only prepends separator for non-first header values
                if (firstValue) firstValue = false;
                else {
                    valBuilder.append(VALUE_SEPARATOR);
                }

                valBuilder.append(vals.nextElement());
            }

            event.put(REQUEST_HEADER_PREFIX + name, valBuilder.toString());
        }


        // request params
        // HTTP POSTs send a param with a weird encoded name, so we strip them out with this regex
        if("GET".equals(req.getMethod())) {
            final Map<String,String[]> params = req.getParameterMap();
            for (final Object key : params.keySet()) {
                final String keyString = key.toString();

                final Object val = params.get(key);
                String valString;
                if (val instanceof String[]) {
                    final String[] valArray = (String[]) val;
                    if (valArray.length == 1)
                        valString = valArray[0];
                    else
                        valString = Arrays.asList((String[]) val).toString();
                } else {
                    valString = val.toString();
                }
                event.put("param." + key, valString);
            }
        }
    }


    @Override
    public void destroy() {

    }

    /**
     * A simple wrapper for {@link HttpServletResponseWrapper} that is used to capture headers
     * and cookies associated with the response.
     */
    private static final class ResponseSpy extends HttpServletResponseWrapper {

        int statusCode = 200;
        final Map<String, List<String>> headers = new ConcurrentHashMap<>();
        final List<Cookie> cookies = new ArrayList<>();

        private ResponseSpy(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setStatus(int sc) {
            super.setStatus(sc);
            this.statusCode = sc;
        }

        @Override
        public void addCookie(Cookie cookie) {
            cookies.add(cookie);
            super.addCookie(cookie);
        }

        @Override public void setHeader(String name, String value) {
            List<String> values = new ArrayList<>();
            values.add(value);
            headers.put(name, values);
            super.setHeader(name, value);
        }

        @Override public void addHeader(String name, String value) {
            List<String> values = headers.computeIfAbsent(name, k -> new ArrayList<>());
            values.add(value);
            super.addHeader(name, value);
        }

        @Override public void setDateHeader(String name, long date) {
            List<String> values = new ArrayList<>();
            values.add(Long.toString(date));
            headers.put(name, values);
            super.setDateHeader(name, date);
        }

        @Override public void setIntHeader(String name, int val) {
            List<String> values = new ArrayList<>();
            values.add(Integer.toString(val));
            headers.put(name, values);
            super.setIntHeader(name, val);
        }
    }
}
