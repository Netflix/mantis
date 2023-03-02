package io.mantisrx.api.filters;

import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.zuul.filters.http.HttpSyncEndpoint;
import com.netflix.zuul.message.http.HttpHeaderNames;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import io.mantisrx.api.proto.Artifact;
import io.mantisrx.api.services.artifacts.ArtifactManager;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

@Slf4j
public class Artifacts extends HttpSyncEndpoint {

    private final ArtifactManager artifactManager;
    private final ObjectMapper objectMapper;
    public static final String PATH_SPEC = "/api/v1/artifacts";

    @Override
    public boolean needsBodyBuffered(HttpRequestMessage input) {
        return input.getMethod().toLowerCase().equals("post");
    }

    @Inject
    public Artifacts(ArtifactManager artifactManager, ObjectMapper objectMapper) {
        this.artifactManager = artifactManager;
        this.objectMapper = objectMapper;
        artifactManager.putArtifact(new Artifact("mantis.json", 0, new byte[0]));
        artifactManager.putArtifact(new Artifact("mantis.zip", 0, new byte[0]));
    }

    @Override
    public HttpResponseMessage apply(HttpRequestMessage request) {

        if (request.getMethod().toLowerCase().equals("get")) {

            String fileName = request.getPath().replaceFirst("^" + PATH_SPEC + "/?", "");
            if (Strings.isNullOrEmpty(fileName)) {
                List<String> files = artifactManager
                        .getArtifacts();
                Try<String> serialized = Try.of(() -> objectMapper.writeValueAsString(files));

                return serialized.map(body -> {
                    HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request, 200);
                    response.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
                    response.setBodyAsText(body);
                    return response;
                }).getOrElseGet(t -> {
                    HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request, 500);
                    response.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString());
                    response.setBodyAsText(t.getMessage());
                    return response;
                });
            } else {
                Optional<Artifact> artifact = artifactManager.getArtifact(fileName);

                return artifact.map(art -> {
                    HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request,
                            HttpResponseStatus.OK.code());
                    response.setBody(art.getContent());
                    response.getHeaders().set(HttpHeaderNames.CONTENT_TYPE,
                            fileName.endsWith("json")
                                    ? HttpHeaderValues.APPLICATION_JSON.toString()
                                    : HttpHeaderValues.APPLICATION_OCTET_STREAM.toString());
                    response.getHeaders().set("Content-Disposition",
                            String.format("attachment; filename=\"%s\"", fileName));
                    return response;
                }).orElseGet(() -> {
                    HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request,
                            HttpResponseStatus.NOT_FOUND.code());
                    response.setBody(new byte[]{});
                    return response;
                });

            }
        } else if (request.getMethod().toLowerCase().equals("post")) {

            byte[] body = request.getBody();
            artifactManager.putArtifact(new Artifact("testing.json", body.length, body));

            HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request,
                    HttpResponseStatus.OK.code());
            return response;

        }

        HttpResponseMessage response = new HttpResponseMessageImpl(request.getContext(), request, HttpResponseStatus.METHOD_NOT_ALLOWED.code());
        response.setBodyAsText(HttpResponseStatus.METHOD_NOT_ALLOWED.reasonPhrase());
        response.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString());
        return response;
    }
}
