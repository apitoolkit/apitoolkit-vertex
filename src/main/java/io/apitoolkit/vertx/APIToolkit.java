package io.apitoolkit.vertx;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;

public class APIToolkit {
    private Publisher pubsubClient;
    private ClientMetadata clientMetadata;
    private Boolean debug;
    private List<String> redactHeaders;
    private List<String> redactRequestBody;
    private List<String> redactResponseBody;

    APIToolkit(Boolean debug, List<String> redactHeaders, List<String> redactRequestBody,
            List<String> redactResponseBody, ClientMetadata metadata) throws Exception {

        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonStr = mapper.writeValueAsString(metadata.pubsubPushServiceAccount);
            Credentials credentials;
            credentials = GoogleCredentials.fromStream(
                    new ByteArrayInputStream(jsonStr.getBytes()))
                    .createScoped();
            ProjectTopicName topicName = ProjectTopicName.of(metadata.pubsubProjectId,
                    metadata.topicId);
            pubsubClient = Publisher.newBuilder(topicName).setCredentialsProvider(
                    FixedCredentialsProvider.create(credentials)).build();
            this.clientMetadata = metadata;
            this.redactHeaders = redactHeaders;
            this.redactRequestBody = redactRequestBody;
            this.redactResponseBody = redactResponseBody;
            this.debug = debug;

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("APIToolkit: Error initializing client", e);
        }

    }

    public void vertxHandler(RoutingContext context) {
        long startTime = System.nanoTime();
        String msgId = UUID.randomUUID().toString();
        int statusCode = 200;
        context.put("apitoolkit_msg_id", msgId);
        context.response().bodyEndHandler(v -> {

        });
        context.next();
        statusCode = context.response().getStatusCode();

        ByteString payload = buildPayload(startTime, context, context.request(), context.response(),
                statusCode, msgId);
        this.publishMessage(payload);
    }

    private ByteString buildPayload(long duration, RoutingContext context, HttpServerRequest req,
            HttpServerResponse res,
            Integer statusCode, String msgid) {

        HashMap<String, Object> reqHeaders = new HashMap<>();

        MultiMap reqHeadersV = req.headers();
        for (String key : reqHeadersV.names()) {
            String value = reqHeadersV.get(key);
            reqHeaders.put(key, value);
        }

        reqHeaders = Utils.redactHeaders(reqHeaders, this.redactHeaders);

        HashMap<String, Object> resHeaders = new HashMap<>();
        MultiMap resHeadersV = res.headers();
        for (String key : resHeadersV.names()) {
            String value = resHeadersV.get(key);
            resHeaders.put(key, value);
        }

        resHeaders = Utils.redactHeaders(resHeaders, this.redactHeaders);

        MultiMap params = req.params();
        HashMap<String, Object> query_params = new HashMap<>();
        for (String key : params.names()) {
            Object value = params.get(key);
            query_params.put(key, value);
        }

        String method = req.method().toString();
        String queryString = req.query() == null ? "" : "?" + req.query();
        String rawUrl = req.path() + queryString;
        String matchedPattern = context.currentRoute().getPath();
        String req_body_str = context.body().asString();
        byte[] req_body = req_body_str == null ? "".getBytes() : req_body_str.getBytes();
        byte[] res_body = "".getBytes();

        Map<String, String> pathVariables = (Map<String, String>) context.pathParams();
        byte[] redactedBody = Utils.redactJson(req_body,
                this.redactRequestBody, this.debug);
        byte[] redactedResBody = Utils.redactJson(res_body, this.redactResponseBody, this.debug);
        Date currentDate = new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
        String isoString = dateFormat.format(currentDate);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errorList = (List<Map<String, Object>>) context.get("APITOOLKIT_ERRORS");

        Map<String, Object> payload = new HashMap<>();
        payload.put("request_headers", reqHeaders);
        payload.put("response_headers", resHeaders);
        payload.put("status_code", statusCode);
        payload.put("method", method);
        payload.put("errors", errorList);
        payload.put("host", req.authority().host());
        payload.put("raw_url", rawUrl);
        payload.put("duration", duration);
        payload.put("url_path", matchedPattern);
        payload.put("query_params", query_params);
        payload.put("path_params", pathVariables);
        payload.put("project_id", this.clientMetadata.projectId);
        payload.put("proto_major", 1);
        payload.put("proto_minor", 1);
        payload.put("msg_id", msgid);
        payload.put("timestamp", isoString);
        payload.put("referer", req.getHeader("referer") == null ? "" : req.getHeader("referer"));
        payload.put("sdk_type", "JavaVertx");
        payload.put("request_body", Base64.getEncoder().encodeToString(redactedBody));
        payload.put("response_body", Base64.getEncoder().encodeToString(redactedResBody));

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            byte[] jsonBytes = objectMapper.writeValueAsBytes(payload);
            return ByteString.copyFrom(jsonBytes);
        } catch (Exception e) {
            e.printStackTrace();
            if (this.debug) {
                e.printStackTrace();
            }
            return ByteString.EMPTY;
        }

    }

    public static APIToolkit newClient(String apikey, Boolean debug, List<String> redactHeaders,
            List<String> redactRequestBody,
            List<String> redactResponseBody) throws Exception {
        ClientMetadata metadata;
        try {
            metadata = getClientMetadata(apikey, null);

            if (debug) {
                System.out.println("Client initialized successfully");
            }
            return new APIToolkit(debug, redactHeaders, redactRequestBody, redactResponseBody, metadata);
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception("APIToolkit: Error initializing client", e);
        }
    }

    public static APIToolkit newClient(String apikey) throws Exception {
        List<String> emptyList = Arrays.asList();
        return newClient(apikey, false, emptyList, emptyList, emptyList);
    }

    public static APIToolkit newClient(String apikey, Boolean debug) throws Exception {
        List<String> emptyList = Arrays.asList();
        return newClient(apikey, debug, emptyList, emptyList, emptyList);
    }

    public static APIToolkit newClient(String apikey, Boolean debug, List<String> redactHeaders) throws Exception {
        List<String> emptyList = Arrays.asList();
        return newClient(apikey, debug, redactHeaders, emptyList, emptyList);
    }

    public static APIToolkit newClient(String apikey, Boolean debug, List<String> redactHeaders,
            List<String> redactRequestBody) throws Exception {
        List<String> emptyList = Arrays.asList();
        return newClient(apikey, debug, redactHeaders, redactRequestBody, emptyList);
    }

    private static class ClientMetadata {

        @JsonProperty("project_id")
        private String projectId;

        @JsonProperty("pubsub_project_id")
        private String pubsubProjectId;

        @JsonProperty("topic_id")
        private String topicId;

        @JsonProperty("pubsub_push_service_account")
        private Map<String, String> pubsubPushServiceAccount;
    }

    private void publishMessage(ByteString message) {
        if (this.pubsubClient != null) {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(message).build();
            ApiFuture<String> messageIdFuture = this.pubsubClient.publish(pubsubMessage);
            try {
                String messageId = messageIdFuture.get();
                if (this.debug) {
                    System.out.println("Published a message with custom attributes: " + messageId);
                }
            } catch (Exception e) {
                if (this.debug) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static ClientMetadata getClientMetadata(String apiKey, String rootUrl) throws IOException {
        String url = "https://app.apitoolkit.io";
        if (rootUrl != null && !rootUrl.isEmpty()) {
            url = rootUrl;
        }
        url += "/api/client_metadata";
        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + apiKey)
                .build();

        Response response = client.newCall(request).execute();

        if (!response.isSuccessful()) {
            throw new IOException("Failed to get client metadata: " + response);
        }
        String jsonResponse = response.body().string();
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(jsonResponse, ClientMetadata.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
