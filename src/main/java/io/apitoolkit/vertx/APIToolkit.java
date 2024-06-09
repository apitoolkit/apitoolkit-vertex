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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    public static class ClientMetadata {

        @JsonProperty("project_id")
        private String projectId;

        @JsonProperty("pubsub_project_id")
        private String pubsubProjectId;

        @JsonProperty("topic_id")
        private String topicId;

        @JsonProperty("pubsub_push_service_account")
        private Map<String, String> pubsubPushServiceAccount;
    }

    public void publishMessage(ByteString message) {
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

    public static ClientMetadata getClientMetadata(String apiKey, String rootUrl) throws IOException {
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
