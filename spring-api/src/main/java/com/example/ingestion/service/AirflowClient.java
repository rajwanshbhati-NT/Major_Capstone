package com.example.ingestion.service;

import com.example.ingestion.config.IngestionProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class AirflowClient {

    private static final Logger LOG = LoggerFactory.getLogger(AirflowClient.class);
    private final IngestionProperties properties;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .build();

    public AirflowClient(IngestionProperties properties) {
        this.properties = properties;
    }

    public String triggerDag(String executionId, List<String> sourceFilePaths,
                             String fileFormat, String targetTable,
                             String schemaFilePath, String controlFilePath) {
        try {
            String apiUrl = properties.getAirflow().getApiUrl();
            String user = properties.getAirflow().getApiUsername();
            String pass = properties.getAirflow().getApiPassword();

            if (apiUrl == null || apiUrl.isEmpty()) {
                throw new RuntimeException("airflow.api.url not configured");
            }
            if (user == null || pass == null) {
                throw new RuntimeException("airflow.api credentials not configured");
            }

            String fullUrl = apiUrl.endsWith("/") ? apiUrl + "dagRuns" : apiUrl + "/dagRuns";

            String auth = Base64.getEncoder().encodeToString((user + ":" + pass).getBytes());

            Map<String, Object> conf = new HashMap<>();
            conf.put("execution_id", executionId);
            conf.put("source_file_paths", sourceFilePaths);
            conf.put("file_format", fileFormat);
            conf.put("target_table", targetTable);
            conf.put("schema_file_path", schemaFilePath);
            conf.put("control_file_path", controlFilePath);

            Map<String, Object> body = new HashMap<>();
            body.put("dag_run_id", executionId);
            body.put("conf", conf);

            String jsonBody = mapper.writeValueAsString(body);

            LOG.info("AIRFLOW DEBUG");
            LOG.info("Base URL: [{}]", apiUrl);
            LOG.info("Full URL: [{}]", fullUrl);
            LOG.info("Auth user: [{}]", user);
            LOG.info("Conf: source_file_paths={}, schema_file_path={}, control_file_path={}",
                    sourceFilePaths.size(), schemaFilePath, controlFilePath);


            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(fullUrl))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Basic " + auth)
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            LOG.info("Airflow response status: {}", response.statusCode());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                JsonNode root = mapper.readTree(response.body());
                return root.path("dag_run_id").asText(executionId);
            } else {
                throw new RuntimeException("Airflow returned " + response.statusCode()
                        + ": " + response.body());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to trigger DAG: " + e.getMessage(), e);
        }
    }
}