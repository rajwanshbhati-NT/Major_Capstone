package com.example.ingestion.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class AirflowTriggerService {

    private final RestTemplate restTemplate;

    @Value("${airflow.api.url}")
    private String airflowUrl;

    @Value("${airflow.api.username}")
    private String airflowUser;

    @Value("${airflow.api.password}")
    private String airflowPassword;

    public AirflowTriggerService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public boolean triggerAirflowDag(String filePath,
                                     String schemaFilePath,
                                     String fileFormat,
                                     String targetTable,
                                     String executionId) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(airflowUser, airflowPassword);


            Map<String, Object> conf = new HashMap<>();
            conf.put("source_file_path", filePath);
            conf.put("schema_file_path", schemaFilePath);
            conf.put("file_format",      fileFormat);
            conf.put("target_table",     targetTable);
            conf.put("execution_id",     executionId);

            // body = Airflow's required format
            Map<String, Object> body = new HashMap<>();
            body.put("dag_run_id",   executionId);
            body.put("logical_date", OffsetDateTime.now().toString());
            body.put("conf", conf);

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

            String triggerUrl = airflowUrl + "/dagRuns";
            ResponseEntity<String> response = restTemplate.postForEntity(
                    triggerUrl, request, String.class);

            return response.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            System.err.println("Failed to trigger Airflow DAG: " + e.getMessage());
            return false;
        }
    }
}
