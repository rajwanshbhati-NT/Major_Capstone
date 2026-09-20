package com.example.ingestion.service;

import com.example.ingestion.config.IngestionProperties;
import com.example.ingestion.dto.SplitResultDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class PySparkInvoker {

    private static final Logger LOG = LoggerFactory.getLogger(PySparkInvoker.class);
    private final IngestionProperties properties;
    // converts JSON text to Java objects
    private final ObjectMapper mapper = new ObjectMapper();

    public PySparkInvoker(IngestionProperties properties) {
        this.properties = properties;
    }

    public SplitResultDto splitFile(String inputPath, String fileFormat) {
        long sizeBytes = new File(inputPath).length();
        long sizeKb = sizeBytes / 1024;
        long targetKb = properties.getPyspark().getTargetPartitionSizeKb();

        LOG.info("Splitting: {} ({} KB) into parts of ~{} KB", inputPath, sizeKb, targetKb);

        String ts = String.valueOf(System.currentTimeMillis());
        String fileName = new File(inputPath).getName();
        String baseName = fileName.contains(".")
                ? fileName.substring(0, fileName.lastIndexOf('.'))
                : fileName;
        String outputDir = properties.getPyspark().getOutputDir() + "/" + baseName + "_" + ts;

        try {
            Files.createDirectories(Paths.get(outputDir));
        } catch (Exception e) {
            throw new RuntimeException("Cannot create output dir: " + outputDir, e);
        }

        String scriptPath = safe(properties.getPyspark().getScriptPath(), "/opt/pyspark/splitter_job.py");
        String master = safe(properties.getPyspark().getSparkMaster(), "local[*]");
        String driverMem = safe(properties.getPyspark().getSparkDriverMemory(), "1g");

        // Use docker exec into airflow-scheduler Run Python script inside airflow-scheduler container
        List<String> cmd = Arrays.asList(
                "docker", "exec", "airflow-scheduler",
                "python3", scriptPath,
                "--input", inputPath,
                "--output-dir", outputDir,
                "--target-size-kb", String.valueOf(targetKb),
                "--format", fileFormat,
                "--master", master,
                "--driver-memory", driverMem
        );

        for (int i = 0; i < cmd.size(); i++) {
            if (cmd.get(i) == null) {
                throw new RuntimeException("Command arg " + i + " is null! cmd=" + cmd);
            }
        }

        LOG.info("Running: {}", String.join(" ", cmd));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process process;
        try {
            process = pb.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start PySpark: " + e.getMessage(), e);
        }

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
                LOG.info("[PySpark] {}", line);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed reading PySpark output", e);
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("PySpark failed (exit=" + exitCode + "):\n" + output);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Wait interrupted", e);
        }

        String jsonLine = findJsonLine(output.toString());
        try {
            JsonNode result = mapper.readTree(jsonLine);
            List<String> parts = new ArrayList<>();
            result.get("parts").forEach(p -> parts.add(p.asText()));
            LOG.info("Split complete: {} parts", parts.size());
            return new SplitResultDto(true, sizeBytes, parts.size(), parts);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse result: " + jsonLine, e);
        }
    }

    private String safe(String value, String defaultValue) {
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }

    private String findJsonLine(String output) {
        for (String line : output.split("\n")) {
            String t = line.trim();
            if (t.startsWith("__RESULT_JSON__:")) {
                return t.substring("__RESULT_JSON__:".length());
            }
            if (t.startsWith("{") && t.endsWith("}")) {
                return t;
            }
        }
        throw new RuntimeException("No JSON line in PySpark output. Output was:\n" + output);
    }
}