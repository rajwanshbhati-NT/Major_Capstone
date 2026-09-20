package com.example.ingestion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

@Service
public class ControlFileService {

    private static final Logger LOG = LoggerFactory.getLogger(ControlFileService.class);

    public Long readExpectedRecordCount(String controlFilePath) {
        if (controlFilePath == null || controlFilePath.isEmpty()) {
            LOG.info("No control file specified - skipping record count validation");
            return null;
        }

        if (!Files.exists(Paths.get(controlFilePath))) {
            throw new RuntimeException("Control file not found: " + controlFilePath);
        }

        Properties props = new Properties();
        try (var input = Files.newInputStream(Paths.get(controlFilePath))) {
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Cannot read control file: " + controlFilePath, e);
        }

        String recordCountStr = props.getProperty("record_count");
        if (recordCountStr == null || recordCountStr.isEmpty()) {
            throw new RuntimeException(
                    "Control file missing 'record_count' attribute: " + controlFilePath);
        }

        try {
            long expected = Long.parseLong(recordCountStr.trim());
            LOG.info("Control file: {} -> expected record_count = {}", controlFilePath, expected);
            return expected;
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    "Invalid 'record_count' value in control file: '" + recordCountStr + "'", e);
        }
    }
}