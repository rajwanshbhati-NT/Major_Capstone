package com.example.ingestion.service;

import com.example.ingestion.config.IngestionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class FileSizeService {

    private static final Logger LOG = LoggerFactory.getLogger(FileSizeService.class);
    private final IngestionProperties properties;

    public FileSizeService(IngestionProperties properties) {
        this.properties = properties;
    }

    public long getFileSizeBytes(String filePath) {
        File f = new File(filePath);
        if (!f.exists()) {
            LOG.warn("File not found: {}", filePath);
            return -1L;
        }
        return f.length();
    }

    public boolean exceedsThreshold(String filePath) {
        long sizeBytes = getFileSizeBytes(filePath);
        if (sizeBytes < 0) {
            return false;
        }
        long sizeKb = sizeBytes / 1024;
        long thresholdKb = parseLongSafe(properties.getFileSize().getThresholdKb(), 5120L);
        boolean exceeds = sizeKb > thresholdKb;
        LOG.info("File: {} | Size: {} KB | Threshold: {} KB | Need split: {}",
                filePath, sizeKb, thresholdKb, exceeds);
        return exceeds;
    }

    private long parseLongSafe(String value, long defaultValue) {
        if (value == null || value.isEmpty()) return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}