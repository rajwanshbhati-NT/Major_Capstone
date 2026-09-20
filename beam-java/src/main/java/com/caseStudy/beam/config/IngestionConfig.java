package com.caseStudy.beam.config;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class IngestionConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String executionId;
    private final String sourceFilePath;
    private final String schemaFilePath;
    private final String fileFormat;
    private final String targetTable;
    private final String warehouseJdbcUrl;
    private final String warehouseUsername;
    private final String warehousePassword;
    private final String errorRecordLocation;
    private final Instant ingestionTimestamp;
    private final Instant sourceCreationTime;

    private IngestionConfig(Builder b) {
        this.executionId         = b.executionId != null ? b.executionId : UUID.randomUUID().toString();
        this.sourceFilePath      = require(b.sourceFilePath,  "sourceFilePath");
        this.schemaFilePath      = require(b.schemaFilePath,  "schemaFilePath");
        this.fileFormat          = require(b.fileFormat,      "fileFormat").toUpperCase();
        this.targetTable         = require(b.targetTable,     "targetTable");
        this.warehouseJdbcUrl    = require(b.warehouseJdbcUrl,"warehouseJdbcUrl");
        this.warehouseUsername   = require(b.warehouseUsername,"warehouseUsername");
        this.warehousePassword   = require(b.warehousePassword,"warehousePassword");
        this.errorRecordLocation = require(b.errorRecordLocation,"errorRecordLocation");
        this.ingestionTimestamp  = b.ingestionTimestamp != null ? b.ingestionTimestamp : Instant.now();
        this.sourceCreationTime  = b.sourceCreationTime != null ? b.sourceCreationTime : Instant.now();
    }

    public static IngestionConfig fromArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                String[] kv = arg.substring(2).split("=", 2);
                map.put(kv[0], kv[1]);
            }
        }
        return new Builder()
                .executionId(map.get("executionId"))
                .sourceFilePath(map.get("sourceFilePath"))
                .schemaFilePath(map.get("schemaFilePath"))
                .fileFormat(map.get("fileFormat"))
                .targetTable(map.get("targetTable"))
                .warehouseJdbcUrl(map.get("warehouseJdbcUrl"))
                .warehouseUsername(map.get("warehouseUsername"))
                .warehousePassword(map.get("warehousePassword"))
                .errorRecordLocation(map.get("errorRecordLocation"))
                .ingestionTimestamp(parseInstant(map.get("ingestionTimestamp")))
                .sourceCreationTime(parseInstant(map.get("sourceCreationTime")))
                .build();
    }

    public String[] toPipelineArgs() {
        return new String[] {
                "--sourceFilePath=" + sourceFilePath,
                "--schemaFilePath=" + schemaFilePath,
                "--fileFormat="     + fileFormat,
                "--executionId="    + executionId,
                "--runner=DirectRunner",
                "--targetParallelism=1"
        };
    }

    private static Instant parseInstant(String value) {
        return value == null ? null : Instant.parse(value);
    }

    private static String require(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Required config field missing: " + field);
        }
        return value;
    }

    public String getExecutionId()            { return executionId; }
    public String getSourceFilePath()         { return sourceFilePath; }
    public String getSchemaFilePath()         { return schemaFilePath; }
    public String getFileFormat()             { return fileFormat; }
    public String getTargetTable()            { return targetTable; }
    public String getWarehouseJdbcUrl()       { return warehouseJdbcUrl; }
    public String getWarehouseUsername()      { return warehouseUsername; }
    public String getWarehousePassword()      { return warehousePassword; }
    public String getErrorRecordLocation()    { return errorRecordLocation; }
    public Instant getIngestionTimestamp()    { return ingestionTimestamp; }
    public Instant getSourceCreationTime()    { return sourceCreationTime; }

    @Override
    public String toString() {
        return "IngestionConfig{executionId='" + executionId + "', file='" + sourceFilePath +
                "', format=" + fileFormat + ", targetTable=" + targetTable + "}";
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String executionId;
        private String sourceFilePath;
        private String schemaFilePath;
        private String fileFormat;
        private String targetTable;
        private String warehouseJdbcUrl;
        private String warehouseUsername;
        private String warehousePassword;
        private String errorRecordLocation;
        private Instant ingestionTimestamp;
        private Instant sourceCreationTime;

        public Builder executionId(String v)         { this.executionId = v; return this; }
        public Builder sourceFilePath(String v)      { this.sourceFilePath = v; return this; }
        public Builder schemaFilePath(String v)      { this.schemaFilePath = v; return this; }
        public Builder fileFormat(String v)          { this.fileFormat = v; return this; }
        public Builder targetTable(String v)         { this.targetTable = v; return this; }
        public Builder warehouseJdbcUrl(String v)    { this.warehouseJdbcUrl = v; return this; }
        public Builder warehouseUsername(String v)   { this.warehouseUsername = v; return this; }
        public Builder warehousePassword(String v)   { this.warehousePassword = v; return this; }
        public Builder errorRecordLocation(String v) { this.errorRecordLocation = v; return this; }
        public Builder ingestionTimestamp(Instant v){ this.ingestionTimestamp = v; return this; }
        public Builder sourceCreationTime(Instant v){ this.sourceCreationTime = v; return this; }
        public IngestionConfig build()               { return new IngestionConfig(this); }
    }
}
