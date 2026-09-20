package com.example.ingestion.dto;

public class IngestionRequestDto {
    private String filePath;
    private String schemaFilePath;
    private String fileFormat;
    private String targetTable;
    private String executionId;
    private String controlFilePath;

    public String getFilePath() { return filePath; }
    public void setFilePath(String v) { this.filePath = v; }
    public String getSchemaFilePath() { return schemaFilePath; }
    public void setSchemaFilePath(String v) { this.schemaFilePath = v; }
    public String getFileFormat() { return fileFormat; }
    public void setFileFormat(String v) { this.fileFormat = v; }
    public String getTargetTable() { return targetTable; }
    public void setTargetTable(String v) { this.targetTable = v; }
    public String getExecutionId() { return executionId; }
    public void setExecutionId(String v) { this.executionId = v; }
    public String getControlFilePath() { return controlFilePath; }
    public void setControlFilePath(String v) { this.controlFilePath = v; }
}