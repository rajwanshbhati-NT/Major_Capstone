package com.example.ingestion.dto;

import java.util.List;

public class IngestionResponseDto {
    private String status;
    private String executionId;
    private String message;
    private boolean wasSplit;
    private int partCount;
    private List<String> partPaths;
    private Long expectedRecordCount;
    private String controlFilePath;

    public IngestionResponseDto() {}

    public IngestionResponseDto(String status, String executionId, String message,
                                boolean wasSplit, int partCount, List<String> partPaths) {
        this.status = status;
        this.executionId = executionId;
        this.message = message;
        this.wasSplit = wasSplit;
        this.partCount = partCount;
        this.partPaths = partPaths;
    }

    public IngestionResponseDto(String status, String executionId, String message,
                                boolean wasSplit, int partCount, List<String> partPaths,
                                Long expectedRecordCount, String controlFilePath) {
        this(status, executionId, message, wasSplit, partCount, partPaths);
        this.expectedRecordCount = expectedRecordCount;
        this.controlFilePath = controlFilePath;
    }

    public String getStatus() { return status; }
    public void setStatus(String v) { this.status = v; }
    public String getExecutionId() { return executionId; }
    public void setExecutionId(String v) { this.executionId = v; }
    public String getMessage() { return message; }
    public void setMessage(String v) { this.message = v; }
    public boolean isWasSplit() { return wasSplit; }
    public void setWasSplit(boolean v) { this.wasSplit = v; }
    public int getPartCount() { return partCount; }
    public void setPartCount(int v) { this.partCount = v; }
    public List<String> getPartPaths() { return partPaths; }
    public void setPartPaths(List<String> v) { this.partPaths = v; }
    public Long getExpectedRecordCount() { return expectedRecordCount; }
    public void setExpectedRecordCount(Long v) { this.expectedRecordCount = v; }
    public String getControlFilePath() { return controlFilePath; }
    public void setControlFilePath(String v) { this.controlFilePath = v; }
}