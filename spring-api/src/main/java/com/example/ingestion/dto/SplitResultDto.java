package com.example.ingestion.dto;

import java.util.List;

public class SplitResultDto {
    private boolean wasSplit;
    private long originalSizeBytes;
    private int partCount;
    private List<String> partPaths;

    public SplitResultDto() {}

    public SplitResultDto(boolean wasSplit, long originalSizeBytes,
                          int partCount, List<String> partPaths) {
        this.wasSplit = wasSplit;
        this.originalSizeBytes = originalSizeBytes;
        this.partCount = partCount;
        this.partPaths = partPaths;
    }

    public boolean isWasSplit() { return wasSplit; }
    public void setWasSplit(boolean v) { this.wasSplit = v; }
    public long getOriginalSizeBytes() { return originalSizeBytes; }
    public void setOriginalSizeBytes(long v) { this.originalSizeBytes = v; }
    public int getPartCount() { return partCount; }
    public void setPartCount(int v) { this.partCount = v; }
    public List<String> getPartPaths() { return partPaths; }
    public void setPartPaths(List<String> v) { this.partPaths = v; }
}