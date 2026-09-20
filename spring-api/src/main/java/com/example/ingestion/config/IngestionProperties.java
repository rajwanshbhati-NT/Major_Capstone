package com.example.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@ConfigurationProperties(prefix = "")
public class IngestionProperties {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionProperties.class);

    private FileSize fileSize = new FileSize();
    private Airflow airflow = new Airflow();
    private Pyspark pyspark = new Pyspark();

    @PostConstruct
    public void logProperties() {
        LOG.info("LOADED PROPERTIES");
        LOG.info("FileSize: threshold={} KB", fileSize.getThresholdKb());
        LOG.info("Airflow: url={}", airflow.getApiUrl());
        LOG.info("PySpark: script={}, target={} KB, master={}",
                pyspark.getScriptPath(), pyspark.getTargetPartitionSizeKb(),
                pyspark.getSparkMaster());
    }

    public static class FileSize {
        private String thresholdKb = "5120";
        private String partitionKb = "1024";

        public String getThresholdKb() { return thresholdKb; }
        public void setThresholdKb(String v) { this.thresholdKb = v; }
        public String getPartitionKb() { return partitionKb; }
        public void setPartitionKb(String v) { this.partitionKb = v; }
    }

    public static class Airflow {
        private String apiUrl;
        private String apiUsername;
        private String apiPassword;
        private String dagId = "sor_ingestion_phase1";

        public String getApiUrl() { return apiUrl; }
        public void setApiUrl(String v) { this.apiUrl = v; }
        public String getApiUsername() { return apiUsername; }
        public void setApiUsername(String v) { this.apiUsername = v; }
        public String getApiPassword() { return apiPassword; }
        public void setApiPassword(String v) { this.apiPassword = v; }
        public String getDagId() { return dagId; }
        public void setDagId(String v) { this.dagId = v; }
    }

    public static class Pyspark {
        private String scriptPath = "/opt/pyspark/splitter_job.py";
        private String outputDir = "/opt/airflow/data/splits";
        private long targetPartitionSizeKb = 1024;
        private String sparkMaster = "local[*]";
        private String sparkDriverMemory = "1g";

        public String getScriptPath() { return scriptPath; }
        public void setScriptPath(String v) { this.scriptPath = v; }
        public String getOutputDir() { return outputDir; }
        public void setOutputDir(String v) { this.outputDir = v; }
        public long getTargetPartitionSizeKb() { return targetPartitionSizeKb; }
        public void setTargetPartitionSizeKb(long v) { this.targetPartitionSizeKb = v; }
        public String getSparkMaster() { return sparkMaster; }
        public void setSparkMaster(String v) { this.sparkMaster = v; }
        public String getSparkDriverMemory() { return sparkDriverMemory; }
        public void setSparkDriverMemory(String v) { this.sparkDriverMemory = v; }
    }

    public FileSize getFileSize() { return fileSize; }
    public void setFileSize(FileSize v) { this.fileSize = v; }
    public Airflow getAirflow() { return airflow; }
    public void setAirflow(Airflow v) { this.airflow = v; }
    public Pyspark getPyspark() { return pyspark; }
    public void setPyspark(Pyspark v) { this.pyspark = v; }
}