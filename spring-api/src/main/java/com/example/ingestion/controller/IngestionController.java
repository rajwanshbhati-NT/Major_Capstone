package com.example.ingestion.controller;

import com.example.ingestion.dto.IngestionRequestDto;
import com.example.ingestion.dto.IngestionResponseDto;
import com.example.ingestion.dto.SplitResultDto;
import com.example.ingestion.service.AirflowClient;
import com.example.ingestion.service.ControlFileService;
import com.example.ingestion.service.FileSizeService;
import com.example.ingestion.service.PySparkInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/ingestion")
public class IngestionController {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionController.class);

    private final FileSizeService fileSizeService;
    private final PySparkInvoker pySparkInvoker;
    private final AirflowClient airflowClient;
    private final ControlFileService controlFileService;

    public IngestionController(FileSizeService fileSizeService,
                               PySparkInvoker pySparkInvoker,
                               AirflowClient airflowClient,
                               ControlFileService controlFileService) {
        this.fileSizeService = fileSizeService;
        this.pySparkInvoker = pySparkInvoker;
        this.airflowClient = airflowClient;
        this.controlFileService = controlFileService;
    }

    @PostMapping("/trigger")
    public IngestionResponseDto trigger(@RequestBody IngestionRequestDto request) {
        String executionId = request.getExecutionId() != null
                ? request.getExecutionId()
                : UUID.randomUUID().toString();

        String filePath = request.getFilePath();
        String fileFormat = request.getFileFormat();
        String targetTable = request.getTargetTable();
        String schemaFilePath = request.getSchemaFilePath();
        String controlFilePath = request.getControlFilePath();

        LOG.info("Triggering: executionId={}, file={}, schema={}, control={}",
                executionId, filePath, schemaFilePath, controlFilePath);

        boolean wasSplit = false;
        int partCount = 1;
        List<String> filePathsToProcess;

        try {
            Long expectedRecordCount = controlFileService.readExpectedRecordCount(controlFilePath);

            if (fileSizeService.exceedsThreshold(filePath)) {
                LOG.info("File too big - invoking PySpark to split");
                SplitResultDto splitResult = pySparkInvoker.splitFile(filePath, fileFormat);
                wasSplit = splitResult.isWasSplit();
                partCount = splitResult.getPartCount();
                filePathsToProcess = splitResult.getPartPaths();
            } else {
                LOG.info("File below threshold - no split needed");
                filePathsToProcess = Collections.singletonList(filePath);
            }

            String dagRunId = airflowClient.triggerDag(
                    executionId, filePathsToProcess, fileFormat, targetTable,
                    schemaFilePath, controlFilePath);

            String message = wasSplit
                    ? "DAG triggered with " + partCount + " split files"
                    : "DAG triggered (no split)";

            if (expectedRecordCount != null) {
                message += ". Expected records: " + expectedRecordCount;
            }

            return new IngestionResponseDto("SUCCESS", dagRunId, message,
                    wasSplit, partCount, filePathsToProcess,
                    expectedRecordCount, controlFilePath);

        } catch (Exception e) {
            LOG.error("Ingestion failed: {}", e.getMessage(), e);
            return new IngestionResponseDto("ERROR", executionId,
                    "Failed: " + e.getMessage(), false, 0, null, null, controlFilePath);
        }
    }
}