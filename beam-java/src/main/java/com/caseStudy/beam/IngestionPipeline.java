package com.caseStudy.beam;

import com.caseStudy.beam.config.IngestionConfig;
import com.caseStudy.beam.parser.FileParserFactory;
import com.caseStudy.beam.security.EncryptionService;
import com.caseStudy.beam.transform.AddMetadataColumns;
import com.caseStudy.beam.transform.EncryptSensitiveFields;
import com.caseStudy.beam.transform.ReplaceNullsWithWhitespace;
import com.caseStudy.beam.transform.WriteErrorsToTextFile;
import com.caseStudy.beam.transform.WriteToWarehouse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.runners.direct.DirectOptions;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class IngestionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private IngestionPipeline() { }

    public static void main(String[] args) {
        IngestionConfig config = IngestionConfig.fromArgs(args);
        LOG.info("Starting ingestion pipeline with config: {}", config);

        EncryptionService encryptionService = new EncryptionService(
                System.getenv("INGESTION_AES_KEY"));

        List<String> sensitiveFields = readSensitiveFields(config);

        // Create pipeline with Beam-specific options only
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs("--runner=DirectRunner", "--targetParallelism=1")
                .create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> rawLines = pipeline
                .apply("ReadSourceFile", FileParserFactory.readSource(config));

        PCollection<String> records = rawLines
                .apply("ParseRecords", FileParserFactory.parseRecords(config));

        PCollection<String> normalRecords = records
                .apply("WriteParseErrors",
                        WriteErrorsToTextFile.of(config.getErrorRecordLocation()));

        PCollection<String> cleansed = normalRecords
                .apply("ReplaceNullsWithWhitespace", ReplaceNullsWithWhitespace.of());

        PCollection<String> encrypted = sensitiveFields.isEmpty()
                ? cleansed
                : cleansed.apply("EncryptSensitiveFields",
                EncryptSensitiveFields.of(encryptionService, sensitiveFields));

        PCollection<String> enriched = encrypted
                .apply("AddMetadataColumns",
                        AddMetadataColumns.of(
                                config.getExecutionId(),
                                config.getIngestionTimestamp(),
                                config.getSourceCreationTime()));

        enriched.apply("WriteToWarehouse", WriteToWarehouse.of(config));

        pipeline.run().waitUntilFinish();
        LOG.info("Ingestion pipeline completed for execution_id={}", config.getExecutionId());
    }

    private static List<String> readSensitiveFields(IngestionConfig config) {
        try {
            JsonNode schema = MAPPER.readTree(new File(config.getSchemaFilePath()));
            JsonNode fields = schema.path("fields");
            if (!fields.isObject()) { return List.of(); }
            List<String> sensitive = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> it = fields.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                if (e.getValue().path("encrypt").asBoolean(false)) {
                    sensitive.add(e.getKey());
                }
            }
            return sensitive;
        } catch (Exception e) {
            LOG.warn("Could not read sensitive fields from schema, none will be encrypted.", e);
            return List.of();
        }
    }
}