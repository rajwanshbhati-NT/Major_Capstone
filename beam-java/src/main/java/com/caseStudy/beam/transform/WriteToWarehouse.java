package com.caseStudy.beam.transform;

import com.caseStudy.beam.config.IngestionConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.File;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class WriteToWarehouse
        extends PTransform<PCollection<String>, PDone>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final TupleTag<String> VALID_TAG = new TupleTag<String>() { };
    public static final TupleTag<String> ERROR_TAG = new TupleTag<String>() { };

    private final IngestionConfig config;
    private final List<String> dataColumns;
    private final List<String> encryptedColumns;
    private final List<String> requiredFields;

    private WriteToWarehouse(IngestionConfig config,
                             List<String> dataColumns,
                             List<String> encryptedColumns,
                             List<String> requiredFields) {
        this.config = config;
        this.dataColumns = dataColumns;
        this.encryptedColumns = encryptedColumns;
        this.requiredFields = requiredFields;
    }

    public static WriteToWarehouse of(IngestionConfig config) {
        return new WriteToWarehouse(config,
                readDataColumns(config),
                readEncryptedColumns(config),
                readRequiredFields(config));
    }

    @Override
    public PDone expand(PCollection<String> input) {
        String insert = buildInsertStatement(config.getTargetTable(),
                dataColumns, encryptedColumns);
        System.out.println("[WriteToWarehouse] INSERT SQL: " + insert);
        System.out.println("[WriteToWarehouse] dataColumns=" + dataColumns);
        System.out.println("[WriteToWarehouse] encryptedColumns=" + encryptedColumns);

        // Validate and split
        PCollectionTuple split = input.apply("ValidateAndSplit",
                ParDo.of(new ValidateAndSplitFn(requiredFields))
                        .withOutputTags(VALID_TAG, TupleTagList.of(ERROR_TAG)));

        split.get(ERROR_TAG)
                .apply( "WriteErrorsToTextFile",
                WriteErrorsToTextFile.of(
                        config.getErrorRecordLocation()));

        PDone jdbcSink = split.get(VALID_TAG)
                .apply("InsertIntoWarehouse", JdbcIO.<String>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create("org.postgresql.Driver", config.getWarehouseJdbcUrl())
                                .withUsername(config.getWarehouseUsername())
                                .withPassword(config.getWarehousePassword()))
                        .withStatement(insert)
                        .withPreparedStatementSetter(
                                new RecordSetter(dataColumns, encryptedColumns)));

        return jdbcSink;
    }

    public static class ValidateAndSplitFn extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<String> requiredFields;

        public ValidateAndSplitFn(List<String> requiredFields) {
            this.requiredFields = requiredFields;
        }

        @ProcessElement
        public void processElement(@Element String row,
                                   MultiOutputReceiver out) {
            if (row == null || row.startsWith("PARSE_ERROR")) {
                out.get(ERROR_TAG).output(row);
                return;
            }

            if (isBlankRow(row)) {
                System.out.println("[WriteToWarehouse] Blank row -> error");
                out.get(ERROR_TAG).output("BLANK|" + row);
                return;
            }

            String[] validationError = validateRequiredFields(row);
            if (validationError != null) {
                System.out.println("[WriteToWarehouse] Validation failed: "
                        + validationError[0] + "=" + validationError[1]);
                out.get(ERROR_TAG).output(
                        "VALIDATION_ERROR|warehouse|" + validationError[0] + "|"
                                + validationError[1] + "|" + row);
                return;
            }

            System.out.println("[WriteToWarehouse] Sending row: " + row);
            out.get(VALID_TAG).output(row);
        }

        private boolean isBlankRow(String row) {
            if (row == null || row.equals("{}") || row.length() < 5) return true;
            try {
                ObjectMapper m = new ObjectMapper();
                JsonNode node = m.readTree(row);
                int nonEmpty = 0;
                for (String f : requiredFields) {
                    JsonNode v = node.path(f);
                    if (v.isMissingNode() || v.isNull()) continue;
                    if (v.isValueNode() && v.asText().trim().isEmpty()) continue;
                    nonEmpty++;
                }
                return nonEmpty == 0;
            } catch (Exception e) {
                return false;
            }
        }

        private String[] validateRequiredFields(String row) {
            try {
                ObjectMapper m = new ObjectMapper();
                JsonNode node = m.readTree(row);
                for (String f : requiredFields) {
                    JsonNode v = node.path(f);
                    if (v.isMissingNode()) {
                        continue;
                    }
                    if (v.isNull()) {
                        return new String[]{f, "null"};
                    }
                    if (v.isValueNode() && v.asText().trim().isEmpty()) {
                        return new String[]{f, "empty"};
                    }
                }
                return null;
            } catch (Exception e) {
                return new String[]{"", "json_parse_failed:" + e.getMessage()};
            }
        }
    }

    private static List<String> readDataColumns(IngestionConfig config) {
        List<String> cols = new ArrayList<>();
        try {
            JsonNode schema = new ObjectMapper().readTree(new File(config.getSchemaFilePath()));
            JsonNode fields = schema.path("fields");
            if (fields.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = fields.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    cols.add(e.getKey());
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to read schema: " + e.getMessage());
        }
        return cols;
    }

    private static List<String> readEncryptedColumns(IngestionConfig config) {
        List<String> enc = new ArrayList<>();
        try {
            JsonNode schema = new ObjectMapper().readTree(new File(config.getSchemaFilePath()));
            JsonNode fields = schema.path("fields");
            fields.fields().forEachRemaining(e -> {
                if (e.getValue().path("encrypt").asBoolean(false)) {
                    enc.add(e.getKey());
                }
            });
        } catch (Exception ignore) { }
        return enc;
    }

    private static List<String> readRequiredFields(IngestionConfig config) {
        List<String> req = new ArrayList<>();
        try {
            JsonNode schema = new ObjectMapper().readTree(new File(config.getSchemaFilePath()));
            JsonNode fields = schema.path("fields");
            fields.fields().forEachRemaining(e -> {
                if (e.getValue().path("required").asBoolean(false)) {
                    req.add(e.getKey());
                }
            });
        } catch (Exception ignore) { }
        return req;
    }

    private static String buildInsertStatement(String table,
                                               List<String> dataCols,
                                               List<String> encCols) {
        List<String> allCols = new ArrayList<>();
        for (String col : dataCols) {
            if (encCols.contains(col)) continue;
            allCols.add(col);
        }
        for (String enc : encCols) {
//            String target = enc + "_encrypted";
            String target = enc.equals("emergency_contact") ? enc : enc + "_encrypted";
            if (!allCols.contains(target)) allCols.add(target);
        }
        allCols.add("ingestion_timestamp");
        allCols.add("execution_id");
        allCols.add("source_creation_time");

        StringBuilder cols = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < allCols.size(); i++) {
            if (i > 0) { cols.append(", "); placeholders.append(", "); }
            cols.append(allCols.get(i));
            placeholders.append("?");
        }
        return "INSERT INTO " + table + " (" + cols + ") VALUES (" + placeholders + ")";
    }

    public static class RecordSetter
            implements JdbcIO.PreparedStatementSetter<String>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<String> dataColumns;
        private final List<String> encryptedColumns;

        public RecordSetter(List<String> dataColumns, List<String> encryptedColumns) {
            this.dataColumns = dataColumns;
            this.encryptedColumns = encryptedColumns;
        }

        private String toDbString(JsonNode v) throws Exception {
            if (v == null || v.isMissingNode() || v.isNull()) {
                return null;
            }
            if (v.isValueNode()) {
                return v.asText();
            }
            return new ObjectMapper().writeValueAsString(v);
        }

        @Override
        public void setParameters(String element, PreparedStatement statement) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(element);
            int idx = 1;

            for (String col : dataColumns) {
                if (encryptedColumns.contains(col)) continue;
                JsonNode v = node.path(col);
                String strVal = toDbString(v);
                if (strVal == null) {
                    statement.setString(idx++, " ");
                } else {
                    statement.setString(idx++, strVal);
                }
            }

            for (String encField : encryptedColumns) {
                JsonNode v = node.path(encField);
                String strVal = toDbString(v);
                if (strVal == null) {
                    statement.setString(idx++, " ");
                } else {
                    statement.setString(idx++, strVal);
                }
            }

            String ingestTs = node.path("__ingestion_ts").asText("");
            if (ingestTs.isEmpty() || ingestTs.equals(" ")) {
                statement.setNull(idx++, java.sql.Types.TIMESTAMP);
            } else {
                statement.setTimestamp(idx++, java.sql.Timestamp.from(java.time.Instant.parse(ingestTs)));
            }

            statement.setString(idx++, node.path("__execution_id").asText(" "));

            String sourceTs = node.path("__source_ts").asText("");
            if (sourceTs.isEmpty() || sourceTs.equals(" ")) {
                statement.setNull(idx++, java.sql.Types.TIMESTAMP);
            } else {
                statement.setTimestamp(idx++, java.sql.Timestamp.from(java.time.Instant.parse(sourceTs)));
            }
        }
    }
}
