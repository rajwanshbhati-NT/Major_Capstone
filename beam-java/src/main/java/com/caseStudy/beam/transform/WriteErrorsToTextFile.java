package com.caseStudy.beam.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.Serializable;
import java.time.Instant;

public final class WriteErrorsToTextFile
        extends PTransform<PCollection<String>, PCollection<String>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final TupleTag<String> ERROR_TAG = new TupleTag<String>() { };
    private static final TupleTag<String> MAIN_TAG = new TupleTag<String>() { };

    private final String errorFilePath;

    private WriteErrorsToTextFile(String errorFilePath) {
        this.errorFilePath = errorFilePath;
    }

    public static WriteErrorsToTextFile of(String errorFilePath) {
        return new WriteErrorsToTextFile(errorFilePath);
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {

        String writePrefix = errorFilePath.endsWith(".txt")
                ? errorFilePath.substring(0, errorFilePath.length() - 4)
                : errorFilePath;

        PCollectionTuple split = input.apply("RouteErrors", ParDo
                .of(new RouteFn())
                .withOutputTags(MAIN_TAG, TupleTagList.of(ERROR_TAG)));

        split.get(ERROR_TAG)
                .apply("FormatErrorMessages", ParDo.of(new FormatFn()))
                .apply("WriteErrorsToFile",
                        TextIO.write()
                                .to(writePrefix)
                                .withSuffix(".txt")
                                .withNumShards(1));

        return split.get(MAIN_TAG);
    }

    public static class RouteFn extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@DoFn.Element String line,
                            MultiOutputReceiver out) {
            if (line == null) { return; }

            if (line.startsWith("PARSE_ERROR|") || line.startsWith("VALIDATION_ERROR|")) {
                out.get(ERROR_TAG).output(line);
            } else if (line.startsWith("ERROR|") || line.startsWith("BLANK|")) {
                out.get(ERROR_TAG).output(line);
            } else {
                out.get(MAIN_TAG).output(line);
            }
        }
    }

    public static class FormatFn extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@DoFn.Element String line, DoFn.OutputReceiver<String> out) {
            out.output(formatError(line));
        }

        private String formatError(String line) {
            String[] parts = line.split("\\|", -1);
            StringBuilder sb = new StringBuilder();

            String originalLine = "";
            if (parts.length >= 5 && parts[0].equals("VALIDATION_ERROR")) {
                originalLine = parts[4];
            } else if (parts.length >= 4 && parts[0].equals("PARSE_ERROR")) {
                originalLine = parts[3];
            } else if (parts.length >= 3 && parts[0].equals("ERROR")) {
                originalLine = parts.length > 2 ? parts[2] : "";
            } else if (parts.length >= 2 && parts[0].equals("BLANK")) {
                originalLine = parts[1];
            }

            sb.append("Timestamp     : ").append(Instant.now()).append("\n");

            String errorType = "UNKNOWN";
            String stage = "";
            String field = "";
            String message = "";

            if (parts.length >= 4 && parts[0].equals("PARSE_ERROR")) {
                errorType = "PARSE_ERROR";
                stage = parts[1];
                message = "Could not parse the row: " + parts[2];
            } else if (parts.length >= 5 && parts[0].equals("VALIDATION_ERROR")) {
                errorType = "VALIDATION_ERROR";
                stage = parts[1];
                field = parts[2];
                message = humanizeValidationError(parts[2], parts[3]);
            } else if (parts.length >= 3 && parts[0].equals("ERROR")) {
                errorType = "ERROR";
                message = parts[1];
            } else if (parts.length >= 2 && parts[0].equals("BLANK")) {
                errorType = "BLANK_ROW";
                message = "Row is empty - all required fields are blank";
            } else {
                errorType = "UNKNOWN";
                message = line;
            }

            sb.append("Error Type    : ").append(errorType).append("\n");
            if (!stage.isEmpty())   sb.append("Stage         : ").append(stage).append("\n");
            if (!field.isEmpty())   sb.append("Field         : ").append(field).append("\n");
            sb.append("Message       : ").append(message).append("\n");
            if (!originalLine.isEmpty()) {
                String preview = originalLine.length() > 200
                        ? originalLine.substring(0, 200) + "..."
                        : originalLine;
                sb.append("Data          : ").append(preview).append("\n");
            }

            return sb.toString();
        }

        private String humanizeValidationError(String field, String reason) {
            if (field == null || field.isEmpty()) {
                return "Validation failed: " + reason;
            }
            if ("empty".equals(reason)) {
                return "Required field '" + field + "' is empty - cannot be blank";
            }
            if ("null".equals(reason)) {
                return "Required field '" + field + "' is null";
            }
            if (reason != null && reason.startsWith("json_parse_failed:")) {
                return "Row is not valid JSON: " + reason.substring("json_parse_failed:".length());
            }
            return "Field '" + field + "' validation failed: " + reason;
        }
    }
}