package com.caseStudy.beam.parser;

import com.caseStudy.beam.config.IngestionConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class FileParserFactory {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final XmlMapper   XML_MAPPER  = new XmlMapper();

    private FileParserFactory() { }

    public static PTransform<PBegin, PCollection<String>> readSource(IngestionConfig config) {
        return TextIO.read().from(config.getSourceFilePath());// Beam's built-in file reader
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static PTransform<PCollection<String>, PCollection<String>> parseRecords(IngestionConfig config) {
        switch (config.getFileFormat()) {
            case "CSV":
                return (PTransform) ParDo.of(new CsvLineParser(config));
            case "FIXEDWIDTH":
                return (PTransform) ParDo.of(new FixedWidthLineParser(config));
            case "JSON":
                return (PTransform) ParDo.of(new JsonLineParser(config));
            case "XML":
                return (PTransform) ParDo.of(new XmlLineParser(config));
            default:
                throw new IllegalArgumentException("Unsupported file format: " + config.getFileFormat());
        }
    }

    private static String[] readSchemaFieldNames(IngestionConfig config) {
        try {
            JsonNode schema = JSON_MAPPER.readTree(new File(config.getSchemaFilePath()));
            JsonNode fields = schema.path("fields");
            if (!fields.isObject()) return null;
            List<String> names = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> it = fields.fields();
            while (it.hasNext()) {
                names.add(it.next().getKey());
            }
            return names.toArray(new String[0]);
        } catch (Exception e) {
            return null;
        }
    }


    public static class CsvLineParser extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final IngestionConfig config;
        private String delimiter;
        private boolean hasHeader;
        private String[] fieldNames;

        public CsvLineParser(IngestionConfig config) {
            this.config = config;
            try {
                JsonNode schema = JSON_MAPPER.readTree(new File(config.getSchemaFilePath()));
                this.delimiter = schema.path("delimiter").asText(",");
                this.hasHeader = schema.path("csv_header").asBoolean(true);
                this.fieldNames = readSchemaFieldNames(config);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read CSV schema", e);
            }
        }
        // Call this method for EACH element in PCollection
        @ProcessElement
        public void process(@Element String line, OutputReceiver<String> out) {
            try {
                if (line == null) return;
                if (hasHeader && line.startsWith("employee_id,")) return;
                if (line.trim().isEmpty()) {
                    out.output("BLANK|");
                    return;
                }
                String[] parts = line.split(delimiter, -1);
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < parts.length; i++) {
                    if (i > 0) sb.append(",");
                    String name = (fieldNames != null && i < fieldNames.length)
                            ? fieldNames[i] : "col" + (i + 1);
                    String val = parts[i];

                    if (name.equals("skills")) {
                        String[] skills = val.split(";");
                        StringBuilder arr = new StringBuilder("[");
                        for (int j = 0; j < skills.length; j++) {
                            if (j > 0) arr.append(",");
                            arr.append("\"").append(skills[j].trim()).append("\"");
                        }
                        arr.append("]");
                        sb.append("\"skills\":").append(arr);
                    } else if (name.equals("address")) {
                        sb.append("\"address\":{\"raw\":").append(jsonEscape(val)).append("}");
                    } else if (name.equals("emergency_contact")) {
                        sb.append("\"emergency_contact\":{\"raw\":").append(jsonEscape(val)).append("}");
                    } else if (name.equals("col14") || name.equals("col15") || name.equals("col16")) {
                        if (name.equals("col14")) {
                            String[] skills = val.split(";");
                            StringBuilder arr = new StringBuilder("[");
                            for (int j = 0; j < skills.length; j++) {
                                if (j > 0) arr.append(",");
                                arr.append("\"").append(skills[j].trim()).append("\"");
                            }
                            arr.append("]");
                            sb.append("\"skills\":").append(arr);
                        } else if (name.equals("col15")) {
                            sb.append("\"address\":{\"raw\":").append(jsonEscape(val)).append("}");
                        } else {
                            sb.append("\"emergency_contact\":{\"raw\":").append(jsonEscape(val)).append("}");
                        }
                    } else {
                        sb.append("\"").append(name).append("\":").append(jsonEscape(val));
                    }
                }
                sb.append("}");
                out.output(sb.toString());
            } catch (Exception e) {
                out.output("PARSE_ERROR|csv|" + e.getMessage() + "|" + line);
            }
        }
    }


    public static class FixedWidthLineParser extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final IngestionConfig config;
        private int[] starts;
        private int[] ends;
        private String[] fieldNames;

        public FixedWidthLineParser(IngestionConfig config) {
            this.config = config;
            try {
                JsonNode schema = JSON_MAPPER.readTree(new File(config.getSchemaFilePath()));
                JsonNode cols = schema.path("columns");
                starts = new int[cols.size()];
                ends   = new int[cols.size()];
                int i = 0;
                for (JsonNode c : cols) {
                    starts[i] = c.path("start").asInt() - 1;
                    ends[i]   = starts[i] + c.path("length").asInt();
                    i++;
                }
                List<String> names = new ArrayList<>();
                for (JsonNode c : cols) names.add(c.path("name").asText("col" + (names.size() + 1)));
                this.fieldNames = names.toArray(new String[0]);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read fixed-width schema", e);
            }
        }

        @ProcessElement
        public void process(@Element String line, OutputReceiver<String> out) {
            try {
                if (line == null || line.isEmpty()) return;
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < ends.length; i++) {
                    if (i > 0) sb.append(",");
                    int s = Math.min(starts[i], line.length());
                    int e = Math.min(ends[i], line.length());
                    String name = (fieldNames != null && i < fieldNames.length)
                            ? fieldNames[i] : "col" + (i + 1);
                    sb.append("\"").append(name).append("\":")
                            .append(jsonEscape(line.substring(s, e)));
                }
                sb.append("}");
                out.output(sb.toString());
            } catch (Exception ex) {
                out.output("PARSE_ERROR|fixedwidth|" + ex.getMessage() + "|" + line);
            }
        }
    }

    public static class JsonLineParser extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final IngestionConfig config;
        private final boolean isJsonArray;
        private final String sourceFilePath;
        private boolean emitted = false;

        public JsonLineParser(IngestionConfig config) {
            this.config = config;
            this.sourceFilePath = config.getSourceFilePath();
            boolean arr = false;
            try {
                JsonNode schema = JSON_MAPPER.readTree(new File(config.getSchemaFilePath()));
                arr = schema.path("is_json_array").asBoolean(false);
            } catch (Exception ignored) {}
            this.isJsonArray = arr;
        }

        @ProcessElement
        public void process(@Element String line, OutputReceiver<String> out) {
            try {
                if (line == null) return;
                if (isJsonArray) {
                    if (emitted) return;
                    if (line.trim().isEmpty()) return;
                    String whole = new String(Files.readAllBytes(Paths.get(sourceFilePath)));
                    JsonNode node = JSON_MAPPER.readTree(whole);
                    if (node.isArray()) {
                        for (JsonNode item : node)
                            if (item.isObject()) out.output(JSON_MAPPER.writeValueAsString(item));
                    } else if (node.isObject()) {
                        out.output(JSON_MAPPER.writeValueAsString(node));
                    }
                    emitted = true;
                } else {
                    if (line.trim().isEmpty()) return;
                    JsonNode node = JSON_MAPPER.readTree(line);
                    out.output(JSON_MAPPER.writeValueAsString(node));
                }
            } catch (Exception e) {
                if (!isJsonArray) out.output("PARSE_ERROR|json|" + e.getMessage() + "|" + line);
            }
        }
    }


    public static class XmlLineParser extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private final IngestionConfig config;
        private final String rowTag;
        private final String sourceFilePath;
        private boolean emitted = false;

        public XmlLineParser(IngestionConfig config) {
            this.config = config;
            this.sourceFilePath = config.getSourceFilePath();
            String tag = "row";
            try {
                JsonNode schema = JSON_MAPPER.readTree(new File(config.getSchemaFilePath()));
                tag = schema.path("row_tag").asText("row");
            } catch (Exception e) { tag = "row"; }
            this.rowTag = tag;
        }

        @ProcessElement
        public void process(@Element String line, OutputReceiver<String> out) {
            try {
                if (line == null) return;
                if (emitted) return;
                if (line.trim().isEmpty()) return;
                String whole = new String(Files.readAllBytes(Paths.get(sourceFilePath)));
                String openTag = "<" + rowTag + ">";
                String closeTag = "</" + rowTag + ">";
                int idx = 0;
                while ((idx = whole.indexOf(openTag, idx)) != -1) {
                    int end = whole.indexOf(closeTag, idx);
                    if (end < 0) break;
                    String xml = whole.substring(idx, end + closeTag.length());
                    try {
                        JsonNode node = XML_MAPPER.readTree(xml);
                        StringBuilder sb = new StringBuilder("{");
                        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
                        boolean first = true;
                        while (it.hasNext()) {
                            Map.Entry<String, JsonNode> e = it.next();
                            if (!first) sb.append(",");
                            first = false;
                            sb.append("\"").append(e.getKey()).append("\":");
                            JsonNode v = e.getValue();
                            if (v.isNull() || v.isMissingNode()) sb.append("\"\"");
                            else if (v.isValueNode())           sb.append(jsonEscape(v.asText()));
                            else                                sb.append(v.toString());
                        }
                        sb.append("}");
                        out.output(sb.toString());
                    } catch (Exception e) {
                        out.output("PARSE_ERROR|xml|" + e.getMessage() + "|" + xml);
                    }
                    idx = end + closeTag.length();
                }
                emitted = true;
            } catch (Exception e) {
                out.output("PARSE_ERROR|xml|" + e.getMessage() + "|" + line);
                emitted = true;
            }
        }
    }

    private static String jsonEscape(String raw) {
        if (raw == null) return "\"\"";
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:   sb.append(c);
            }
        }
        sb.append("\"");
        return sb.toString();
    }
}