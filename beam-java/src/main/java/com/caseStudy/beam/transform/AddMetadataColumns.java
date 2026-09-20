package com.caseStudy.beam.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.time.Instant;


public final class AddMetadataColumns
        extends PTransform<PCollection<String>, PCollection<String>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String executionId;
    private final String ingestionTimestamp;
    private final String sourceCreationTime;

    private AddMetadataColumns(String executionId, String ingestionTs, String sourceTs) {
        this.executionId        = executionId;
        this.ingestionTimestamp = ingestionTs;
        this.sourceCreationTime = sourceTs;
    }

    public static AddMetadataColumns of(String executionId, Instant ingestionTs, Instant sourceTs) {
        return new AddMetadataColumns(executionId, ingestionTs.toString(), sourceTs.toString());
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new Enricher(executionId, ingestionTimestamp, sourceCreationTime)));
    }

    public static class Enricher extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String executionId;
        private final String ingestionTs;
        private final String sourceTs;

        public Enricher(String executionId, String ingestionTs, String sourceTs) {
            this.executionId = executionId;
            this.ingestionTs = ingestionTs;
            this.sourceTs    = sourceTs;
        }

        @ProcessElement
        public void process(@Element String jsonLine, OutputReceiver<String> out) {
            try {
                ObjectNode node = (ObjectNode) MAPPER.readTree(jsonLine);
                node.put("__ingestion_ts", ingestionTs);
                node.put("__execution_id", executionId);
                node.put("__source_ts",    sourceTs);
                out.output(MAPPER.writeValueAsString(node));
            } catch (Exception e) {
                out.output("PARSE_ERROR|metadata|" + e.getMessage() + "|" + jsonLine);
            }
        }
    }
}
