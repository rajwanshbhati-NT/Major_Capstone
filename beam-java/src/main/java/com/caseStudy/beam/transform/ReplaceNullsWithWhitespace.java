package com.caseStudy.beam.transform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public final class ReplaceNullsWithWhitespace
        extends PTransform<PCollection<String>, PCollection<String>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public static ReplaceNullsWithWhitespace of() {
        return new ReplaceNullsWithWhitespace();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new Cleanser()));
    }

    public static class Cleanser extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@Element String line, OutputReceiver<String> out) {
            if (line == null) { out.output(""); return; }
            String cleansed = line
                    .replaceAll("\\|null\\|", "| |")
                    .replaceAll("\\|null$",  "| ")
                    .replaceAll("^null\\|",   " |")
                    .replaceAll("\\|NULL\\|", "| |")
                    .replaceAll("\\|NULL$",   "| ")
                    .replaceAll("\\|\\|",     "| |");
            out.output(cleansed);
        }
    }
}
