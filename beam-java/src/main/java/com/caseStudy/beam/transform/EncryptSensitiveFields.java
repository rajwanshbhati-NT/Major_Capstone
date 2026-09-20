package com.caseStudy.beam.transform;

import com.caseStudy.beam.security.EncryptionService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.Serializable;
import java.util.List;


public final class EncryptSensitiveFields
        extends PTransform<PCollection<String>, PCollection<String>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final EncryptionService encryptionService;
    private final List<String> sensitiveFields;

    private EncryptSensitiveFields(EncryptionService service, List<String> sensitiveFields) {
        this.encryptionService = service;
        this.sensitiveFields   = sensitiveFields;
    }

    public static EncryptSensitiveFields of(EncryptionService service, List<String> fields) {
        return new EncryptSensitiveFields(service, fields);
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new Encrypter(encryptionService, sensitiveFields)));
    }

    public static class Encrypter extends DoFn<String, String> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final EncryptionService encryptionService;
        private final List<String> sensitiveFields;

        public Encrypter(EncryptionService service, List<String> fields) {
            this.encryptionService = service;
            this.sensitiveFields   = fields;
        }

        @ProcessElement
        public void process(@Element String jsonLine, OutputReceiver<String> out) {
            try {
                ObjectNode node = (ObjectNode) MAPPER.readTree(jsonLine);
                for (String field : sensitiveFields) {
                    if (node.has(field) && !node.get(field).isNull()) {
                        JsonNode fieldValue = node.get(field);
                        String plain;

                        // For nested object (e.g., emergency_contact with phone inside):
                        if (fieldValue.isObject()) {
                            ObjectNode nested = (ObjectNode) fieldValue.deepCopy();
                            // Encrypt inner "phone" field if exists
                            if (nested.has("phone") && !nested.get("phone").isNull()) {
                                String innerPhone = nested.get("phone").asText();
                                if (!innerPhone.isEmpty()) {
                                    nested.put("phone", encryptionService.encrypt(innerPhone));
                                }
                            }
                            node.set(field, nested);
                            continue;
                        }
                        // For arrays: encrypt each element's "phone" field
                        else if (fieldValue.isArray()) {
                            ArrayNode arr = MAPPER.createArrayNode();
                            for (JsonNode elem : fieldValue) {
                                if (elem.isObject() && elem.has("phone") && !elem.get("phone").isNull()) {
                                    ObjectNode elemCopy = (ObjectNode) elem.deepCopy();
                                    String innerPhone = elemCopy.get("phone").asText();
                                    if (!innerPhone.isEmpty()) {
                                        elemCopy.put("phone", encryptionService.encrypt(innerPhone));
                                    }
                                    arr.add(elemCopy);
                                } else {
                                    arr.add(elem);
                                }
                            }
                            node.set(field, arr);
                            continue;
                        }
                        // For simple value (text/number/bool):
                        else {
                            plain = fieldValue.asText();
                            if (!plain.isEmpty()) {
                                node.put(field, encryptionService.encrypt(plain));
                            }
                        }
                    }
                }
                out.output(MAPPER.writeValueAsString(node));
            } catch (Exception e) {
                out.output("PARSE_ERROR|encrypt|" + e.getMessage() + "|" + jsonLine);
            }
        }
    }
}
