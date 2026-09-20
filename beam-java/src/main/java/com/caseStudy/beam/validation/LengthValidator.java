package com.caseStudy.beam.validation;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashSet;
import java.util.Set;

public final class LengthValidator {

    private LengthValidator() { }

    public static String validateAndNormalize(String fieldName, String value, JsonNode rule) {
        if (rule == null || rule.isMissingNode() || rule.isNull()) {
            return value;
        }
        boolean required = rule.path("required").asBoolean(false);
        if ((value == null || value.isEmpty()) && required) {
            return null;
        }
        if (value == null) { return null; }

        int min = rule.path("min_length").asInt(0);
        int max = rule.path("max_length").asInt(Integer.MAX_VALUE);

        int len = value.length();
        if (len < min || len > max) { return null; }
        return value;
    }

    public static Set<String> requiredFieldNames(JsonNode fieldsNode) {
        Set<String> required = new HashSet<>();
        if (fieldsNode == null) { return required; }
        fieldsNode.fields().forEachRemaining(e -> {
            if (e.getValue().path("required").asBoolean(false)) {
                required.add(e.getKey());
            }
        });
        return required;
    }
}
