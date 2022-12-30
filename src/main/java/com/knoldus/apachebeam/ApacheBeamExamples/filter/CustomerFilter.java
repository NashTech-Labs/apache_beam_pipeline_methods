package com.knoldus.apachebeam.ApacheBeamExamples.filter;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class CustomerFilter implements SerializableFunction<String, Boolean> {
    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}
