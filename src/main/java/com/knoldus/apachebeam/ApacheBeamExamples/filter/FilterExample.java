package com.knoldus.apachebeam.ApacheBeamExamples.filter;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;

public class FilterExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pOutput = pipeline
                .apply(TextIO.read().from("src/main/resources/customer_pardo.csv"));
//        using filter
        PCollection<String> filteredData = pOutput.apply(Filter.by(new CustomerFilter()));
        filteredData.apply(TextIO
                .write()
                .to("src/main/resources/output/customer.csv")
                .withHeader("Id,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipeline.run();
    }
}
