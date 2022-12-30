package com.knoldus.apachebeam.ApacheBeamExamples.capitalizewords;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Locale;

/**
 * This pipeline will capitalize the whole paragraph.
 */
public class CapitalizeWords {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply("Read", TextIO
                .read()
                .from("src/main/resources/txt.csv"));
        pCollection.apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via((String str)-> str.toUpperCase(Locale.ROOT)))
        .apply(TextIO
                .write()
                .to("src/main/resources/output1")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipeline.run();
    }
}
