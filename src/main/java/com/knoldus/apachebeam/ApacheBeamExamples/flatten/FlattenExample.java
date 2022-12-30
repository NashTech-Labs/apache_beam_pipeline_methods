package com.knoldus.apachebeam.ApacheBeamExamples.flatten;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input1 = pipeline.apply(TextIO
                .read()
                .from("src/main/resources/flatten_input/customer_1.csv"));
        PCollection<String> input2 = pipeline.apply(TextIO
                .read()
                .from("src/main/resources/flatten_input/customer_2.csv"));
        PCollection<String> input3 = pipeline.apply(TextIO
                .read()
                .from("src/main/resources/flatten_input/customer_3.csv"));
//        using pcollectionList
        PCollectionList<String> pCollectionList = PCollectionList.of(input1).and(input2).and(input3);
        PCollection<String> mergedOutput = pCollectionList.apply(Flatten.pCollections());
        mergedOutput.apply(TextIO
                .write()
                .to("src/main/resources/output/merged_output")
                .withHeader("Id,Username,Lastname,city")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipeline.run().waitUntilFinish();
    }
}
