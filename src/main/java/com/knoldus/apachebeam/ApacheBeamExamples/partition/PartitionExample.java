package com.knoldus.apachebeam.ApacheBeamExamples.partition;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * In this pipeline we are creating different file for users living in the same city.
 */
public class PartitionExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input1 = pipeline.apply(TextIO
                .read()
                .from("src/main/resources/partition_input/input.csv"));
        PCollectionList<String> partitionList = input1
                .apply(Partition.of(3, new MyCityPartition()));
        PCollection<String> pCollection = partitionList.get(0);
        PCollection<String> pCollection1 = partitionList.get(1);
        PCollection<String> pCollection2 = partitionList.get(2);
        pCollection.apply(TextIO
                .write()
                .to("src/main/resources/output/partition_output/partition")
                .withHeader("Id,Username,Lastname,city")
                .withNumShards(1)
                .withSuffix(".csv"));
        
        pCollection1.apply(TextIO
                .write()
                .to("src/main/resources/output/partition_output/partition1")
                .withHeader("Id,Username,Lastname,city")
                .withNumShards(1)
                .withSuffix(".csv"));
        pCollection2.apply(TextIO
                .write()
                .to("src/main/resources/output/partition_output/partition2")
                .withHeader("Id,Username,Lastname,city")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipeline.run().waitUntilFinish();
    }
}
