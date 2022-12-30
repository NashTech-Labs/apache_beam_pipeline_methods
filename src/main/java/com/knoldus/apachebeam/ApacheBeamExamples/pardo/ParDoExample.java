package com.knoldus.apachebeam.ApacheBeamExamples.pardo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pOutput = pipeline
                .apply(TextIO.read().from("src/main/resources/user.csv"));
//        using parDo
        PCollection<String> filteredData = pOutput.apply(ParDo.of(new CustomerFilter()));
        filteredData.apply(TextIO
                .write()
                .to("src/main/resources/output.csv")
                .withNumShards(1)
                .withSuffix(".csv")
                .withHeader("Id,UserId,UserName,VideoId,Duration,StartTime,Sex")
        );
        pipeline.run().waitUntilFinish();
    }
}
