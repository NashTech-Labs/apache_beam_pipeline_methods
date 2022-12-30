package com.knoldus.apachebeam.ApacheBeamExamples.capitalizewords;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * In this pipeline we are getting data of users having city Los Angeles
 */
public class FilterBeam {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pOutput = pipeline
                .apply(TextIO.read().from("src/main/resources/customer.csv"));
        pOutput.apply(Filter.by(new ProcessFunction<String, Boolean>() {
                    /**
                     * @param input line as string
                     * @return line that contains city Los Angeles
                     * @throws Exception
                     */
            @Override
            public Boolean apply(String input) throws Exception {
                System.out.println("*****"+input);
                return input.contains("Los Angeles");
            }
        }))
                .apply(TextIO.write()
                        .to("src/main/resources/customer")
                        .withHeader("id,name,last name, city")
                        .withNumShards(1)
                        .withSuffix(".csv"));
        pipeline.run();
        
    }
}
