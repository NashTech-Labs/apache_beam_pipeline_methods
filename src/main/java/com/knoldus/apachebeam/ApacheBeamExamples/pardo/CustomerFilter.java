package com.knoldus.apachebeam.ApacheBeamExamples.pardo;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * In this pipeline we are parsing some data like gender of the user.
 */
public class CustomerFilter extends DoFn<String,String> {
    @ProcessElement
    public void processElement(ProcessContext c){
        String line = c.element();
        String words[] = line.split(",");
        String id = words[0];
        String uid = words[1];
        String uName = words[2];
        String videoId = words[3];
        String duration = words[4];
        String startTime = words[5];
        String sex = words[6];
        String output = "";
        if (sex.equals("1")) {
            output = id+","+uid+","+uName+","
                    +videoId+","+duration+","+startTime+","+"M";
        }else if (sex.equals("2")) {
            output = id+","+uid+","+uName+","
                    +videoId+","+duration+","+startTime+","+"F";
        }else {
            System.out.println("running......");
            output = line;
        }
        if(words[2].equals("James")){
            c.output(output);
        }
    }
}
