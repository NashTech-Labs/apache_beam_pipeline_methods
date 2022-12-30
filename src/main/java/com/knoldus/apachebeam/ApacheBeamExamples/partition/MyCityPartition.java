package com.knoldus.apachebeam.ApacheBeamExamples.partition;

import org.apache.beam.sdk.transforms.Partition;

public class MyCityPartition implements Partition.PartitionFn<String> {
    @Override
    public int partitionFor(String elem, int numPartitions) {
        String arr[] = elem.split(",");
        if(arr[3].equals("Los Angeles")) {
            return 0;
        } else if(arr[3].equals("Phoenix")){
            return 1;
        } else{
            return 2;
        }
    }
}
