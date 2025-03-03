package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MP_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
    private int n;

    @Override
    public void configure(JobConf job) {
        n = job.getInt("matrix.size", 2);
    }
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{
        String[] parts = value.toString().split(",");
        String matrixType = parts[0];

        int i = Integer.parseInt(parts[1]);
        int j = Integer.parseInt(parts[2]);
        int elem = Integer.parseInt(parts[3]);

        if(matrixType.equals("A")){
            for(int k=0; k<n; k++){
                output.collect(new Text(i+","+k),new Text("A,"+j+","+elem));
            }
        }
        else{
            for(int k=0; k<n; k++){
                output.collect(new Text(k+","+j),new Text("B,"+i+","+elem));
            }
        }
    }

}
