package org.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MP_Reducer extends MapReduceBase implements Reducer<Text,Text,Text,IntWritable>{

    public void reduce(Text key, java.util.Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        Map<Integer, Integer> A = new HashMap<>();
        Map<Integer, Integer> B = new HashMap<>();

        while (values.hasNext()) {
            String[] parts = values.next().toString().split(",");
            int index = Integer.parseInt(parts[1]);
            int val = Integer.parseInt(parts[2]);

            if (parts[0].equals("A")) {
                A.put(index, val);
            } else {
                B.put(index, val);
            }
        }

        int sum = 0;
        for (int k : A.keySet()) {
            if (B.containsKey(k)) {
                sum += A.get(k) * B.get(k);
            }
        }

        output.collect(key, new IntWritable(sum));
    }
}
