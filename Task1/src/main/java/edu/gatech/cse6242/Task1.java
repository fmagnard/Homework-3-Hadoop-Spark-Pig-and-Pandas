package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {
    
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{
        
        private Text nodeText = new Text();
        private final static IntWritable weightWritable = new IntWritable(0);
        
        public void map(Object key, Text value, Context context
                        ) throws java.io.IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String node = line[0];
            nodeText.set(node);
            int weight = Integer.parseInt(line[2]);
            weightWritable.set(weight);
            context.write(nodeText, weightWritable);
        }
    }


public static class IntSumReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result= new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                          Context context
                          ) throws java.io.IOException, InterruptedException {
        int strongweight=0;
        for (IntWritable val : values) {
            if(val.get()>strongweight){
                strongweight=val.get();
            }
        }
        result.set(strongweight);
        context.write(key, result);
    }
}




public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Task1");

/* TODO: Needs to be implemented */
job.setJarByClass(Task1.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));


job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);
job.setReducerClass(IntSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);




System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
