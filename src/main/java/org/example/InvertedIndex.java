package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
    public static void main(String[]args){
        try {
            Configuration configuration= new Configuration();
            Job job=Job.getInstance(configuration);
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(myreduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job,new Path("/exp2"));
            FileOutputFormat.setOutputPath(job,new Path("/exp2/output"));
            System.exit(job.waitForCompletion(true)?0:1);
        }catch (Exception ignored){

        }
    }
}
