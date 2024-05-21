package org.example;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Map extends Mapper<Text,Text,Text,Text> {
    // private static Text keyInfo

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit= (FileSplit) context.getInputSplit();
        Text word=new Text();
        Text fileName=new Text(fileSplit.getPath().getName());
        StringTokenizer itr=new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word,fileName);
        }
    }
}

