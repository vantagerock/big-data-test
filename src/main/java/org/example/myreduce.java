package org.example;

import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myreduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator= values.iterator();
        StringBuilder all=new StringBuilder();
        if(iterator.hasNext())all.append(iterator.next().toString());
        while (iterator.hasNext()){
            all.append(';');
            all.append(iterator.next().toString());
        }
        context.write(key,new Text(all.toString()));
    }
}
