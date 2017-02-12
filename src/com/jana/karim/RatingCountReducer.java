package com.jana.karim;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hdadmin on 10/17/16.
 */
public class RatingCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    int i = 0;
    IntWritable count = new IntWritable();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        i = 0;
        for(IntWritable val : values){
            i = i + 1;
        }

        count.set(i);
        context.write(key, count);
    }
}
