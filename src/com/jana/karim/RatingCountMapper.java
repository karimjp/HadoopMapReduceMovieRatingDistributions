package com.jana.karim;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hdadmin on 10/17/16.
 */
public class RatingCountMapper extends Mapper <LongWritable, Text, Text, IntWritable>{
    IntWritable one = new IntWritable(1);
    Text rating_number = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] result = line.split("\t");
        rating_number.set(result[2]);
        context.write(rating_number, one);


    }
}
