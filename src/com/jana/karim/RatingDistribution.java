package com.jana.karim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class RatingDistribution {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{


        String hdfs_source_data_arg= args[1];
        //String hdfs_source_data_arg = "/movie-ratings/u1.data";
        Path hdfs_source_data_path =  new Path(hdfs_source_data_arg);
        String hdfs_destination_data_arg = args[2];
        //String hdfs_destination_data_arg = "/movie-rating-distribution/";
        Path hdfs_destination_data_path = new Path(hdfs_destination_data_arg);

        Configuration conf = new Configuration();
        System.out.println(conf);
        System.out.println(conf.get("fs.default.name"));

        //Configure the Job
        Job job = Job.getInstance(conf, "RatingDistribution");

        //Configure the Job Input  ( Can call multiple times for file, dir, or pattern)
        //Specify the Input path
        TextInputFormat.addInputPath(job, hdfs_source_data_path);
        //Specify the Input data format
        job.setInputFormatClass(TextInputFormat.class);

        //Configure the Job Process
        job.setMapperClass(RatingCountMapper.class);
        job.setReducerClass(RatingCountReducer.class);

        //Set the Jar file
        job.setJarByClass(RatingDistribution.class);

        //Configure the Job Output
        FileOutputFormat.setOutputPath(job, hdfs_destination_data_path);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Specify the Output key-value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setNumReduceTasks(0);

        //Submit the job
        job.waitForCompletion(true);

    }
}
