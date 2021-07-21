package com.DSCAN.Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GlobalPipe {

    public static void main(String[] args) throws Exception {

        if(args.length!=5){
            System.out.println("You should specify some parameters: 1: hdfs input, 2: output, 3:temp");
            System.exit(1);
        }
        System.out.println("start");
        long startTime = System.nanoTime();
        String input=args[0];
        String output=args[1];
        String temp=args[2];
        int nbmap=Integer.parseInt(args[3]);
        int nbreduce=Integer.parseInt(args[4]);
        // step 1
        //Configuration conf = new Configuration();
        //Job job = Job.getInstance(conf, "Step1");
        JobConf jconf = new JobConf(new Configuration(), Step1.class);
        jconf.setNumReduceTasks(nbreduce);
        jconf.setNumMapTasks(nbmap);
        Job job = Job.getInstance(jconf,"Step1");

        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.Step1Mapper.class);
        job.setCombinerClass(Step1.Step1Reducer.class);
        job.setReducerClass(Step1.Step1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(temp+"/step1"));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
        // step 2
        JobConf jconf2 = new JobConf(new Configuration(), Step2.class);
        jconf2.setNumReduceTasks(nbreduce);
        jconf2.setNumMapTasks(nbmap);
        Job job2 = Job.getInstance(jconf2,"Step2");
        job2.setJarByClass(Step2.class);
        job2.setMapperClass(Step2.Step2Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job2.setReducerClass(Step2.Step2Reducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job2, new Path(temp+"/step1/part-r-*"));
        FileOutputFormat.setOutputPath(job2, new Path(temp+"/step2"));
        //System.exit(job2.waitForCompletion(true) ? 0 : 1);
        job2.waitForCompletion(true);
        //setp3
        JobConf jconf3 = new JobConf(new Configuration(), Step3.class);
        jconf3.setNumReduceTasks(nbreduce);
        jconf3.setNumMapTasks(nbmap);
        Job job3 = Job.getInstance(jconf3,"Step3");
        job3.setJarByClass(Step3.class);
        job3.setMapperClass(Step3.Step3Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job3.setReducerClass(Step3.Step3Reducer.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job3, new Path(temp+"/step2/part-r-*"));
        FileOutputFormat.setOutputPath(job3, new Path(temp+"/step3"));
        //System.exit(job3.waitForCompletion(true) ? 0 : 1);
        job3.waitForCompletion(true);
        // step 4
        JobConf jconf4 = new JobConf(new Configuration(), Step4.class);
        jconf4.setNumReduceTasks(nbreduce);
        jconf4.setNumMapTasks(nbmap);
        Job job4 = Job.getInstance(jconf4,"Step4");
        job4.setJarByClass(Step4.class);
        job4.setMapperClass(Step4.Step4Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job4.setReducerClass(Step4.Step4Reducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job4, new Path(temp+"/step3/part-r-*"));
        FileOutputFormat.setOutputPath(job4, new Path(temp+"/step4"));
        //System.exit(job4.waitForCompletion(true) ? 0 : 1);
        job4.waitForCompletion(true);
        //step5
        JobConf jconf5 = new JobConf(new Configuration(), Step5.class);
        jconf5.setNumReduceTasks(nbreduce);
        jconf5.setNumMapTasks(nbmap);
        Job job5 = Job.getInstance(jconf5,"Step5");
        job5.setJarByClass(Step5.class);
        job5.setMapperClass(Step5.Step5Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job5.setReducerClass(Step5.Step5Reducer.class);
        job5.setOutputKeyClass(IntWritable.class);
        job5.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job5, new Path(temp+"/step4/part-r-*"));
        FileOutputFormat.setOutputPath(job5, new Path(output));
        //System.exit(job5.waitForCompletion(true) ? 0 : 1);
        job5.waitForCompletion(true);

        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println("The running time is "+(totalTime/1000000000));
    }
}
