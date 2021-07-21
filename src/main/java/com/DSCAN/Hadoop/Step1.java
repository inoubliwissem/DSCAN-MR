package com.DSCAN.Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step1 {


    public static class Step1Mapper
            extends Mapper<Object, Text, IntWritable, Vertex> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String parts[] = value.toString().split("\t");
            Vertex v1 = new Vertex();
            IntWritable idv1 = new IntWritable(Integer.parseInt(parts[0]));
            v1.setId(new IntWritable(Integer.parseInt(parts[0])));
            v1.addNeighbor(new IntWritable(Integer.parseInt(parts[0])));
            v1.addNeighbor(new IntWritable(Integer.parseInt(parts[1])));

            Vertex v2 = new Vertex();
            IntWritable idv2 = new IntWritable(Integer.parseInt(parts[1]));
            v2.setId(new IntWritable(Integer.parseInt(parts[1])));
            v2.addNeighbor(new IntWritable(Integer.parseInt(parts[0])));
            v2.addNeighbor(new IntWritable(Integer.parseInt(parts[1])));
            context.write(idv1,v1);
            context.write(idv2,v2);
        }
    }
    public static class Step1Reducer
            extends Reducer<IntWritable,Vertex,IntWritable,Vertex> {

        public void reduce(IntWritable key, Iterable<Vertex> values,
                           Context context
        ) throws IOException, InterruptedException {
            Vertex v=new Vertex();
            v.setId(key);
            for (Vertex vx:values){
                v.addNeighbors(vx.getNeighbors());
            }
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws Exception {
        //Configuration conf = new Configuration();

        //conf.set
      JobConf jconf = new JobConf(new Configuration(), Step1.class);
      jconf.setNumReduceTasks(2);
      jconf.setNumMapTasks(2);
      Job job = Job.getInstance(jconf,"ss");
        job.setJarByClass(Step1.class);

        job.setMapperClass(Step1Mapper.class);
        job.setCombinerClass(Step1.Step1Reducer.class);
        job.setReducerClass(Step1.Step1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job, new Path("graph"));
        FileOutputFormat.setOutputPath(job, new Path("step1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
