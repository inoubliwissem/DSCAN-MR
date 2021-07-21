package com.DSCAN.Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Step5 {
    public static class Step5Mapper
            extends Mapper<Object, Text, IntWritable, Vertex> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String parts[] = value.toString().split("\t");
            Vertex vertex = new Vertex(parts[1]);
            for (IntWritable v : vertex.getNeighbors()) {
                context.write(v, vertex);
            }
        }
    }

    public static class Step5Reducer
            extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {
        public void reduce(IntWritable key, Iterable<Vertex> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<Vertex> listOfNeighbors=new HashSet<Vertex>();
            List<Vertex> currentVertexList=new ArrayList<>();
            try {
                StringBuilder sb=new StringBuilder();
                StringBuilder currentv=new StringBuilder();
                for (Vertex n:values) {
                    sb.append(n.toString()+";");
                    if(key.get()==n.getId().get()){
                        currentv.append(n.toString());
                    }
                }
                String parts[]=sb.toString().split(";");
                Vertex currentVertex=new Vertex(currentv.toString());
                // if the current vertex is free does not belong to any cluster
                if(currentVertex.getCluster().get()==-1){
                    Set<Integer> clusters=new HashSet<>();
                    for(int i=0;i<parts.length;i++){
                        Vertex n = new Vertex(parts[i]);
                        if(n.getCluster().get()!=-1){
                            clusters.add(n.getCluster().get());
                        }
                    }
                    if(clusters.size()>1) {
                        currentVertex.setCluster(new IntWritable(-2));
                    }
                }

                context.write(key,currentVertex);

            }catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(Step5.Step5Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job.setReducerClass(Step5.Step5Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job, new Path("adj4"));
        FileOutputFormat.setOutputPath(job, new Path("step5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

