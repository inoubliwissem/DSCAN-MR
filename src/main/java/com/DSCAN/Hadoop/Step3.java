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

public class Step3 {
    public static class Step3Mapper
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

    public static class Step3Reducer
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
                int nb=0;
                // if the current vertex is not core
                if(currentVertex.getNbStrongConnections().get()>=1 && !currentVertex.getNodeType().toString().equals("c")) {
                    boolean exit = false;
                    int i = 0;
                    while (i < parts.length && !exit) {
                        Vertex n = new Vertex(parts[i]);
                        // if one vetex has a core vertex neighbor and at least one strong conection
                        if (n.getNodeType().toString().equals("c") ){
                            currentVertex.setNodeType(new Text("b"));
                            exit=true;
                        }
                        i++;
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
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(Step3.Step3Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job.setReducerClass(Step3.Step3Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job, new Path("adj2"));
        FileOutputFormat.setOutputPath(job, new Path("step3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
