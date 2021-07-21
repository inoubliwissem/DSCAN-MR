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
import java.util.*;

public class Step2 {

    public static class Step2Mapper
            extends Mapper<Object, Text, IntWritable, Vertex> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String parts[] = value.toString().split("\t");
            Vertex vertex = new Vertex(parts[1]);
            for (IntWritable v : vertex.getNeighbors()) {
               // System.out.println("key map : "+v+" vertex :"+vertex.toString());
                context.write(v, vertex);
            }
        }
    }

    public static class Step2Reducer
            extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {

        public void reduce(IntWritable key, Iterable<Vertex> values,
                           Context context
        ) throws IOException, InterruptedException {
       //   List<Vertex> listOfNeighbors = new ArrayList<>();
            Set<Vertex> listOfNeighbors=new HashSet<Vertex>();
          List<Vertex> currentVertexList=new ArrayList<>();

            try {

                StringBuilder sb=new StringBuilder();
                StringBuilder currentv=new StringBuilder();
                for (Vertex n:values) {
                   // System.out.println("key reduce : "+ key+" current "+ n.getId()+" :"+n.getNeighbors());
                  //  listOfNeighbors.addElement(n);
                  //  System.out.println("node : "+n.toString());
                    sb.append(n.toString()+";");
                    if(key.get()==n.getId().get()){
                        currentv.append(n.toString());
                    }
                }
              String parts[]=sb.toString().split(";");
                Vertex currentVertex=new Vertex(currentv.toString());
                int nb=0;
                for (int i=0;i<parts.length;i++){
                    Vertex n=new Vertex(parts[i]);
                    if(currentVertex.getId().get()!=n.getId().get()) {
                        double sim = currentVertex.getsimilarity(n.getNeighbors());
                        if(sim>=0.7){
                            nb++;
                        }
                       // System.out.println("similarity between " + currentVertex.getId() + " and " + n.getId() + " equal " + sim);
                    }

                }

             currentVertex.setNbStrongConnections(new IntWritable(nb));
                if(nb>=3) {
                    currentVertex.setNodeType(new Text("c"));
                }


               context.write(key,currentVertex);

            }catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2.Step2Mapper.class);
        //job.setCombinerClass(Step2.Step2Reducer.class);
        job.setReducerClass(Step2.Step2Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Vertex.class);
        FileInputFormat.addInputPath(job, new Path("adj"));
        FileOutputFormat.setOutputPath(job, new Path("step2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
