package com.DSCAN.Hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DTriblet implements Writable {

   public Vertex src,dist;
   public DoubleWritable edge;

   public DTriblet(){
       this.src=new Vertex();
       this.dist=new Vertex();
       this.edge=new DoubleWritable(0);
   }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
     src.write(dataOutput);
     dist.write(dataOutput);
     edge.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
     src.readFields(dataInput);
     dist.readFields(dataInput);
     edge.readFields(dataInput);
    }

    public Vertex getSrc() {
        return src;
    }

    public void setSrc(Vertex src) {
        this.src = src;
    }

    public Vertex getDist() {
        return dist;
    }

    public void setDist(Vertex dist) {
        this.dist = dist;
    }

    public DoubleWritable getEdge() {
        return edge;
    }

    public void setEdge(DoubleWritable edge) {
        this.edge = edge;
    }

    @Override
    public String toString() {
        return "DTriblet{" +
                "src=" + src.toString() +
                ", dist=" + dist.toString() +
                ", edge=" + edge +
                '}';
    }
}
