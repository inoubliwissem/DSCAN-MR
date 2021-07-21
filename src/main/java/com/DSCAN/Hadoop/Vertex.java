package com.DSCAN.Hadoop;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Vertex  implements Writable {
    private IntWritable id, nbStrongConnections,cluster;
    private Set<IntWritable> neighbors;
    private Text nodeType;


    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public IntWritable getNbStrongConnections() {
        return nbStrongConnections;
    }

    public void setNbStrongConnections(IntWritable nbStrongConnections) {
        this.nbStrongConnections = nbStrongConnections;
    }

    public Set<IntWritable> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(Set<IntWritable> neighbors) {
        this.neighbors = neighbors;
    }

    public Text getNodeType() {
        return nodeType;
    }

    public void setNodeType(Text nodeType) {
        this.nodeType = nodeType;
    }

    public IntWritable getCluster() {
        return cluster;
    }

    public void setCluster(IntWritable cluster) {
        this.cluster = cluster;
    }

    public Vertex() {
        neighbors = new HashSet<>();
        this.id=new IntWritable(0);
        this.nbStrongConnections=new IntWritable(0);
        this.nodeType=new Text("no");
        this.cluster=new IntWritable(-1);
    }
   public Vertex(String line){
        try {
            String vertexElements[] = line.split(":");
            String features[] = vertexElements[0].split(",");
            String neig[] = vertexElements[1].split(",");
            neighbors = new HashSet<>();
            this.id = new IntWritable(Integer.parseInt(features[0].trim()));
            this.nbStrongConnections = new IntWritable(Integer.parseInt(features[1].trim()));
            this.cluster= new IntWritable(Integer.parseInt(features[3].trim()));
            this.nodeType = new Text(features[2].trim());

          //  System.out.println("Id" + id + " nb strongConnection" + nbStrongConnections + " node type " + nodeType);
            for (String n : neig) {
               this.neighbors.add(new IntWritable(Integer.parseInt(n.trim())));
            }
        }catch(Exception e){
            System.out.println("Vertex class "+e.getMessage());
        }
   }


    public void addNeighbor(IntWritable i) {
        this.neighbors.add(i);

    }

    public void addNeighbors(Set<IntWritable> neighbors) {
        this.neighbors.addAll(neighbors);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.id.write(dataOutput);
        this.nbStrongConnections.write(dataOutput);
        this.nodeType.write(dataOutput);
        this.cluster.write(dataOutput);
        dataOutput.writeInt(this.neighbors.size());
        for (IntWritable elm :this.neighbors) {
            elm.write(dataOutput);
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
          this.id.readFields(dataInput);
          this.nbStrongConnections.readFields(dataInput);
          this.nodeType.readFields(dataInput);
          this.cluster.readFields(dataInput);
          int size=dataInput.readInt();
          neighbors=new HashSet<IntWritable>(size);
          for (int i=0;i<size;i++){
              IntWritable nei=new IntWritable();
              nei.readFields(dataInput);
              neighbors.add(nei);
          }

    }
    @Override
    public String toString(){
        return this.id+", "+this.nbStrongConnections+", "+this.nodeType+", "+this.cluster+":"+this.neighbors.toString().replace("[","").replace("]","");
    }
    public double getsimilarity(Set<IntWritable> nei){
       int nb=0;
        for (IntWritable i: nei){
            if(this.getNeighbors().contains(i)){
                nb++;
            }
        }
        double sim=nb/Math.sqrt(this.neighbors.size()*nei.size());

       return sim;
    }

}
