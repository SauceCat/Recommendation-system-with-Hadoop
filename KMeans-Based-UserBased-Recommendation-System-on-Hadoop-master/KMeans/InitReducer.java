package KMeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

//Reduce Output: k randomly chosen users
public class InitReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>{
	int k;
	ArrayList<Vector> centers = new ArrayList<Vector>();
	private MultipleOutputs<IntWritable, VectorWritable> out;
	private final static Random rand = new Random();
	 
	protected void setup(Context context) throws IOException,
  	InterruptedException {
		Configuration conf = context.getConfiguration();
		k = Integer.parseInt(conf.get("k"));
		// and then you can use it
		out = new MultipleOutputs<IntWritable, VectorWritable>(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	  out.close();
	}
	 
	public void reduce(IntWritable one, Iterable<VectorWritable> vectors, Context context)
            throws IOException, InterruptedException{
		 ArrayList<Vector> allVectors = new ArrayList<Vector>();
		 
		 int count = 0;
		 for(VectorWritable vector : vectors){
			 allVectors.add(vector.get());
			 out.write(new IntWritable(count), vector, "Cluster/output");
			 count++;
		 }
		 
		 // Randomly choose k users to be the initial centers
		 // Do shuffle
		 Collections.shuffle(allVectors, rand);
		 int j = 0;
		 for(int i = 0; i < allVectors.size(); i++){
			 if(centers.contains(allVectors.get(i))){
				 continue;
			 }else{
				 centers.add(allVectors.get(i));
				 j++;
				 if(j >= k){
					 break;
				 }
			 }
		 }
		 
		 int index = 1;
		 for(Vector center : centers){
			 out.write(new IntWritable(index), new VectorWritable(center), "Center/output");
			 index++;
		 }
	}
 }
