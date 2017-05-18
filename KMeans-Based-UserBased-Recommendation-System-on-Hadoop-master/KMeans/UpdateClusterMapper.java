package KMeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class UpdateClusterMapper 
extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>{
	int jobID;
	ArrayList<Vector> centers = new ArrayList<Vector>();
	 
	protected void setup(Context context) throws IOException,
     	InterruptedException {
		Configuration conf = context.getConfiguration();
		jobID = Integer.parseInt(conf.get("jobID"));
		
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Jobs/Jobs_" + (jobID-1) + "/Center/output-r-00000");
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader reader = new Reader(fs, path, conf);
		    IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		    VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		    while (reader.next(key, value)) {
		    	centers.add(value.get());
		    }
		    IOUtils.closeStream(reader);
	    }catch(IOException e){
			e.printStackTrace();
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Jobs/Jobs_" + (jobID-1) + "/Center/output-r-00000"), true);
	 }
	
	public void map(IntWritable cluster, VectorWritable userVector, Context context)
			throws IOException, InterruptedException{
		Vector nearest = null;
		float nearestDistance = Float.MAX_VALUE;
		
		Vector user = userVector.get();
		int centerindex = 0;
		
		for(int i = 0; i < centers.size(); i++){
			Vector center = centers.get(i);
			float distance = (float) user.getDistanceSquared(center);
			if(nearest == null || distance < nearestDistance){
				nearest = center;
				centerindex = i + 1;
				nearestDistance = distance;
			}
		}
		
		context.write(new IntWritable(centerindex), new VectorWritable(user));
	}
}
