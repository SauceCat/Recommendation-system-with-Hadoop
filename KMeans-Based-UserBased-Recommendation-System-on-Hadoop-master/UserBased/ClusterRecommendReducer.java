package UserBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ClusterRecommendReducer extends Reducer<Text, VectorWritable, Text, FloatWritable>{
	
	Configuration conf = new Configuration();
	
	public void reduce(Text key, Iterable<VectorWritable> simusers, Context context) 
			throws IOException, InterruptedException{
		String[] parts = key.toString().split("@");
		long userID = Long.parseLong(parts[0]);
		float clusterSim = Float.parseFloat(parts[1]);
		
		Vector userPref = null;
		
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/Recommendation/TestUserItemPref");
			
			// only want to read the part-r-* file
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status_list = fs.listStatus(path);
			
			if(status_list != null){
				for(FileStatus status : status_list){
					String filename = status.getPath().getName();
					
					// pattern to match
					String pattern = "part-r-*";
					Pattern regex = Pattern.compile(pattern);
					Matcher matcher = regex.matcher(filename);
					
					if(matcher.find()){
						Path fullpath = new Path(path + "/" + filename);
						SequenceFile.Reader reader = new Reader(fs, fullpath, conf);
						VLongWritable user = (VLongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
					    VectorWritable uservector = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
					    while (reader.next(user, uservector)) {
					    	if(user.get() == userID){
					    		userPref = uservector.get();
					    		break;
					    	}
					    }
					    IOUtils.closeStream(reader);
					}
				}
			}
	    }catch(IOException e){
			e.printStackTrace();
	    }
		
		float userSim = 0;
		
		// make the filter set (the already appeared items)
		ArrayList<Long> filter = new ArrayList<Long>();
		Iterator<Vector.Element> userIt = ((RandomAccessSparseVector) userPref).iterateNonZero();
		while(userIt.hasNext()){
			Vector.Element e = userIt.next();
			filter.add((long) e.index());
		}
		
		for(VectorWritable simuser : simusers){
			float up = (float) userPref.times(simuser.get()).zSum();
			float down1 = (float) Math.sqrt(userPref.getLengthSquared());
			float down2 = (float) Math.sqrt(simuser.get().getLengthSquared());
			
			userSim = up / (down1 * down2);
			
			Vector sim = simuser.get().times(userSim).times(clusterSim);
			
			Iterator<Vector.Element> simuserIt = ((RandomAccessSparseVector) sim).iterateNonZero();
			
			while(simuserIt.hasNext()){
				Vector.Element e = simuserIt.next();
				long itemIndex = e.index();
				if(!filter.contains(itemIndex)){
					String outkey = "";
					outkey = outkey + userID + "@" + itemIndex;
					float outsim = (float) e.get();
					context.write(new Text(outkey), new FloatWritable(outsim));
				}
			}	
		}
	}
}
