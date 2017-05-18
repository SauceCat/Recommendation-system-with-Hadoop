package UserBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public class ClusterRecommendMapper extends Mapper<Object, Text, Text, VectorWritable>{
	
	Configuration conf = new Configuration();
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException{
		String[] parts = value.toString().split("\t");
		long userID = Long.parseLong(parts[0]);
		int centerID = Integer.parseInt(parts[1].split("@")[1]);
		float sim = Float.parseFloat(parts[2].split("@")[1]);
		int num = Integer.parseInt(parts[3].split("@")[1]);
		
		ArrayList<Vector> simUsers = new ArrayList<Vector>();
		// read the users from the cluster
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Result/Cluster");
			
			// only want to read the Cluster_centerID_* file
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status_list = fs.listStatus(path);
			
			if(status_list != null){
				for(FileStatus status : status_list){
					String filename = status.getPath().getName();
					
					// pattern to match
					String pattern = "Cluster_" + centerID + "-*";
					Pattern regex = Pattern.compile(pattern);
					Matcher matcher = regex.matcher(filename);
					
					if(matcher.find()){
						Path fullpath = new Path(path + "/" + filename);
						SequenceFile.Reader reader = new Reader(fs, fullpath, conf);
						IntWritable cluster = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
					    VectorWritable simuser = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
					    while (reader.next(cluster, simuser)) {
					    	simUsers.add(simuser.get());
					    }
					    IOUtils.closeStream(reader);
					}
				}
			}
	    }catch(IOException e){
			e.printStackTrace();
		}
		
		String outkey = userID + "@" + sim;
		
		if(simUsers.size() <= num){
			for(Vector simuser : simUsers){
				context.write(new Text(outkey), new VectorWritable(simuser));
			}
		}else{
			final Random rand = new Random();
			Collections.shuffle(simUsers, rand);
			for(int i = 0; i < num; i++){
				context.write(new Text(outkey), new VectorWritable(simUsers.get(i)));
			}
		}
	}
}
