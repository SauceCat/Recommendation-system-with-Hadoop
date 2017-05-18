package UserBased;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class UserCenterSimMapper extends Mapper<VLongWritable, VectorWritable, VLongWritable, Text>{

	ArrayList<Vector> centers = new ArrayList<Vector>();
	ArrayList<Integer> cluster_nums = new ArrayList<Integer>();
	
	@Override
	protected void setup(Context context) {
		Configuration conf = new Configuration();
		
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Result/Center");
			
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
						IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
					    VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
					    while (reader.next(key, value)) {
					    	centers.add(value.get());
					    }
					    IOUtils.closeStream(reader);
					}
				}
			}
	    }catch(IOException e){
			e.printStackTrace();
		}
		
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/UserBased/KMeans/Summary");
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
						BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fullpath)));
						String num = null;
						while((num = br.readLine()) != null){
							cluster_nums.add(Integer.parseInt(num.split("\t")[1]));
						}
					}
				}
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public void map(VLongWritable userID, VectorWritable userPref, Context context) 
			throws IOException, InterruptedException{
		Vector userVector = userPref.get();
		
		float sim = 0;
		int count = 0;
		for(Vector center : centers){
			float up = (float) userVector.times(center).zSum();
			float down1 = (float) Math.sqrt(userVector.getLengthSquared());
			float down2 = (float) Math.sqrt(center.getLengthSquared());
			
			sim = up / (down1 * down2);
			int num = cluster_nums.get(count);
			int index = count + 1;
			count++;
			
			String out = "";
			out = out + "centerID@" + index + "\t" + "sim@" + sim + "\t" + "num@" + num;
			context.write(userID, new Text(out));
		}
	}
}
