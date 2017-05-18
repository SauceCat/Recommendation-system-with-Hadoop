package UserBased;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SortRecommendMapper extends Mapper<Object,Text,VLongWritable,Text>{
	public void map(Object key,Text value, Context context)
			throws IOException, InterruptedException{
		String[] parts = value.toString().split("\t");
		long userID = Long.parseLong(parts[0].split("@")[0]);
		long itemID = Long.parseLong(parts[0].split("@")[1]);
		float sim = Float.parseFloat(parts[1]);
		
		String out = "";
		out = out + itemID + "@" + sim;
		
		context.write(new VLongWritable(userID), new Text(out));
	}
}
