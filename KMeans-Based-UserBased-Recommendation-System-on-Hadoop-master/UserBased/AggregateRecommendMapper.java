package UserBased;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateRecommendMapper extends Mapper<Object, Text, Text, FloatWritable>{
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException{
		String[] parts = value.toString().split("\t");
		String newkey = parts[0];
		float sim = Float.parseFloat(parts[1]);
		context.write(new Text(newkey), new FloatWritable(sim));
	}
}
