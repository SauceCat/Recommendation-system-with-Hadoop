package UserBased;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateRecommendReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException{
		float sumsim = 0;
		for(FloatWritable value : values){
			sumsim += value.get();
		}
		context.write(key, new FloatWritable(sumsim));
	}
}
