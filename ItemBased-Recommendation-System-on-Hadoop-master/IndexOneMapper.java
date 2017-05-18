package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/*
 song1 sqrt(sum(user)) -> song1 sqrt(sum(user))
 */
public class IndexOneMapper 
extends Mapper<Object,Text,VLongWritable,Text> {
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] parts = value.toString().split("\t");
		context.write(new VLongWritable((long) Float.parseFloat(parts[0])), new Text(parts[1]));
	}
}