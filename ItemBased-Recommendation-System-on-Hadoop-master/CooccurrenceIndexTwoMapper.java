package ItemBased;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/*
 song2 song1@count/index1user -> song2 song1@count/index1user
 */
public class CooccurrenceIndexTwoMapper 
extends Mapper<Object,Text,VLongWritable,Text> {
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] parts = value.toString().split("\t");
		context.write(new VLongWritable((long) Float.parseFloat(parts[0])), new Text(parts[1]));
	}
}