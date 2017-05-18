package UserBased;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TestUserItemPrefMapper extends Mapper<Object, Text, VLongWritable, VLongWritable>{
	public void map(Object key, Text value, Context context) 
	throws IOException, InterruptedException{
		VLongWritable userID = new VLongWritable();
		VLongWritable itemID = new VLongWritable();
		String[] parts = value.toString().split("\t");
		userID.set(Long.parseLong(parts[0]));
		itemID.set(Long.parseLong(parts[1]));
		context.write(userID, itemID);
    }
}
