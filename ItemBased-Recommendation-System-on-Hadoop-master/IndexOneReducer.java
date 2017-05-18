package ItemBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 index1 index2@count index1user -> index2 index1@(count/index1user)
 */
public class IndexOneReducer extends Reducer<VLongWritable, Text, VLongWritable, Text>{
	public void reduce(VLongWritable index1, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		List<Float> coo = new ArrayList<Float>();
		List<Integer> index2s = new ArrayList<Integer>();
		
		float index1user = 0;
		for(Text val : values){
			if(val.toString().contains("@")){
				coo.add(Float.parseFloat(val.toString().split("@")[1]));
				index2s.add(Integer.parseInt(val.toString().split("@")[0]));
			}else{
				index1user = Float.parseFloat(val.toString());
			}
		}
		for(int i = 0; i < coo.size(); i++){
			float result = 0;
			if(index1user != 0){
				result = coo.get(i) / index1user;
				String out = index1.toString() + "@" + Float.toString(result);
				context.write(new VLongWritable(index2s.get(i)), new Text(out));
			}
		}
	}
}