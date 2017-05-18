package ItemBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 index2 index1@count/index1user index2user -> index1 index2@(count/index1user*index2user)
 */
public class IndexTwoReducer extends Reducer<VLongWritable, Text, VLongWritable, Text>{
	public void reduce(VLongWritable index2, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException{
		List<Float> coo_index1 = new ArrayList<Float>();
		List<Integer> index1s = new ArrayList<Integer>();
		
		float index2user = 0;
		for(Text val : values){
			if(val.toString().contains("@")){
				coo_index1.add(Float.parseFloat(val.toString().split("@")[1]));
				index1s.add(Integer.parseInt(val.toString().split("@")[0]));
			}else{
				index2user = Float.parseFloat(val.toString());
			}
		}
		for(int i = 0; i < coo_index1.size(); i++){
			float result = 0;
			if(index2user != 0){
				result = coo_index1.get(i) / index2user;
				String out = index2.toString() + "@" + Float.toString(result);
				context.write(new VLongWritable(index1s.get(i)), new Text(out));
			}
		}
	}
}