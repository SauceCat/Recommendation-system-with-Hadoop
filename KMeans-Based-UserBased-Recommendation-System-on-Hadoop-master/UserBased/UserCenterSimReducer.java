package UserBased;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCenterSimReducer extends Reducer<VLongWritable, Text, VLongWritable, Text>{
	
	int k; 
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		k = Integer.parseInt(conf.get("k"));
	}
	public void reduce(VLongWritable userID, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		class UserCenter{
			int centerID;
			float sim;
			int num;
		}
		ArrayList<UserCenter> usercenters = new ArrayList<UserCenter>();
		
		for(Text value : values){
			UserCenter usercenter = new UserCenter();
			String [] parts = value.toString().split("\t");
			int centerID = Integer.parseInt(parts[0].split("@")[1]);
			float sim = Float.parseFloat(parts[1].split("@")[1]);
			int num = Integer.parseInt(parts[2].split("@")[1]);
			
			usercenter.centerID = centerID;
			usercenter.sim = sim;
			usercenter.num = num;
			
			usercenters.add(usercenter);
		}
		
		class CustomComparator implements Comparator<UserCenter> {
		    @Override
		    public int compare(UserCenter o1, UserCenter o2) {
		        return Float.compare(o2.sim, o1.sim);
		    }
		}
		
		Collections.sort(usercenters, new CustomComparator());
		
		int sum = 0;
		for(UserCenter usercenter : usercenters){
			int num = 0;
			int sum_old = sum;
			sum += usercenter.num;
			int sum_new = sum;
			String out = "";
			if(sum_new >= k){
				num = k - sum_old;
				out = out + "centerID@" + usercenter.centerID + "\t" + "sim@" + usercenter.sim + "\t" + "num@" + num;
				context.write(userID, new Text(out));
				break;
			}else{
				num = usercenter.num;
				out = out + "centerID@" + usercenter.centerID + "\t" + "sim@" + usercenter.sim + "\t" + "num@" + num;
				context.write(userID, new Text(out));
			}
		}
	}
}
