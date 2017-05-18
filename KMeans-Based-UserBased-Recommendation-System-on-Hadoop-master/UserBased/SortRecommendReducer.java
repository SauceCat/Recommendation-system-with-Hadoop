package UserBased;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortRecommendReducer extends Reducer<VLongWritable, Text, VLongWritable, Text>{
	ArrayList<String> popular = new ArrayList<String>();
	int n; 
	protected void setup(Context context) throws IOException,InterruptedException {
		Configuration conf = context.getConfiguration();
		n = Integer.parseInt(conf.get("n"));
		try{
			Path path = new Path("hdfs://localhost:9000/user/mac/input/popular_songs.txt");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String word = null;
			while((word = br.readLine()) != null){
				String[] parts = word.split("\t");
				popular.add(parts[0]);
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void reduce(VLongWritable userID, Iterable<Text> itemSimsOrFilter, Context context) 
			throws IOException, InterruptedException{
		class Item{
			long itemID;
			float itemSim;
		}
		
		ArrayList<Item> items = new ArrayList<Item>();
		ArrayList<Long> filter = new ArrayList<Long>();
		
		String flag = "filter";
		
		for(Text value : itemSimsOrFilter){
			// if value contains "filter", it is the Filter
			String temp = value.toString();
			if(temp.contains(flag)){
				String[] parts = temp.split("\t");
				for(int i = 1; i < parts.length; i++){
					filter.add(Long.parseLong(parts[i]));
				}
			}else{
				String[] parts = value.toString().split("@");
				long itemID = Long.parseLong(parts[0]);
				float sim = Float.parseFloat(parts[1]);
				Item item = new Item();
				item.itemID = itemID;
				item.itemSim = sim;
				items.add(item);
			}
		}
		
		ArrayList<Item> filterItems = new ArrayList<Item>();
		for(int i = 0; i < items.size(); i++){
			if(!filter.contains(items.get(i).itemID)){
				filterItems.add(items.get(i));
			}
		}
		
		class CustomComparator implements Comparator<Item> {
		    @Override
		    public int compare(Item o1, Item o2) {
		        return Float.compare(o2.itemSim, o1.itemSim);
		    }
		}
		
		Collections.sort(filterItems, new CustomComparator());
		
		ArrayList<String> recommends = new ArrayList<String>();
		ArrayList<Long> already = new ArrayList<Long>();
		
		if(filterItems.size() >= n){
			for(int i = 0; i < n; i++){
				String recommend = filterItems.get(i).itemID + "@" + filterItems.get(i).itemSim;
				recommends.add(recommend);
			}
		}else{
			for(int i = 0; i < filterItems.size(); i++){
				String recommend = filterItems.get(i).itemID + "@" + filterItems.get(i).itemSim;
				recommends.add(recommend);
				already.add(filterItems.get(i).itemID);
			}
			
			int addnum = n - filterItems.size();
			int count = 0;
			
			for(int i = 0; i < popular.size(); i++){
				String pop = popular.get(i);
				if(!filter.contains(pop)){
					if(!already.contains(pop)){
						String recommend = pop + "@" + "popular";
						recommends.add(recommend);
						already.add(Long.parseLong(pop));
					}
				}
				count++;
				if(count >= addnum){
					break;
				}
			}
		}
		
		String recommendItems = "";
		for(int i = 0; i < recommends.size(); i++){
			recommendItems = recommendItems + recommends.get(i) + "\t";
		}
		context.write(userID, new Text(recommendItems));
		
	}
}
