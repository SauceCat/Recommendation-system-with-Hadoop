package ItemBased;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class RecommendReducer
extends Reducer<VLongWritable, VectorWritable, VLongWritable, Text>{
    // If the size of recommendlist is less than recommendationsPerUser
    // append the list with popular songs
    // If recommendlist is null
    // recommend popular songs
    ArrayList<String> popular = new ArrayList<String>();

	@Override
	protected void setup(Context context) {
		@SuppressWarnings("unused")
		Configuration conf = context.getConfiguration();
		try{
			Path path = new Path("hdfs://ec2-54-169-47-8.ap-southeast-1.compute.amazonaws.com:9000/input/popular_songs.txt");
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
	
	public void reduce(VLongWritable userID, Iterable<VectorWritable> itemSimsOrFilter, Context context)
			throws IOException, InterruptedException{
		
		// if the vector length is 2
		// we get the recommendlist and the userpref
		// else we just get the userpref
		Vector recommendlist = null;
		Vector userPref = null;
		int popularflag = 0;
		
		ArrayList<Vector> vectors = new ArrayList<Vector>();
		ArrayList<Long> filter = new ArrayList<Long>();
		
		int recommendationsPerUser = 10000;

		for(VectorWritable value : itemSimsOrFilter){
			vectors.add(value.get());
		}
		
		if(vectors.size() > 1){
			Iterator<Vector.Element> temp = ((RandomAccessSparseVector) vectors.get(0)).iterateNonZero();
			Vector.Element element = temp.next();
			int flag = (int) element.get();
			if(flag < 0){
				recommendlist = vectors.get(1);
				userPref = vectors.get(0);
			}else{
				recommendlist = vectors.get(0);
				userPref = vectors.get(1);
			}
		}else{
			userPref = vectors.get(0);
			popularflag = 1;
		}
		

		Iterator<Vector.Element> user = ((RandomAccessSparseVector) userPref).iterateNonZero();
		while(user.hasNext()){
			Vector.Element e = user.next();
			Long itemIndex = (long) e.index();
			filter.add(itemIndex);
		}
		if(popularflag == 0){
			Iterator<Vector.Element> it_recommend = ((RandomAccessSparseVector) recommendlist).iterateNonZero();
			
			Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(
					recommendationsPerUser + 1, Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
					
			while (it_recommend.hasNext()){
				Vector.Element element = it_recommend.next();
				Long index = (long) element.index();
				if(!filter.contains(index)){
					float value = (float) element.get();
					if(topItems.size() < recommendationsPerUser){
						topItems.add(new GenericRecommendedItem(index, value));
					}else if (value > topItems.peek().getValue()){
						topItems.add(new GenericRecommendedItem(index, value));
						topItems.poll();
					}
				}else{
					continue;
				}
			}
	
			List<RecommendedItem> recommendations = new ArrayList<RecommendedItem>(topItems.size());
			recommendations.addAll(topItems);
			Collections.sort(recommendations, ByValueRecommendedItemComparator.getInstance());
			
			if(recommendations.size() < recommendationsPerUser){
				ArrayList<VLongWritable> already = new ArrayList<VLongWritable>();
				for(int i = 0; i < recommendations.size(); i++){
					RecommendedItem element = recommendations.get(i);
					already.add(new VLongWritable(element.getItemID()));
				}
				while(recommendations.size() < recommendationsPerUser){
					int count = recommendations.size();
					for(int j = 0; j < popular.size(); j++){
						int popularindex = Integer.parseInt(popular.get(j));
						if(!filter.contains(new VLongWritable(popularindex))){
							if(!already.contains(new VLongWritable(popularindex))){
								recommendations.add(new GenericRecommendedItem(popularindex, 0));
								count++;
								if(count >= recommendationsPerUser){
									break;
								}
							}
						}
					}
				}
			}
			
			String recommendItems = "";
			for(int i = 0; i < recommendations.size(); i++){
				if(recommendations.get(i).getValue() != 0){
					recommendItems = recommendItems + recommendations.get(i).getItemID() + "@" + recommendations.get(i).getValue() + "\t";
				}else{
					recommendItems = recommendItems + recommendations.get(i).getItemID() + "@popular" + "\t";
				}
			}
			context.write(userID, new Text(recommendItems));
		}else{
			String recommendItems = "";
			int count = 0;
			for(int i = 0; i < popular.size(); i++){
				Long pop = Long.parseLong(popular.get(i));
				if(!filter.contains(pop)){
					recommendItems = recommendItems + pop + "@popular" + "\t";
				}
				count++;
				if(count >= recommendationsPerUser){
					break;
				}
			}
			context.write(userID, new Text(recommendItems));
		}
		
		System.out.println("========= :3 =============");
		
	}
}
