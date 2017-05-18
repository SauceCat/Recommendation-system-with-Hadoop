package UserBased;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VectorWritable;


public class UserBasedJob {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 3) {
	      System.err.println("Usage: UserBased <test> <k> <n>");
	      System.exit(2);
	    }
	    
	    
		/////////////////////////////////////////////////
		// Stage 1: make test user-item preference matrix
		/////////////////////////////////////////////////
	    // userID {song1, song2, song3...}
		
		Job TestUserItemPref = new Job(new Configuration(), "UserBased_TestUserItemPref");
		TestUserItemPref.setJarByClass(UserBasedJob.class);
		
		// Map settings
		TestUserItemPref.setMapperClass(TestUserItemPrefMapper.class);
		TestUserItemPref.setMapOutputKeyClass(VLongWritable.class);
		TestUserItemPref.setMapOutputValueClass(VLongWritable.class);
		
		// Reduce settings
		TestUserItemPref.setReducerClass(TestUserItemPrefReducer.class);
		TestUserItemPref.setOutputKeyClass(VLongWritable.class);
		TestUserItemPref.setOutputValueClass(VectorWritable.class);
		
		// Set the input output format
		TestUserItemPref.setInputFormatClass(TextInputFormat.class);
		TestUserItemPref.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Set path
		TextInputFormat.addInputPath(TestUserItemPref, new Path(otherArgs[0]));
		SequenceFileOutputFormat.setOutputPath(TestUserItemPref, new Path("UserBased/Recommendation/TestUserItemPref"));
		TestUserItemPref.waitForCompletion(true);
		
		
		////////////////////////////////////////////////////
		// Stage 2: similarity between test user and centers
		////////////////////////////////////////////////////
		Job UserCenterSim = new Job(new Configuration(), "UserBased_UserCenterSim");
		UserCenterSim.getConfiguration().setInt("k", Integer.parseInt(otherArgs[1]));
		UserCenterSim.setJarByClass(UserBasedJob.class);
		
		// Map settings
		UserCenterSim.setMapperClass(UserCenterSimMapper.class);
		UserCenterSim.setMapOutputKeyClass(VLongWritable.class);
		UserCenterSim.setMapOutputValueClass(Text.class);
		
		// Reduce settings
		UserCenterSim.setReducerClass(UserCenterSimReducer.class);
		UserCenterSim.setOutputKeyClass(VLongWritable.class);
		UserCenterSim.setOutputValueClass(Text.class);
		
		// Set the input output format
		UserCenterSim.setInputFormatClass(SequenceFileInputFormat.class);
		UserCenterSim.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		SequenceFileInputFormat.addInputPath(UserCenterSim, new Path("UserBased/Recommendation/TestUserItemPref"));
		TextOutputFormat.setOutputPath(UserCenterSim, new Path("UserBased/Recommendation/UserCenterSim"));
		UserCenterSim.waitForCompletion(true);
		
		
		//////////////////////////////////////////////////
		// Stage 3: find the clusters to do recommendation 
		//////////////////////////////////////////////////
		Job ClusterRecommend = new Job(new Configuration(), "UserBased_ClusterRecommend");
		ClusterRecommend.setJarByClass(UserBasedJob.class);
		
		// Map settings
		ClusterRecommend.setMapperClass(ClusterRecommendMapper.class);
		ClusterRecommend.setMapOutputKeyClass(Text.class);
		ClusterRecommend.setMapOutputValueClass(VectorWritable.class);
		
		// Reduce settings
		ClusterRecommend.setReducerClass(ClusterRecommendReducer.class);
		ClusterRecommend.setOutputKeyClass(Text.class);
		ClusterRecommend.setOutputValueClass(FloatWritable.class);
		
		// Set the input output format
		ClusterRecommend.setInputFormatClass(TextInputFormat.class);
		ClusterRecommend.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		TextInputFormat.addInputPath(ClusterRecommend, new Path("UserBased/Recommendation/UserCenterSim"));
		TextOutputFormat.setOutputPath(ClusterRecommend, new Path("UserBased/Recommendation/ClusterRecommend"));
		ClusterRecommend.waitForCompletion(true);

		
		///////////////////////////////////
		// Stage 4: aggregate and recommend 
		///////////////////////////////////
		Job AggregateRecommend = new Job(new Configuration(), "UserBased_AggregateRecommend");
		AggregateRecommend.setJarByClass(UserBasedJob.class);
		
		// Map settings
		AggregateRecommend.setMapperClass(AggregateRecommendMapper.class);
		AggregateRecommend.setMapOutputKeyClass(Text.class);
		AggregateRecommend.setMapOutputValueClass(FloatWritable.class);
		
		// Reduce settings
		AggregateRecommend.setReducerClass(AggregateRecommendReducer.class);
		AggregateRecommend.setOutputKeyClass(Text.class);
		AggregateRecommend.setOutputValueClass(FloatWritable.class);
		
		// Set the input output format
		AggregateRecommend.setInputFormatClass(TextInputFormat.class);
		AggregateRecommend.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		TextInputFormat.addInputPath(AggregateRecommend, new Path("UserBased/Recommendation/ClusterRecommend"));
		TextOutputFormat.setOutputPath(AggregateRecommend, new Path("UserBased/Recommendation/AggregateRecommend"));
		AggregateRecommend.waitForCompletion(true);
		
		
		//////////////////////////////////////
		// Stage 5: sort, filter and recommend 
		//////////////////////////////////////
		Job SortRecommend = new Job(new Configuration(), "UserBased_SortRecommend");
		SortRecommend.getConfiguration().setInt("n", Integer.parseInt(otherArgs[2]));
		SortRecommend.setJarByClass(UserBasedJob.class);
		
		// Map settings
		MultipleInputs.addInputPath(SortRecommend, new Path("UserBased/Recommendation/TestUserItemPref"), 
				SequenceFileInputFormat.class, FilterMapper.class);
		MultipleInputs.addInputPath(SortRecommend, new Path("UserBased/Recommendation/AggregateRecommend"), 
				TextInputFormat.class, SortRecommendMapper.class);
		SortRecommend.setMapOutputKeyClass(VLongWritable.class);
		SortRecommend.setMapOutputValueClass(Text.class);
		
		// Reduce settings
		SortRecommend.setReducerClass(SortRecommendReducer.class);
		SortRecommend.setOutputKeyClass(VLongWritable.class);
		SortRecommend.setOutputValueClass(Text.class);
		
		// Set the input output format
		SortRecommend.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		TextOutputFormat.setOutputPath(SortRecommend, new Path("UserBased/Recommendation/SortRecommend"));
		// SortRecommend.waitForCompletion(true);
		
		System.exit(SortRecommend.waitForCompletion(true) ? 0 : 1);
		
	}
}
