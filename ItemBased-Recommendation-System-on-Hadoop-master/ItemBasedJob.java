package ItemBased;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VectorWritable;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class ItemBasedJob {
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: recommendation <train> <test>");
	      System.exit(2);
	    }
	    
	    
	    ////////////////////////////////////////////
	    // Stage 1: Generating training user vectors
	    ////////////////////////////////////////////
	    Job UserItemPref = new Job(conf, "ItemBased");
	    UserItemPref.setJarByClass(ItemBasedJob.class);
	    
	    // Map Settings
	    UserItemPref.setMapperClass(UserItemPrefMapper.class);
	    UserItemPref.setMapOutputKeyClass(VLongWritable.class);
	    UserItemPref.setMapOutputValueClass(VLongWritable.class);
	    
	    // Reduce Settings
	    UserItemPref.setReducerClass(UserItemPrefReducer.class);
	    UserItemPref.setOutputKeyClass(VLongWritable.class);
	    UserItemPref.setOutputValueClass(VectorWritable.class);

	    // Set input output formats
	    UserItemPref.setInputFormatClass(TextInputFormat.class);
	    UserItemPref.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
		TextInputFormat.addInputPath(UserItemPref, new Path(otherArgs[0]));
	    SequenceFileOutputFormat.setOutputPath(UserItemPref, new Path("ItemBased/UserItemPref"));
	    UserItemPref.waitForCompletion(true);
	    
	    
	    ///////////////////////////////////
	    // Stage 2: Generating song vectors
	    ///////////////////////////////////
	    Job ItemUser = new Job(conf, "ItemBased");
	    ItemUser.setJarByClass(ItemBasedJob.class);
	    
	    // Map Settings
	    ItemUser.setMapperClass(ItemUserMapper.class);
	    ItemUser.setMapOutputKeyClass(VLongWritable.class);
	    ItemUser.setMapOutputValueClass(VLongWritable.class);
	    
	    // Reduce Settings
	    ItemUser.setReducerClass(ItemUserReducer.class);
	    ItemUser.setOutputKeyClass(VLongWritable.class);
	    ItemUser.setOutputValueClass(Text.class);

	    // Set input output formats
	    ItemUser.setInputFormatClass(TextInputFormat.class);
	    ItemUser.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Set path
		TextInputFormat.addInputPath(ItemUser, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(ItemUser, new Path("ItemBased/ItemUser"));
	    ItemUser.waitForCompletion(true);
	    
	    
		//////////////////////////////////////////
		// Stage 3: Calculating song co-occurrence
		//////////////////////////////////////////
		Job UserVectorToCooccurrence = new Job(conf, "ItemBased");
		UserVectorToCooccurrence.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		UserVectorToCooccurrence.setMapperClass(UserVectorToCooccurrenceMapper.class);
		UserVectorToCooccurrence.setMapOutputKeyClass(VLongWritable.class);
		UserVectorToCooccurrence.setMapOutputValueClass(VLongWritable.class);
		
		// Reduce Settings
		UserVectorToCooccurrence.setReducerClass(UserVectorToCooccurrenceReducer.class);
		UserVectorToCooccurrence.setOutputKeyClass(VLongWritable.class);
		UserVectorToCooccurrence.setOutputValueClass(Text.class);
		
		// Set input output formats
		UserVectorToCooccurrence.setInputFormatClass(SequenceFileInputFormat.class);
		UserVectorToCooccurrence.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		SequenceFileInputFormat.addInputPath(UserVectorToCooccurrence, new Path("ItemBased/UserItemPref"));
		TextOutputFormat.setOutputPath(UserVectorToCooccurrence, new Path("ItemBased/UserVectorToCooccurrence"));
		UserVectorToCooccurrence.waitForCompletion(true);
		
	    
		//////////////////////////////////////
		// Stage 4: co-occurrence / index1user
		//////////////////////////////////////
		Job CooccurrenceIndexOne = new Job(conf, "ItemBased");
		CooccurrenceIndexOne.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		MultipleInputs.addInputPath(CooccurrenceIndexOne, new Path("ItemBased/UserVectorToCooccurrence"), 
				TextInputFormat.class, CooccurrenceIndexOneMapper.class);
		MultipleInputs.addInputPath(CooccurrenceIndexOne, new Path("ItemBased/ItemUser"), 
				TextInputFormat.class, IndexOneMapper.class);
		
		CooccurrenceIndexOne.setMapOutputKeyClass(VLongWritable.class);
		CooccurrenceIndexOne.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		CooccurrenceIndexOne.setReducerClass(IndexOneReducer.class);
		CooccurrenceIndexOne.setOutputKeyClass(VLongWritable.class);
		CooccurrenceIndexOne.setOutputValueClass(Text.class);
		
		// Set input output formats
		CooccurrenceIndexOne.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		TextOutputFormat.setOutputPath(CooccurrenceIndexOne, new Path("ItemBased/CooccurrenceIndexOne"));
		CooccurrenceIndexOne.waitForCompletion(true);
		
		
		///////////////////////////////////////////////////
		// Stage 5: co-occurrence / index1user * index2user
		///////////////////////////////////////////////////
		Job CooccurrenceIndexTwo = new Job(conf, "ItemBased");
		CooccurrenceIndexTwo.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		MultipleInputs.addInputPath(CooccurrenceIndexTwo, new Path("ItemBased/CooccurrenceIndexOne"), 
		TextInputFormat.class, CooccurrenceIndexTwoMapper.class);
		MultipleInputs.addInputPath(CooccurrenceIndexTwo, new Path("ItemBased/ItemUser"), 
		TextInputFormat.class, IndexTwoMapper.class);
		CooccurrenceIndexTwo.setMapOutputKeyClass(VLongWritable.class);
		CooccurrenceIndexTwo.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		CooccurrenceIndexTwo.setReducerClass(IndexTwoReducer.class);
		CooccurrenceIndexTwo.setOutputKeyClass(VLongWritable.class);
		CooccurrenceIndexTwo.setOutputValueClass(Text.class);
		
		// Set input output formats
		CooccurrenceIndexTwo.setOutputFormatClass(TextOutputFormat.class);
		
		// Set path
		TextOutputFormat.setOutputPath(CooccurrenceIndexTwo, new Path("ItemBased/CooccurrenceIndexTwo"));
		CooccurrenceIndexTwo.waitForCompletion(true);
		

		///////////////////////////////////
		// Stage 6: Form similarity vectors
		///////////////////////////////////
		Job Similarity = new Job(conf, "ItemBased");
		Similarity.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		Similarity.setMapperClass(SimilarityMapper.class);
		Similarity.setMapOutputKeyClass(VLongWritable.class);
		Similarity.setMapOutputValueClass(Text.class);
		
		// Reduce Settings
		Similarity.setReducerClass(SimilarityReducer.class);
		Similarity.setOutputKeyClass(VLongWritable.class);
		Similarity.setOutputValueClass(VectorWritable.class);
		
		// Set input output formats
		Similarity.setInputFormatClass(TextInputFormat.class);
		Similarity.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Set path
		TextInputFormat.addInputPath(Similarity, new Path("ItemBased/CooccurrenceIndexTwo"));
		SequenceFileOutputFormat.setOutputPath(Similarity, new Path("ItemBased/Similarity"));
		Similarity.waitForCompletion(true);
		
		
		///////////////////////////////////////////
		// Stage 7: Generating testing user vectors
		///////////////////////////////////////////
		Job TestUserPref = new Job(conf, "ItemBased");
	    TestUserPref.setJarByClass(ItemBasedJob.class);
	     
	    // Map Settings
	    TestUserPref.setMapperClass(TestUserPrefMapper.class);
	    TestUserPref.setMapOutputKeyClass(VLongWritable.class);
	    TestUserPref.setMapOutputValueClass(VLongWritable.class);
	    
	    // Reduce Settings
	    TestUserPref.setReducerClass(TestUserPrefReducer.class);
	    TestUserPref.setOutputKeyClass(VLongWritable.class);
	    TestUserPref.setOutputValueClass(VectorWritable.class);

	    // Set input output formats
	    TestUserPref.setInputFormatClass(TextInputFormat.class);
	    TestUserPref.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    // Set path
		TextInputFormat.addInputPath(TestUserPref, new Path(otherArgs[1]));
	    SequenceFileOutputFormat.setOutputPath(TestUserPref, new Path("ItemBased/TestUserPref"));
	    TestUserPref.waitForCompletion(true);
		
		
		////////////////////////////////////////
		// Stage 8: Form ToVectorAndPref vectors
		////////////////////////////////////////
		Job ToVectorAndPref = new Job(conf, "ItemBased");
		ToVectorAndPref.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		MultipleInputs.addInputPath(ToVectorAndPref, new Path("ItemBased/Similarity"), 
				SequenceFileInputFormat.class, CooccurrenceColumnWrapperMapper.class);
		MultipleInputs.addInputPath(ToVectorAndPref, new Path("ItemBased/TestUserPref"), 
				SequenceFileInputFormat.class, UserVectorSplitterMapper.class);
		// SequenceFileInputFormat.setMaxInputSplitSize(ToVectorAndPref, 20971520);
		ToVectorAndPref.setMapOutputKeyClass(VLongWritable.class);
		ToVectorAndPref.setMapOutputValueClass(VectorOrPrefWritable.class);
		
		// Reduce Settings
		ToVectorAndPref.setReducerClass(ToVectorAndPrefReducer.class);
		ToVectorAndPref.setOutputKeyClass(VLongWritable.class);
		ToVectorAndPref.setOutputValueClass(VectorAndPrefsWritable.class);
		
		// Set input output formats
		ToVectorAndPref.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Set path
		SequenceFileOutputFormat.setOutputPath(ToVectorAndPref, new Path("ItemBased/ToVectorAndPref"));
		ToVectorAndPref.waitForCompletion(true);
		
		
		////////////////////////////////////////////
		// Stage 9: Aggregate vectors from same user
		////////////////////////////////////////////
		Job Aggregate = new Job(conf, "ItemBased");
		Aggregate.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		Aggregate.setMapperClass(PartialMultiplyMapper.class);
		Aggregate.setMapOutputKeyClass(VLongWritable.class);
		Aggregate.setMapOutputValueClass(VectorWritable.class);
		
		// Combiner Settings
		Aggregate.setCombinerClass(AggregateCombiner.class);
		
		// Reduce Settings
		Aggregate.setReducerClass(AggregateAndRecommendReducer.class);
		Aggregate.setOutputKeyClass(VLongWritable.class);
		Aggregate.setOutputValueClass(VectorWritable.class);
		
		// Set input output formats
		Aggregate.setInputFormatClass(SequenceFileInputFormat.class);
		Aggregate.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Set path
		SequenceFileInputFormat.addInputPath(Aggregate, new Path("/ItemBased/ToVectorAndPref"));
		SequenceFileOutputFormat.setOutputPath(Aggregate, new Path("/ItemBased/Aggregate"));
		Aggregate.setNumReduceTasks(10);
		Aggregate.waitForCompletion(true);
		
		
		/////////////////////////////////
		// Stage 10: Make recommendations
		/////////////////////////////////
		Job Recommend = new Job(conf, "ItemBased");
		Recommend.setJarByClass(ItemBasedJob.class);
		
		// Map Settings
		MultipleInputs.addInputPath(Recommend, new Path("/ItemBased/TestUserPref"), 
				SequenceFileInputFormat.class, FilterMapper.class);
		MultipleInputs.addInputPath(Recommend, new Path("/ItemBased/Aggregate"), 
				SequenceFileInputFormat.class, RecommendMapper.class);
		Recommend.setMapOutputKeyClass(VLongWritable.class);
		Recommend.setMapOutputValueClass(VectorWritable.class);
		
		// Reduce Settings
		Recommend.setReducerClass(RecommendReducer.class);
		Recommend.setOutputKeyClass(VLongWritable.class);
		Recommend.setOutputValueClass(Text.class);
		
		// Set input output formats
		Recommend.setOutputFormatClass(TextOutputFormat.class);
		Recommend.setNumReduceTasks(10);
		
		// Set path
		TextOutputFormat.setOutputPath(Recommend, new Path("/ItemBased/Recommend"));
		
	    System.exit(Recommend.waitForCompletion(true) ? 0 : 1);
	}
}
