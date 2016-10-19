
import java.io.IOException; 
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UserRating {

public static class UserIDMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
			if(line[1].equals("F"))
			{
			context.write(new Text(line[0]), new Text("UID"+line[0]));
			}
		}
	}

	public static class RatingMapper extends	Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("::");

			context.write(new Text(line[0]), new Text("R"+line[1]+"\t"+line[2]));
		}
	}

	public static class ReduceSideJoin1 extends 
	Reducer<Text, Text, Text, Text>{
		
		// ArrayList to store movie ratings
		private static ArrayList<Text> listA = new ArrayList<Text>();
		// ArrayList to store movie names
		private static ArrayList<Text> listB = new ArrayList<Text>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			listA.clear();
			listB.clear();

			for(Text temp : values){	
				// Store user IDs
				if(temp.toString().startsWith("UID")){
					listA.add(new Text(temp.toString().substring(3)));
				}
				// Store ratings 
				else if(temp.toString().startsWith("R")){
					listB.add(new Text(temp.toString().substring(1)));
				}
			}

			// Write userID and corresponding movie ID and rating
			if(!listA.isEmpty() && !listB.isEmpty()){
				for(Text user : listA){
					for(Text movie : listB){
						context.write(new Text(user), new Text(movie));
					}
				}
			}
		}
	}

public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
private final static IntWritable one = new IntWritable(1); 
private static FloatWritable rating = new FloatWritable(0);
private Text word = new Text(); // type of output key
public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
//StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
String s = value.toString();
String itr[] = s.split("\t");
word.set(itr[1]); // set word as each input keyword
rating = new FloatWritable(Float.parseFloat(itr[2]));
context.write(word, rating); // create a pair <keyword, 1> 
}
}


public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
private FloatWritable result = new FloatWritable();
public void reduce(Text key, Iterable<FloatWritable> values, Context context ) throws IOException, InterruptedException {
float sum = 0.0f; 
float count = 0.0f;// initialize the sum for each keyword 
for (FloatWritable val : values) { 
sum += val.get();
count = count + 1;} 
result.set(sum/count);
context.write(key, result); // create a pair <keyword, number of occurences> 
}
}

public static class TopfiveMapper extends Mapper<Object, Text, NullWritable, Text> {
// Stores a map of user reputation to the record
private TreeMap<Float, Text> repToRecordMap = new TreeMap<Float, Text>();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	//String S = value.toString();
    //String[] splitedLine = S.split("\t");
    
    // Add this record to our map with the reputation as the key
    //repToRecordMap.put(Float.parseFloat(splitedLine[1]),new Text(value));

	//	if (repToRecordMap.size() > 10) {
	//		repToRecordMap.remove(repToRecordMap.firstKey());
	
	context.write(NullWritable.get(), new Text(value));
	//	}
	}
//protected void cleanup(Context context) throws IOException,InterruptedException {
// Output our ten records to the reducers with a null key
//	for (Text t : repToRecordMap.values()) {
//		context.write(NullWritable.get(), t);
//	}
//}

}

public static class TopfiveReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
// Stores a map of user reputation to the record
// Overloads the comparator to order the reputations in descending order
private TreeMap<Float, LinkedList<Text>> repToRecordMap = new TreeMap<Float, LinkedList<Text>>();
public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
{
	for (Text value : values) {
		String S = value.toString();
		String[] splitedLine = S.split("\t");
		float rating = Float.parseFloat(splitedLine[1]);
		
		// If key already present, append movie to the list
		if(repToRecordMap.containsKey(rating)){
			repToRecordMap.get(rating).add(new Text(value));
		}
		// If key not present, add new entry
		else{
			LinkedList<Text> movie = new LinkedList<Text>();
			movie.add(new Text(value));
			repToRecordMap.put(rating, movie);
		}
    
	//	repToRecordMap.put(Float.parseFloat(splitedLine[1]), new Text(value));
    
	//	if (repToRecordMap.size() > 10) {
	//		repToRecordMap.remove(repToRecordMap.firstKey());
    //	}
    
	}
	
	//for (Text t : repToRecordMap.descendingMap().values()) {
	// Output our ten records to the file system with a null key
	//	context.write(NullWritable.get(), t);
	//}
	
	int i = 0;
	// Write top ten movies to context
	for(LinkedList<Text> movieList : repToRecordMap.descendingMap().values()){
		for(Text movie2 : movieList){
			context.write(NullWritable.get(), movie2);
			i++;
			if(i==10)
				return;
		}
	}
	

}
}

public static class RatingMovieIDMapper extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		context.write(new Text(line[0]), new Text("ID"+line[1]));
	}
}

public static class MovieNameMapper extends	Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("::");

		context.write(new Text(line[0]), new Text("NAME"+line[1]));
	}
}

public static class ReduceSideJoin extends 
Reducer<Text, Text, Text, Text>{
	
	// ArrayList to store movie ratings
	private static ArrayList<Text> listA = new ArrayList<Text>();
	// ArrayList to store movie names
	private static ArrayList<Text> listB = new ArrayList<Text>();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
		listA.clear();
		listB.clear();

		for(Text temp : values){	
			// Store movie ratings
			if(temp.toString().startsWith("ID")){
				listA.add(new Text(temp.toString().substring(2)));
			}
			// Store movie names
			else if(temp.toString().startsWith("NAME")){
				listB.add(new Text(temp.toString().substring(4)));
			}
		}

		// Write movie rating and corresponding movie name
		if(!listA.isEmpty() && !listB.isEmpty()){
			for(Text rating : listA){
				for(Text name : listB){
					context.write(new Text(rating), new Text(name));
				}
			}
		}
	}
}


// Driver program
 public static void main(String[] args) throws Exception { 
 Configuration conf = new Configuration(); 
 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args 
 if (otherArgs.length != 7) { 
 System.err.println("Usage: UserRating <in> <out>"); 
 System.exit(2); 
 }
 System.out.println(args[0] + " " + args[1] + " " + args[2] + " " + args[3] + args[4] + " " + args[5]);
// create a job with name "wordcount" 
 Configuration conf3 = new Configuration(); 
 Job job3 = new Job(conf3, "UserRating"); 
 
 MultipleInputs.addInputPath(job3, new Path(otherArgs[0]), TextInputFormat.class, UserIDMapper.class);
 MultipleInputs.addInputPath(job3, new Path(otherArgs[1]), TextInputFormat.class, RatingMapper.class);
 
 job3.setOutputKeyClass(Text.class); // set output key type
 job3.setOutputValueClass(Text.class); // set output value type  
 job3.setJarByClass(UserRating.class);
 
 //job2.setMapperClass(TopfiveMapper.class);
 job3.setReducerClass(ReduceSideJoin1.class);
 //job2.setCombinerClass(ReduceSideJoin.class);
 //job1.setNumReduceTasks(1);

 //FileInputFormat.addInputPath(job2, new Path(otherArgs[1])); //set the HDFS path of the input data
 FileOutputFormat.setOutputPath(job3, new Path(otherArgs[2])); // set the HDFS path for the output
Job job = new Job(conf, "UserRating"); 
if(job3.waitForCompletion(true))
{
job.setOutputKeyClass(Text.class); // set output key type 
job.setOutputValueClass(FloatWritable.class); // set output value type
job.setJarByClass(UserRating.class);
job.setMapperClass(Map.class); 
job.setReducerClass(Reduce.class);
job.setCombinerClass(Reduce.class);

// uncomment the following line to add the Combiner 
//job.setCombinerClass(Reduce.class); 

FileInputFormat.addInputPath(job, new Path(otherArgs[2]));//set the HDFS path of the input data  
FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); // set the HDFS path for the output
}
//Wait till job completion 
//System.exit(job.waitForCompletion(true) ? 0 : 1);

Configuration conf1 = new Configuration(); 
Job job1 = new Job(conf1, "UserRating");

	if(job.waitForCompletion(true))
	{ 
	 
	 job1.setOutputKeyClass(NullWritable.class); // set output key type
	 job1.setOutputValueClass(Text.class); // set output value type  
	 job1.setJarByClass(UserRating.class);
	 
	 job1.setMapperClass(TopfiveMapper.class);
	 job1.setReducerClass(TopfiveReducer.class);
	 job1.setCombinerClass(TopfiveReducer.class);
	 //job1.setNumReduceTasks(1);

	 // uncomment the following line to add the Combiner 
	 //job.setCombinerClass(Reduce.class); 
	 //job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	 
	 FileInputFormat.addInputPath(job1, new Path(otherArgs[3])); //set the HDFS path of the input data
	 FileOutputFormat.setOutputPath(job1, new Path(otherArgs[4])); // set the HDFS path for the output
	 
	 //Wait till job completion 
	 //System.exit(job1.waitForCompletion(true) ? 0 : 1);
	 //job1.waitForCompletion(true);
	 
	}
	
	if(job1.waitForCompletion(true))
	{
	 Configuration conf2 = new Configuration(); 
	 Job job2 = new Job(conf2, "UserRating"); 
	 
	 MultipleInputs.addInputPath(job2, new Path(otherArgs[4]), TextInputFormat.class, RatingMovieIDMapper.class);
	 MultipleInputs.addInputPath(job2, new Path(otherArgs[5]), TextInputFormat.class, MovieNameMapper.class);
	 
	 job2.setOutputKeyClass(Text.class); // set output key type
	 job2.setOutputValueClass(Text.class); // set output value type  
	 job2.setJarByClass(UserRating.class);
	 
	 //job2.setMapperClass(TopfiveMapper.class);
	 job2.setReducerClass(ReduceSideJoin.class);
	 //job2.setCombinerClass(ReduceSideJoin.class);
	 //job1.setNumReduceTasks(1);

	 //FileInputFormat.addInputPath(job2, new Path(otherArgs[1])); //set the HDFS path of the input data
	 FileOutputFormat.setOutputPath(job2, new Path(otherArgs[6])); // set the HDFS path for the output
	 
	 //Wait till job completion 
	 System.exit(job2.waitForCompletion(true) ? 0 : 1);
	 //job1.waitForCompletion(true);
	 
	}
	
	
	
 }

}