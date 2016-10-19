
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;

public class Part2 {
	public static HashMap<String, String> myMap;
	private static Text text_rating;
	public static class Map1 extends Mapper<Object, Text, Text, Text> {
		String mymovieid;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			super.setup(context);

			Configuration conf = context.getConfiguration();
			myMap = new HashMap<String, String>();
			mymovieid = conf.get("movieid"); // for retrieving data you set in
												// driver code
			@SuppressWarnings("deprecation")
			Path[] localPaths = context.getLocalCacheFiles();
			for (Path myfile : localPaths) {
				String line = null;
				String nameofFile = myfile.getName();
				File file = new File(nameofFile + "");
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				line = br.readLine();
				while (line != null) {
					String[] arr = line.split("::");
					myMap.put(arr[0].trim(), arr[0] + arr[1] + arr[2]); // userid
																			// and
																			// gender
																			// and
																			// age
					line = br.readLine();
				}

				// closing connection
				br.close();
			}
		}

		// Reading rating.dat file in this method
				public void map(Object key, Text value, Context context)
						throws IOException, InterruptedException {
					String str = value.toString();
					// UserID::moviesID::Ratings::Timestamp
					String[] array_rating = str.split("::");
					Text user_id = new Text(array_rating[0]);
					@SuppressWarnings("unused")
					Text movie_id = new Text(array_rating[1]);
					int int_rating = Integer.parseInt(array_rating[2]);
					String rating = array_rating[2];
					
					if (int_rating >= 4 && mymovieid.equals(array_rating[1])) {
						text_rating = new Text("rat~" + rating);
						user_id.set(myMap.get(array_rating[0]));
						context.write(user_id, text_rating);
					}

				}
	}
				
				public static void main(String[] args) throws Exception {
					Configuration conf = new Configuration();
					String[] otherArgs = new GenericOptionsParser(conf, args)
							.getRemainingArgs();
					// get all args
					if (otherArgs.length != 3) {
						System.err
								.println("Usage: JoinExample <in> <in2> <out> <anymovieid>");
						System.exit(2);
					}

					conf.set("movieid", otherArgs[2]); // setting global data variable for
														// hadoop

					// create a job with name "joinexc"
					@SuppressWarnings("deprecation")
					Job job = new Job(conf, "mapsideexec");
					job.setJarByClass(Part2.class);
					job.setMapperClass(Map1.class);

					final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
					job.addCacheFile(new URI(NAME_NODE + "/user/hue/users.dat"));
					

					job.setOutputKeyClass(Text.class);
					// set output value type
					job.setOutputValueClass(Text.class);

					// set the HDFS path of the input data
					// set the HDFS path for the output
					FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
					FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
					 
					job.waitForCompletion(true);

				}
	
}