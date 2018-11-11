
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.util.*;
import java.io.*; 

import org.apache.commons.logging.*; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.bson.*; 
 
import com.mongodb.hadoop.*; 
import com.mongodb.hadoop.util.*; 



public class WordCount {
	static String[] topId = new String[5];
	static String[] topLo = new String[4];
	
	public static class WordCountMapper extends 
			Mapper<LongWritable, Text, Text, LongWritable> {
		private static LongWritable one = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

				String line = value.toString();
				String words[] = line.split(" ");

				context.write(new Text(words[0]), one);
			}
	}

	public static class WordCountReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		static Map<String, Integer> m = new HashMap<String, Integer>();
		static ArrayList< Map.Entry<String, Integer> > arr = new ArrayList<>();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			m.put(key.toString(), Integer.parseInt(Long.toString(count)));
		}

		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException  {
    			for( Map.Entry<String, Integer> e:m.entrySet() ) {
    				arr.add(e);
    			}
    			arr.sort((a, b) -> b.getValue() - a.getValue());
    			for(int i=0; i<5; i++) {
    				context.write(new Text(arr.get(i).getKey()), new LongWritable(arr.get(i).getValue()));
    			}
    		}
	}
	public static class LocationCountMapper1 extends Mapper<Text,Text,Text,Text>{
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, new Text(value + "\t" + 1));
	}
}
	
	public static class LocationCountMapper
			extends Mapper<LongWritable,Text,Text,Text>{

		private static LongWritable one = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String words[]=line.split(" ");

			context.write(new Text(words[0]), new Text(words[3] + "\t" + 2));
		}
	}

	public static class LocationCountReducer extends
			Reducer<Text, Text, Text, Text> {
		static Map<String, Integer> m2 = new HashMap<String, Integer>();

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
		Map<String, Integer> mtemp = new HashMap<String, Integer>();
			int count = 0;
			String ID = null;
			String Bus = null;
			
			for (Text val : values) {
				String str = val.toString();
				String[] tokens = str.split("\\t");

				if(tokens[1].equals("1")) {
					ID = tokens[0];
				}
				else {
					Bus = tokens[0];
					mtemp.compute(Bus, (k,v) -> (v==null ? 1:v+1));
				}
			}

			if(ID!=null && Bus!=null){
				for(Map.Entry<String, Integer> e : mtemp.entrySet()) {
					m2.compute(e.getKey(),(k,v) -> (v==null ? e.getValue():v+e.getValue()));
				}
			}
		}

		@Override
    		protected void cleanup(Context context) throws IOException, InterruptedException {
      			ArrayList< Map.Entry<String, Integer> > arr2 = new ArrayList<>();

    			for( Map.Entry<String, Integer> e : m2.entrySet() ) {
    				arr2.add(e);
    			}
			arr2.sort( (a, b) -> b.getValue() - a.getValue() );

			for(int i=0; i<4; i++){
				context.write(new Text(arr2.get(i).getKey()), new Text(arr2.get(i).getValue().toString()));
			}
    		}
	}
	
	
	
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static class StationInvertingMapper1 extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, new Text(value + "\t" + 1));
		}
	}

	public static class StationInvertingMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String words[]=line.split(" ");

			context.write(new Text(words[3]), new Text(words[0] + "\t" + 2));
		}
	}
	
	public static class StationInvertingReducer extends Reducer<Text, Text, Text, Text> {
		Map<String, String> mtemp = new HashMap<>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int count = 0;
			String ID = null;
			String Bus = null;
			Set<Integer> userSet = new HashSet<>();
			for (Text val : values) {
				String str = val.toString();
				String[] tokens = str.split("\\t");

				if(tokens[1].equals("1")) {
					Bus = tokens[0];
				}
				else {
					ID = tokens[0];
					userSet.add(Integer.parseInt(tokens[0]));
				}
			}
			
			if(ID!=null && Bus!=null) {
			ArrayList<Integer> sortlist = new ArrayList<>();
			for(Integer e:userSet){
				sortlist.add(e);
			}

			Collections.sort(sortlist);
				String val = "";
				for(int i =0; i<sortlist.size(); i++){
					val = val + sortlist.get(i) + " ";				
				}
				mtemp.put(key.toString(), val);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
  			ArrayList<String> arr2 = new ArrayList<>();
  			for(Map.Entry<String, String> e:mtemp.entrySet()) {
  				arr2.add(e.getKey());
  			}
  			Collections.sort(arr2);
  			for(int i=0; i<4; i++) {
  				context.write(new Text(arr2.get(i)), new Text(mtemp.get(arr2.get(i))));
  			}
		}
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		String inputPath="test//input";
		String outputPath="test//output";
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		//job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "//First"));

		job.waitForCompletion(true);
/////////////
			Configuration conf2=new Configuration();
			Job job2 = Job.getInstance(conf2, "Location");
			
			job2.setJarByClass(WordCount.class);
			MultipleInputs.addInputPath(job2, new Path(inputPath), TextInputFormat.class, LocationCountMapper.class);
			MultipleInputs.addInputPath(job2, new Path(outputPath+"//First"), KeyValueTextInputFormat.class, LocationCountMapper1.class);

			FileOutputFormat.setOutputPath(job2, new Path(outputPath + "//Second"));

			job2.setReducerClass(LocationCountReducer.class);
			job2.setNumReduceTasks(1);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
/////////////////
			Configuration conf3=new Configuration();
			MongoConfigUtil.setOutputURI( conf3, "mongodb://localhost/test.testFinal1" ); 
			Job job3 = Job.getInstance(conf3, "Indexing");
			job3.setJarByClass(WordCount.class);
			
			MultipleInputs.addInputPath(job3, new Path(inputPath), TextInputFormat.class, StationInvertingMapper.class);
			MultipleInputs.addInputPath(job3, new Path(outputPath+"//Second"), KeyValueTextInputFormat.class, StationInvertingMapper1.class);

			//FileOutputFormat.setOutputPath(job3, new Path(outputPath + "//Final"));

			job3.setReducerClass(StationInvertingReducer.class);
			job3.setNumReduceTasks(1);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setOutputFormatClass( MongoOutputFormat.class ); 
			
			System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}


