import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StationInverting {
	public static class StationInvertingMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text user = new Text();
		private Text station = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split(" ");

			user.set(line[0]);
			station.set(line[3]);
			System.out.println(line[0] + " " + line[3] + "\n");

			context.write(new Text(line[3]), new Text(line[0]));
			}
		}
	

	public static class StationInvertingReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text userList = new Text();


		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String userString = "";

			Set<String> userSet = new HashSet<>();
			Iterator<String> iter = userSet.iterator();
			

			for(Text value : values) {
				userSet.add(value.toString());
			}

			for(String data : userSet) {
				userString += data + " ";
			}

			userList.set(userString);
			
/*			StringBuffer buffer = new StringBuffer();
			for(Text value : values) {
				if(buffer.length() != 0) {
					buffer.append(" ");
				}
				buffer.append(value.toString());
			}
			Text documentList = new Text();
			documentList.set(buffer.toString());
*/

			context.write(key, userList);
		}
	}

	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "station inverting");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(StationInverting.class);
		job.setMapperClass(StationInvertingMapper.class);
		job.setCombinerClass(StationInvertingReducer.class);
		job.setReducerClass(StationInvertingReducer.class);



		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
