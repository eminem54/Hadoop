import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;

public class WordCount{
private static final Log log = LogFactory.getLog{ExportToMongoDBFromHDFS.class);

public static class Read extends Mapper<LongWritable, Text, ObjectId, BSONObject>{
public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
System.out.println("key: " + key);
System.out.println("Value: " + value);

String[] fields = value.toString().split("\t");

String md5 = fields[0];
String url = fields[1];
String date = fields[2];
String time = fields[3];
String ip = fields[4];


BSONObject b = new BasicBSONObject ();

b.put("md5", md5);
b.put("url", url);
b.put("date", date);
b.put("time", time);
b.put("ip", ip);

context.write(new ObjectId(), b);
}
}

public static void main(String[] args) throws Exception {
final Configuration conf = new Configuration();
MongoConfigUtil.setOutputURI(conf, "mongodb://<HOST>:<PORT>/test.weblogs");

System.out.println("Configuration: " + conf);

final Job job = new Job(conf, "Export to Mongo");

Path in = new Path("/data/weblogs/weblog_entries.txt");
FileInputFormat.setInputPaths(job, in);

job.setJarByClass(WordCount.class);
job.setMapperClass(Read.class);

job.setOutputKeyClass(ObjectId.class);
job.setOutputValueClass(BSONObject.class);

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(MongoOutputFormat.class);

job.setNumReduceTasks(0);

System.exit(job.waitForCompletion(true) ? 0:1);
}
}