package hashtag;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OlympicHashtag {

	public static void runJob(String[] input, String output) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		
		job.setJarByClass(OlympicHashtag.class);
		job.setMapperClass(HashtagMapper.class);
		job.setCombinerClass(HashtagReducer.class);
		job.setReducerClass(HashtagReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//		job.setNumReduceTasks(1);
		
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);

	}

}
