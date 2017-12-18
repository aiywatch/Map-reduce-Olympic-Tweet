import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TweetMapper extends Mapper<Object, Text, IntIntPair, IntWritable>{
	IntIntPair content_length = new IntIntPair();
	IntWritable one = new IntWritable(1);
	
	String tweet = "";
	String[] data;
	String tweet_context = "";
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		tweet = value.toString();
		data = tweet.split(";");
		
		if(data.length >= 4 && data[0].matches("\\d*") && data[0].matches("\\d*" )){
			if(data.length > 4){
				String[] context_array = Arrays.copyOfRange(data, 2, data.length-1);
				tweet_context = String.join(";", context_array);
//				System.out.println(tweet_context);
			}else{
				tweet_context = data[2];
			}
			
			int text_length = tweet_context.length();
			IntWritable lower_bound = new IntWritable(text_length/5 * 5 + 1);
			IntWritable upper_bound = new IntWritable((text_length/5 + 1) * 5);
			
//			System.out.println(data[2].length() + ":::::" + lower_bound + ", "+ upper_bound);
			
			content_length.set(lower_bound, upper_bound);
			context.write(content_length, one);
			
		}
		
		
	}

}
