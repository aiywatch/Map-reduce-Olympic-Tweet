package hour;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
	IntWritable hour = new IntWritable();
	IntWritable one = new IntWritable(1);
	
//	public boolean isNumeric(String s) {  
//	    return s != null && s.matches("\\d*");  
//	}  
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String tweet = value.toString();
		String[] data = tweet.split(";");
		
//		if(data.length >= 4 && isNumeric(data[0])){
		if(NumberUtils.isNumber(data[0])) {
			long epoch_time = Long.parseLong(data[0]);
			Instant instant = Instant.ofEpochMilli(epoch_time);
			LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneId.of("Brazil/DeNoronha"));
			
//			System.out.print(date.getHour());
			hour.set(date.getHour());
			context.write(hour, one);
		}
		
		
		
	}

}

