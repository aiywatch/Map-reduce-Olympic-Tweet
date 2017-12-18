package hashtag;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends Mapper<Object, Text, Text, IntWritable>{
	IntWritable hour = new IntWritable();
	IntWritable one = new IntWritable(1);
	
	enum CustomCounters {INVALID_ROW}
	
//	public boolean isNumeric(String s) {  
//	    return s != null && StringUtils.isNotEmpty(s) && s.matches("\\d*");  
//	}  
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String tweet = value.toString();
		String[] data = tweet.split(";");
		Text hashtag = new Text();
		
		if(NumberUtils.isNumber(data[0])) { // isNumeric(data[0])){
			long epoch_time = Long.parseLong(data[0]);
			Instant instant = Instant.ofEpochMilli(epoch_time);
			LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneId.of("Brazil/DeNoronha"));
			
			if(date.getHour() == 23){
				
				// Get only tweet content
				String[] content_array = Arrays.copyOfRange(data, 2, data.length-1);
				String tweet_content = String.join(";", content_array);
				
				Pattern MY_PATTERN = Pattern.compile("#(\\w+)");
				Matcher mat = MY_PATTERN.matcher(tweet_content);
//				List<String> strs=new ArrayList<String>();
				while (mat.find()) {
//				  strs.add(mat.group(1));
				  hashtag.set(mat.group(1).toLowerCase());
				  context.write(hashtag, one);
				}
//				System.out.println(data[2]);
//				System.out.println(strs);
				
			}
		}else {
			context.getCounter(CustomCounters.INVALID_ROW).increment(1);
		}
		
		
	}

}


