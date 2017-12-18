package top.sport;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SportMapper extends Mapper<Object, Text, Text, IntWritable>{
	IntWritable hour = new IntWritable();
	IntWritable one = new IntWritable(1);
	Text athlete_name = new Text();
	
	private Hashtable<String, String> medalist;
	
	public boolean isNumeric(String s) {  
	    return s != null && s.matches("\\d*");  
	}  
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String tweet = value.toString();
		
		
		for(String name : medalist.keySet()) {
			if(tweet.contains(name)) {
				athlete_name.set(medalist.get(name));
				context.write(athlete_name, one);
				
			}
		}
				
		
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		medalist = new Hashtable<String, String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				String[] fields = line.split(",");
				// Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry
				if (fields.length == 11)
					medalist.put(fields[1], fields[7]);
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}

