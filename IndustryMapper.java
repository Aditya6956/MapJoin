package org.industry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndustryMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	Map<String, String> abbMap = new HashMap<String, String>();
	
	protected void setup(Context context) throws IOException, InterruptedException{
		
		// 21,Get Shorty (1995),Comedy|Crime|Thriller
		
		URI[] files = context.getCacheFiles();
		for (URI p: files){
			if(p.getPath().equals("/user/cloudera/movies.csv")){
				BufferedReader br = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(p.getPath()))));
				
				while(br.ready()){
					String line = br.readLine();
					String[] words = line.split(",");
					String id = words[0];
					String name = words[1];
					abbMap.put(id, name);
				}
				br.close();
			}
		}
		
		if(abbMap.isEmpty()){
			throw new IOException("Unable to load data...");
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		// 149271,5349,3.0,1454692277
		String line = value.toString();
		String[] words = line.split(",");
		String id = words[1];
		String rating = words[2];
		String name = abbMap.get(id);
		
		context.getCounter("MOVIES", "No.Of Movies are").increment(1);
		context.write(new Text(id + name), new Text(rating));
	}
}
