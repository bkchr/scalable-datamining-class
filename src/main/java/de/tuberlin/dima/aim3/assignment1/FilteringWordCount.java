package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class FilteringWordCount extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringWordCountMapper.class,
        Text.class, IntWritable.class, WordCountReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
    wordCount.waitForCompletion(true);

    return 0;
  }

  static class FilteringWordCountMapper extends Mapper<Object,Text,Text,IntWritable> {
	  
	  static final String StopWords = "to,and,in,the";
	  
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
    	StringTokenizer tokenizer = new StringTokenizer(line.toString(), " \t\n\r\f,", false);
    	
    	while(tokenizer.hasMoreTokens())
    	{
    		String token = tokenizer.nextToken();
    		token = token.toLowerCase();
    		    		
    		if(StopWords.contains(token))
    			continue;
    		
    		ctx.write(new Text(token), new IntWritable(1));
    	}
    }
  }

  static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
        throws IOException, InterruptedException {
    	int result = 0;    	
    	Iterator<IntWritable> itr = values.iterator();
    	
    	while(itr.hasNext())
    		result += itr.next().get();
    	
    	ctx.write(key, new IntWritable(result));
    }
  }

}