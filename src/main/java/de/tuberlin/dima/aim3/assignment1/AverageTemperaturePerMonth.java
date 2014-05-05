package de.tuberlin.dima.aim3.assignment1;


import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AverageTemperaturePerMonth extends HadoopJob {

  private static double mMinimumQuality;

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    mMinimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job avgtemp = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringTempMonthYearMapper.class,
        Text.class, DoubleWritable.class, TempMonthYearReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);
    avgtemp.waitForCompletion(true);

    return 0;
  }

  static class FilteringTempMonthYearMapper extends Mapper<Object,Text,Text,DoubleWritable> {

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(line.toString());

      while(tokenizer.hasMoreTokens()) {
        String year = tokenizer.nextToken();
        String month = tokenizer.nextToken();
        String temp = tokenizer.nextToken();
        String quality = tokenizer.nextToken();

        if ((new Double(quality)).compareTo(mMinimumQuality) < 0)
          continue;

        ctx.write(new Text(year + "\t" + month), new DoubleWritable(new Double(temp)));
      }
    }
  }

  static class TempMonthYearReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx)
        throws IOException, InterruptedException {
      double result = 0;
      int num = 0;
      Iterator<DoubleWritable> itr = values.iterator();

      while(itr.hasNext()) {
        result += itr.next().get();
        ++num;
      }

      ctx.write(key, new DoubleWritable(result / num));
    }
  }

}