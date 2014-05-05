package de.tuberlin.dima.aim3.assignment1;

import com.thoughtworks.xstream.alias.ClassMapper;
import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.Tokenizer;
import sun.security.pkcs11.wrapper.Functions;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

  static boolean mIsAuthorFile;
  static Path mSmallerFile;

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Path inputpath = null;

    if(true)
    {
      inputpath = books;
      mSmallerFile = authors;
      mIsAuthorFile = true;
    }
    else {
      inputpath = authors;
      mSmallerFile = books;
      mIsAuthorFile = false;
    }

    Job authjoin = prepareJob(inputpath, outputPath, TextInputFormat.class, BroadCastJoinMapper.class,
        Text.class, IntWritable.class, Reducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
    authjoin.setNumReduceTasks(0);
    authjoin.waitForCompletion(true);

    return 0;
  }

  static class BroadCastJoinMapper extends Mapper<Object,Text,Text,IntWritable> {

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(line.toString());
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(mSmallerFile)));

      List<String> lines = new ArrayList<String>();

      String rline = reader.readLine();
      while(rline != null)
      {
        lines.add(rline);
        rline = reader.readLine();
      }

      if(mIsAuthorFile)
        mapAuthor(tokenizer, lines, ctx);
      else
        mapBook(tokenizer, lines, ctx);
    }

    protected void mapAuthor(StringTokenizer tokenizer, List<String> lines, Context ctx) throws IOException, InterruptedException {
      while(tokenizer.hasMoreTokens())
      {
        String bkey = tokenizer.nextToken();
        String year = tokenizer.nextToken();
        String title = tokenizer.nextToken("\n\r");
        title = title.replaceAll("\t", "");

        for(String line : lines)
        {
          StringTokenizer ltokens = new StringTokenizer(line);

          String akey = ltokens.nextToken();
          String prename = ltokens.nextToken();
          String name = ltokens.nextToken();

          if(akey.equals(bkey))
            ctx.write(new Text(prename + " " + name + "\t" + title), new IntWritable(new Integer(year)));
        }
      }
    }

    protected void mapBook(StringTokenizer tokenizer, List<String> lines, Context ctx) throws IOException, InterruptedException {
      while(tokenizer.hasMoreTokens())
      {
        String akey = tokenizer.nextToken();
        String prename = tokenizer.nextToken();
        String name = tokenizer.nextToken();

        for(String line : lines)
        {
          StringTokenizer ltokens = new StringTokenizer(line);

          String bkey = ltokens.nextToken();
          String year = ltokens.nextToken();
          String title = ltokens.nextToken("\n\r");
          title = title.replaceAll("\t", "");

          if(akey.equals(bkey))
            ctx.write(new Text(prename + " " + name + "\t" + title), new IntWritable(new Integer(year)));
        }
      }
    }
  }

}