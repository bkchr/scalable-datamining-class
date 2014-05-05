package de.tuberlin.dima.aim3.assignment1;

import com.google.common.collect.Lists;
import com.sun.corba.se.spi.ior.Writeable;
import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.omg.CORBA_2_3.portable.OutputStream;

import java.io.*;
import java.util.*;

public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job authjoin = prepareJob(authors, books, outputPath, RJMapper.class,
        Text.class, JoinData.class, RJReducer.class, Text.class, Text.class, TextOutputFormat.class);
    authjoin.waitForCompletion(true);

    return 0;
  }

  protected Job prepareJob(Path inputPath, Path inputPath2, Path outputPath,
                           Class<? extends Mapper> mapper, Class<? extends Writable> mapperKey, Class<? extends Writable> mapperValue,
                           Class<? extends Reducer> reducer, Class<? extends Writable> reducerKey, Class<? extends Writable> reducerValue,
                           Class<? extends OutputFormat> outputFormat) throws IOException {

    Job job = new Job(new Configuration(getConf()));
    Configuration jobConf = job.getConfiguration();

    if (reducer.equals(Reducer.class)) {
      if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
      }
      job.setJarByClass(mapper);
    } else {
      job.setJarByClass(reducer);
    }

    TextInputFormat.addInputPaths(job, inputPath.toString() + "," + inputPath2.toString());

    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapperKey);
    job.setMapOutputValueClass(mapperValue);

    jobConf.setBoolean("mapred.compress.map.output", true);

    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);

    job.setJobName(getCustomJobName(job, mapper, reducer));

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  static class JoinData implements Writable
  {
    private int mType;
    private String mData;

    public String getData() { return mData; }
    public int getType() { return mType; }

    JoinData(int type, String data) { mType = type; mData = data; }
    JoinData() { mType = -1; mData = ""; }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(mType);
      out.writeUTF(mData);
    }

    public void readFields(DataInput in) throws IOException {
      mType = in.readInt();
      mData = in.readUTF();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof JoinData) {
        JoinData other = (JoinData) obj;
        return mType == other.mType && mData.equals(other.mData);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (mData + mType).hashCode();
    }

  }

  static class RJMapper extends Mapper<Object,Text,Text,JoinData> {

    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(line.toString(), "\t", false);

      List<String> tokens = new ArrayList<String>();

      while(tokenizer.hasMoreElements())
        tokens.add(tokenizer.nextToken());

      if(tokens.size() == 2)
        ctx.write(new Text(tokens.get(0)), new JoinData(0, tokens.get(1)));
      else
        ctx.write(new Text(tokens.get(0)), new JoinData(1, tokens.get(2) + "\t" + tokens.get(1)));
    }
  }

  static class RJReducer extends Reducer<Text,JoinData,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<JoinData> values, Context ctx)
        throws IOException, InterruptedException {

      List<String> authors = new ArrayList<String>();
      List<String> books = new ArrayList<String>();

      Iterator<JoinData> itr = values.iterator();

      while(itr.hasNext())
      {
        JoinData current = itr.next();

        if(current.getType() == 0)
          authors.add(current.getData());
        else
          books.add(current.getData());
      }


      for(String author : authors)
      {
        for(String book : books)
        {
          ctx.write(new Text(author), new Text(book));
        }

      }
    }
  }

}