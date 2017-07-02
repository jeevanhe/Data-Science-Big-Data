package BookWordCount.BookWordCount;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountInMapper {

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private Map <String,Integer> tokenmap;
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException
	{
		tokenmap = new HashMap<String,Integer>();
	}
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreElements()){
            String token = tokenizer.nextToken();
            Integer count = tokenmap.get(token);
            if(count == null) count = new Integer(0);
            count+=1;
            tokenmap.put(token,count);
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable writableCount = new IntWritable();
        Text text = new Text();
        Set<String> keys = tokenmap.keySet();
        for (String s : keys) {
            text.set(s);
            writableCount.set(tokenmap.get(s));
            context.write(text,writableCount);
        }
    }
  }
  
  

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountInMapper.class);
    job.setMapperClass(TokenizerMapper.class);
    //Using InMapper
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}