package BookWordCount.BookWordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import java.util.Iterator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PairWordCount extends Configured implements Tool {
	
	  private static final Logger LOG = Logger.getLogger(PairWordCount.class);
	  
	  public static void main(String[] args) throws Exception {
	    
		  int res = ToolRunner.run(new PairWordCount(), args);
		  System.exit(res);
	  }
	  
	
	  public int run (String[] args) throws Exception
	  {
		  
		  Job job = Job.getInstance(getConf(), "wordcount");
		  
		  for (int i = 0; i < args.length; i += 1) 
		  {
			  if ("-skip".equals(args[i])) {
				  job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				  i += 1;
				  job.addCacheFile(new Path(args[i]).toUri());
				  // this demonstrates logging
				  LOG.info("Added file to the distributed cache: " + args[i]);
			  }
		  }
		
		  job.setJarByClass(PairWordCount.class);
		  job.setMapperClass(StripesOccurrenceMapper.class);
		  job.setReducerClass(StripesReducer.class);
		  job.setMapOutputKeyClass(TextPair.class);
		  job.setMapOutputValueClass(IntWritable.class);
			
	    
		  job.setOutputKeyClass(TextPair.class);
		  job.setOutputValueClass(IntWritable.class);
	   
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
		  return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	public static class StripesOccurrenceMapper extends Mapper<LongWritable,Text,TextPair, IntWritable> {
		  private MapWritable occurrenceMap = new MapWritable();
		  private Text word = new Text();
		  private boolean caseSensitive = false;
		  private long numRecords = 0;
		  private String input;
		  private Set<String> patternsToSkip = new HashSet<String>();
		  private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		  
		  protected void setup(Mapper.Context context) throws IOException, InterruptedException {
		  
		      if (context.getInputSplit() instanceof FileSplit)
		      {
		        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
		      } 
		      else
		      {
		        this.input = context.getInputSplit().toString();
		      }
		      Configuration config = context.getConfiguration();
		      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
		      if (config.getBoolean("wordcount.skip.patterns", false))
		      {
		        URI[] localPaths = context.getCacheFiles();
		        parseSkipFile(localPaths[0]);
		      }
		    }
		  
	    private void parseSkipFile(URI patternsURI)
	    {
	        LOG.info("Added file to the distributed cache: " + patternsURI);
	        try {
	          BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
	          String pattern;
	          while ((pattern = fis.readLine()) != null) {
	            patternsToSkip.add(pattern);
	          }
	        } catch (IOException ioe) {
	          System.err.println("Caught exception while parsing the cached file '"
	              + patternsURI + "' : " + StringUtils.stringifyException(ioe));
	        }
	      }	  

		  //@Override
		 protected void map(LongWritable key, Text value, OutputCollector<TextPair, IntWritable> output, Reporter reporter) throws IOException, InterruptedException 
		  {
		   //int neighbors = context.getConfiguration().getInt("neighbors", 2);
			 int neighbors = 2;
		   String line = value.toString();
		   if (!caseSensitive) {
		        line = line.toLowerCase();
		    }   
		   
	       String[] tokens = WORD_BOUNDARY.split(line);
		   if (tokens.length > 1) {
			  
		      for (int i = 0; i < tokens.length; i++) { 
		    	  if (tokens[i].isEmpty() || patternsToSkip.contains(tokens[i]) )
			        	continue; //Skip all Stopwords
		    	  if(tokens[i].length() != 5)
		    		  continue;
		    	  
		          word.set(tokens[i]);
		          occurrenceMap.clear();

		          int start = (i - neighbors < 0) ? 0 : i - neighbors;
		          int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
		           for (int j = start; j <= end; j++) {
		                if (j == i || tokens[i].length() != 5) continue;
		                Text neighbor = new Text(tokens[j]);
		                if(occurrenceMap.containsKey(neighbor))
		                {
		                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
		                   count.set(count.get()+1);
		                }
		                else
		                {
		                   occurrenceMap.put(neighbor,new IntWritable(1));
		                }
		                
		           }
		           for(Writable wor : occurrenceMap.keySet()) {
						output.collect(new TextPair(word, (Text)wor), (IntWritable)occurrenceMap.get(wor));
					}
		           //output.collect(new Text(tokens[i]), occurrenceMap);
	               
		          //context.write(word,occurrenceMap);
		       }
		   }
		  }
	}
	
	//Reducer
	public static class StripesReducer extends Reducer<Text, MapWritable, TextPair, IntWritable> {
	    private MapWritable incrementingMap = new MapWritable();

	    //@Override
	    protected void reduce(Text key, Iterable<MapWritable> values, OutputCollector<TextPair, IntWritable> output,
	    		Reporter reporter) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (MapWritable value : values) {
	            addAll(value);
	        }
	        for(Writable word : incrementingMap.keySet()) {
				output.collect(new TextPair(key, (Text)word), (IntWritable)incrementingMap.get(word));
			}
	        //context.write(key, incrementingMap);
	    }

	    private void addAll(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key,fromCount);
	            }
	        }
	    }
	}

}
