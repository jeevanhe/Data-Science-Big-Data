package BookWordCount.BookWordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class CoOccurenceStripes extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(CoOccurenceStripes.class);
	private static int firstArg=8;
	public static class CoOccurenceStripesMapper extends MapReduceBase implements	Mapper<LongWritable, Text, Text, MapWritable> 
	{
		private boolean caseSensitive = false;
		private long numRecords = 0;
		private String input;
		private Set<String> patternsToSkip = new HashSet<String>();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		  	  
		protected void setup(Context context) throws IOException, InterruptedException {
		  
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
		
	    public void map(LongWritable key, Text value,OutputCollector<Text, MapWritable> output, Reporter reporter)
		  throws IOException {
			
			MapWritable countMap = new MapWritable();
			int neighbors = 2; //No of Neighbors
			
			String line = value.toString();
			if (!caseSensitive) 
				line = line.toLowerCase();	     
			String[] words = WORD_BOUNDARY.split(line);
			    	  
			for (int i = 0; i < words.length; i++) 
			{	
				if (words[i].isEmpty() || patternsToSkip.contains(words[i]) )
		        	continue; //Skip all Stopwords
				if(words[i].length() != firstArg)
	    		   continue;
				int start = (i - neighbors < 0) ? 0 : i - neighbors;
		        int end = (i + neighbors >= words.length) ? words.length - 1 : i + neighbors;  
		        for (int j = start; j <= end; j++)
		        {
		        	if (j == i || words[j].length() != firstArg)
		        		continue;
					
					IntWritable countWritable = (IntWritable)countMap.get( new Text( words[j] ) );
					int count = (countWritable == null) ? 1 : countWritable.get() + 1;
					countMap.put(new Text(words[j]), new IntWritable(count));
					
				}
				output.collect(new Text(words[i]), countMap);
				countMap.clear();
			}
		}
	}

	public static class CoOccurenceStripesReducer extends MapReduceBase implements	Reducer<Text, MapWritable, TextPair, IntWritable> 
	{

		public void reduce(Text key, Iterator<MapWritable> values,OutputCollector<TextPair, IntWritable> output, Reporter reporter)
		throws IOException {
			
			MapWritable totalMap = new MapWritable();
			while (values.hasNext()) {
				MapWritable countMap = values.next();
				for(Writable word : countMap.keySet()) {
					IntWritable totalWritable = (IntWritable)totalMap.get( word );
					int total = (totalWritable == null) ? 0 : totalWritable.get();
					IntWritable countWritable = (IntWritable)countMap.get(word);
					int count = countWritable.get();
					totalMap.put(word, new IntWritable(total + count));
				}
			}
			
			for(Writable word : totalMap.keySet()) {
				output.collect(new TextPair(key, (Text)word), (IntWritable)totalMap.get(word));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		 
		int exitCode = ToolRunner.run(new CoOccurenceStripes(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		
		
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("CoOccurenceStripes");
		conf.setMapperClass(CoOccurenceStripesMapper.class);
		conf.setReducerClass(CoOccurenceStripesReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(MapWritable.class);
		conf.setOutputKeyClass(TextPair.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		Job job = Job.getInstance(getConf(), "CoOccurenceStripes");
		firstArg = Integer.parseInt(args[5]);
		for (int i = 0; i < args.length; i += 1) 
		  {
			  if ("-skip".equals(args[i])) {
				  
				  job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				  i += 1;
				  job.addCacheFile(new Path(args[i]).toUri());
				  // this demonstrates logging
				  LOG.info("Added file to the distributed cache: " + args[i]);
			  }
			  //if ("-len".equals(args[i])) {
			//	  firstArg = Integer.parseInt(args[i+1]);
			  //}
		  }
		
		JobClient.runJob(conf);
		return 0;
	}

}
