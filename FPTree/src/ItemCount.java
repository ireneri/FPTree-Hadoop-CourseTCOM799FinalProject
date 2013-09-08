

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
/*
 * Author: Liangchen Li
 * Email: liliangc@seas.upenn.edu
 */
public class ItemCount extends Configured implements Tool {
  
	public static class ItemCountMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter report)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
			
		}		
	}
    /*
     * Reducer part: sum up the flow for each hours
     */
	public static class ItemCountReducer extends MapReduceBase
	implements Reducer<Text, IntWritable, Text, IntWritable> {
	
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			output.collect(key, new IntWritable(sum));
		}
	}
	

    /**
     * Jab configurations
     */
	public int run(String[] args) throws Exception {

		
		JobConf conf = new JobConf(ItemCount.class);
		conf.setJobName("ItemCount");
		conf.setBoolean("mapred.output.compress", false); 
		
		conf.setMapperClass(ItemCountMapper.class);
		conf.setCombinerClass(ItemCountReducer.class);
		conf.setReducerClass(ItemCountReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
    	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// for eclipse test: /home/leon/workspace/FPTree/output1
		// for cluster output1
		FileOutputFormat.setOutputPath(conf, new Path("output1"));
		
		JobClient.runJob(conf);
        return 0;

	}
	
	public static void main(String[] args) throws Exception {
	// ItemCount(first MapReduce)
	//int res = ToolRunner.run(new Configuration(), new ItemCount(), args);
	}
}