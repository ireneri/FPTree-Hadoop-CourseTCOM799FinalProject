import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class ItemPatternFilter implements Tool {

	public static class ItemPatternFilterMapper extends MapReduceBase implements
	Mapper<Text, Text, IntWritable, Text> {

		public void map(Text key, Text value,
		OutputCollector<IntWritable, Text> output, Reporter report)
		throws IOException {
			String strPattern = key.toString();// format:[xx, yy, zz,..]
			String[] strItems = strPattern.substring(1, strPattern.length() - 1).split(", ");
			int[] intItems = new int[strItems.length];
			for (int i = 0; i < strItems.length; i++){
				intItems[i] = Integer.parseInt(strItems[i]);
				IntWritable item = new IntWritable(intItems[i]);
				Text textPatternSupport = new Text(strPattern + "\t" + value.toString());
				
				output.collect(item, textPatternSupport);
			}
		}
	
	}

	public static class ItemPatternFilterReducer extends MapReduceBase
		implements Reducer<IntWritable, Text, IntWritable, Text> {
		
		private int K;// max pattern we saved for each item
		
		public void configure(JobConf job) {
				
			K = Integer.parseInt(job.get("K"));
			
		}
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, 
				Text> output, Reporter reporter) throws IOException {
			PriorityQueue HP = new PriorityQueue(key.get(), K);// for each K, we need a queue
			while(values.hasNext()){
				String[] strPatternSupport = values.next().toString().split("\t");
				String strPattern = strPatternSupport[0];
				String strSupport = strPatternSupport[1];
				String[] strItems = strPattern.substring(1, strPattern.length() - 1).split(", ");
				ArrayList<Integer> pattern = new ArrayList<Integer>();
				for (int i = 0; i < strItems.length; i++){
					pattern.add(Integer.parseInt(strItems[i]));
				}
				FrequentPattern fp = new FrequentPattern(pattern, Integer.parseInt(strSupport));
				HP.add(fp);
				
			}
			//System.out.println("HP and patterns: item " + HP.getName());
			//System.out.println("HP length: " + HP.getLength());
			
			int HPLength = HP.getLength();
			IntWritable itemName = new IntWritable(HP.getName());
			String strPatternSupport = "\n"; 
			for (int i = 0; i < HPLength; i++){
				FrequentPattern fp = HP.getMinPattern();	
				String strPattern = fp.getPattern().toString();
				int intSupport = fp.getSupport();
				strPatternSupport = strPatternSupport + "Pattern: " + strPattern
						+ "; Support: " + intSupport + "\n";			
			}
			Text textPatternSupport = new Text(strPatternSupport);
			output.collect(itemName, textPatternSupport);
		}
	}
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(new Configuration(), ItemPatternFilter.class);

		conf.setJobName("ItemPatternFilter");
		conf.set("K", args[4]);
		
		conf.setBoolean("mapred.output.compress", false); 
		conf.setMapperClass(ItemPatternFilterMapper.class);
		conf.setReducerClass(ItemPatternFilterReducer.class);
		
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
    	
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf, new Path("output2"));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.set("key.value.separator.in.input.line", "\t");
		
		JobClient.runJob(conf);
        return 0;
		
	}

	public static void main(String[] args) throws Exception {
		// ItemCount(first MapReduce)
			ToolRunner.run(new Configuration(), new ItemMining(), args);
	}

	@Override
	public Configuration getConf() {
		
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		
	}
}
