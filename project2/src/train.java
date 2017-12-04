

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.StringReader;  

import org.apache.lucene.analysis.Analyzer;  
import org.apache.lucene.analysis.TokenStream;  
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;  
import org.wltea.analyzer.lucene.IKAnalyzer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class train {

  public static class TokenizerMapper extends
      Mapper<Object, Text,   Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    	
    	 String sourceStr = value.toString();
         String[] sourceStrArray = sourceStr.split("  ");         
		IKAnalyzer analyzer = new IKAnalyzer();  
        // 使用智能分词  
        analyzer.setUseSmart(true); 
        if (sourceStrArray.length<3){
        	return;
        }
        TokenStream tokenStream = analyzer.tokenStream("content",  
                new StringReader(sourceStrArray[3]));  
        while (tokenStream.incrementToken()) {  
            CharTermAttribute charTermAttribute = tokenStream  
                    .getAttribute(CharTermAttribute.class);  
            word.set(charTermAttribute.toString());  
            context.write(word, one);
        }  
    	
    }
  }
 

  public static class IntSumReducer extends
      Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      /*Configuration conf=context.getConfiguration();
      int k=Integer.valueOf(conf.get("k"));
      if (sum<k){
    	  return;
      }*/
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs =
	        new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(train.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}