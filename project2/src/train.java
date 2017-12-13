

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.StringReader;  
import java.lang.Math;
import org.apache.lucene.analysis.Analyzer;  
import org.apache.lucene.analysis.TokenStream;  
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;  
import org.wltea.analyzer.lucene.IKAnalyzer;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class train {

  public static class TokenizerMapper extends
      Mapper<Object,   Text, Text, Text> {


    private Text word = new Text();
    private Text filetitle = new Text();
      
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    	
    	 String sourceStr = value.toString();
        // String[] sourceStrArray = sourceStr.split("  ");         
		IKAnalyzer analyzer = new IKAnalyzer();  
        // 使用智能分词  
        analyzer.setUseSmart(true); 
      /*  if (sourceStrArray.length<3){
        	return;
        }*/
        InputSplit inputSplit = context.getInputSplit();

        Path path=((FileSplit) inputSplit).getPath();
        String  fileName = path.getName();
        String strtype = path.getParent().getName();
        if(strtype.contains("positive")){
        	strtype="P";// 正类样本标记
        }else if(strtype.contains("negative")){
        	strtype="N";// 负类样本标记
        }else{
        	strtype="O";// 中性样本标记
        }
        String strtrainsymbol = path.getParent().getParent().getName();
        fileName=fileName.substring(0, fileName.length()-4);
        System.out.println(fileName+"fileName");
        TokenStream tokenStream = analyzer.tokenStream("content",  
                new StringReader(sourceStr));  
        while (tokenStream.incrementToken()) {  
            CharTermAttribute charTermAttribute = tokenStream  
                    .getAttribute(CharTermAttribute.class);  
            word.set(charTermAttribute.toString());  
            filetitle.set(strtrainsymbol+strtype+fileName+":1");
            context.write(word, filetitle);
           /* type.set(strtype);
            context.write(type, filetitle);
            trainsymbol.set(strtrainsymbol);
            context.write(trainsymbol, filetitle);*/
            
        }  
        
        

    	
    }
  }
 
  public static class TfidfMapper extends
  Mapper<Object,   Text, Text, Text> {

private Text word = new Text();
private Text doc = new Text();
  
public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
	 String title;
	 int conut;
	 String[] v = value.toString().split("\t");
	 String strword=v[0];
	 
	 String sourceStr = v[1];
	 String[] a=sourceStr.split(",");
	 ArrayList<String> titlelist = new ArrayList<String>();
     ArrayList<Integer> tf = new ArrayList<Integer>();
     ArrayList<Double> tfidf = new ArrayList<Double>();
     
   	  for (int j=0;j<a.length;j++){
   		  String[] b=a[j].split(":");
   		  title=b[0];
   		  conut=Integer.parseInt(b[1]);
   		  int index=titlelist.indexOf(title);
       	  if (index==-1){
       		  titlelist.add(title);
       		  tf.add(conut);    		  
       	  }
       	  else{
       		  tf.set(index, tf.get(index)+conut) ;
       	  }
       	  }
      double idf=Math.log(50.0/titlelist.size());
        for(int i=0;i<titlelist.size();i++){
      	  tfidf.add(idf*tf.get(i));
        }
        
        //String strresult="";
        for(int i=0;i<titlelist.size();i++){
        	doc.set(titlelist.get(i));
        	word.set(strword+":"+tfidf.get(i).toString());
        	context.write(doc, word);
        	//strresult=strresult+titlelist.get(i).toString()+":"+tfidf.get(i).toString()+",";
      	}
        /*if(getmax(tfidf)<10){
      	  return;
        }   */	  
     //context.write(word, filetitle);
     
    

	
}
}
  public static class IntSumReducer extends
      Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    //private MultipleOutputs<Text, Text> muloutputs;  
    int id ;
    @Override  
    public void setup(Context context) throws IOException, InterruptedException {  
        //System.out.println("enter LogReducer:::setup method");  
    	//muloutputs = new MultipleOutputs(context);  
    	id = 0;;
    }  
  
    @Override  
    public void cleanup(Context context) throws IOException, InterruptedException {  
        //System.out.println("enter LogReducer:::cleanup method");  
    	//muloutputs.close();  
    } 
    public double getmax(ArrayList<Double> tfidf) throws IOException, InterruptedException { 
    	double max=0;
    	for(int i=0;i<tfidf.size();i++){
    		if(tfidf.get(i)>max){
    			max=tfidf.get(i);
    		}
    	}
    	return max;
    }
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
     
      int conut;
      String title;
      ArrayList<String> titlelist = new ArrayList<String>();
      ArrayList<Integer> tf = new ArrayList<Integer>();
 
      for (Text val : values) {
    	  String[] a=val.toString().split(",");
    	  for (int j=0;j<a.length;j++){
    		  String[] b=a[j].split(":");
    		  title=b[0];
    		  conut=Integer.parseInt(b[1]);
    		  int index=titlelist.indexOf(title);
        	  if (index==-1){
        		  titlelist.add(title);
        		  tf.add(conut);    		  
        	  }
        	  else{
        		  tf.set(index, tf.get(index)+conut) ;
        	  }
    	  }
    	 
        //sum += val.get();
      }
      /*double idf=Math.log(50.0/titlelist.size());
      for(int i=0;i<titlelist.size();i++){
    	  tfidf.add(idf*tf.get(i));
      }
      if(getmax(tfidf)<10){
    	  return;
      }*/
      String strresult="";
      for(int i=0;i<titlelist.size();i++){
    	  strresult=strresult+titlelist.get(i).toString()+":"+tf.get(i).toString()+",";
    	  }
      result.set(strresult);
      /*Text a=new Text();
      i=i+1;
      a.set(String.valueOf(i)+key.toString());
      if(key.toString().contains("train")){
    	  muloutputs.write("train", a, result);
      }
      else{
    	  muloutputs.write("test", a, result);
      }*/
     // context.write(a, result);
      /*Configuration conf=context.getConfiguration();
      int k=Integer.valueOf(conf.get("k"));
      if (sum<k){
    	  return;
      }*/
     /* result.set(sum);
      context.write(key, result);
      Text a=new Text();
      i=i+1;
      a.set(String.valueOf(i));*/
      id=id+1;
      key.set(String.valueOf(id));
     context.write(key, result);
    }
  }

  public static class VectorizeReducer extends
  Reducer<Text, Text, Text, Text> {
	  private MultipleOutputs<Text, Text> muloutputs;
	  Text vectorText=new Text();
	  
	  @Override  
	  public void setup(Context context) throws IOException, InterruptedException {  
	        //System.out.println("enter LogReducer:::setup method");  
	    	muloutputs = new MultipleOutputs<Text, Text>(context); 	    	
	    }  
	  
	    @Override  
	  public void cleanup(Context context) throws IOException, InterruptedException {  
	        //System.out.println("enter LogReducer:::cleanup method");  
	    	muloutputs.close();  
	    }
public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
	String strkey=key.toString();
	String vector;//label
	if(strkey.contains("P")){
		vector="1";//positive label
	}else if(strkey.contains("N")){
		vector="2";//negative label
	}else{
		vector="3";//neutral label
	}
	
	for (Text val : values) {
		vector=vector+" "+val.toString();	     
	 }
	//训练集 测试集 的文本向量分别输出
	if(strkey.contains("train")){ 
	  	  muloutputs.write("train",key, vectorText);
	}else{
	  	  muloutputs.write("test",key, vectorText);
	}
	vectorText.set(vector);
	context.write(key, vectorText);
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
	    String out1="/wcout05";
	    conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true"); 
	    Job job = Job.getInstance(conf, "WordCount & InvertedIndex");
	    job.setJarByClass(train.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    //MultipleOutputs.addNamedOutput(job, "train", TextOutputFormat.class, Text.class, Text.class);                                                                                                                      
        //MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, Text.class, Text.class); 
	    //job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(out1));
	    
	    job.waitForCompletion(true);
	    Job job2 = Job.getInstance(conf, "Tf-idf & Vectorization");
	    job2.setJarByClass(train.class);
	    job2.setMapperClass(TfidfMapper.class);
	    job2.setReducerClass(VectorizeReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    //job2.setInputFormatClass(SequenceFileInputFormat.class);
	    MultipleOutputs.addNamedOutput(job2, "train", TextOutputFormat.class, Text.class, Text.class);                                                                                                                      
        MultipleOutputs.addNamedOutput(job2, "test", TextOutputFormat.class, Text.class, Text.class); 	    
	    FileInputFormat.addInputPath(job2, new Path(out1));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
	    //FileSystem.get(conf).delete(new Path(out1));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	    
	  }
	}