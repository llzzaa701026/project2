
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.StringReader;  
import java.lang.Math;
import java.net.URI;
import org.apache.lucene.analysis.Analyzer;  
import org.apache.lucene.analysis.TokenStream;  
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;  
import org.wltea.analyzer.lucene.IKAnalyzer;

import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
		    int id ;
		    @Override  
		    public void setup(Context context) throws IOException, InterruptedException {
		    	id = 0;
		    }  
		  
		    @Override  
		    public void cleanup(Context context) throws IOException, InterruptedException {  
		    	
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
	  	  muloutputs.write("trainKNN",key, vectorText);
	  	  muloutputs.write("trainSVM",vectorText,null);
	}else{
	  	  muloutputs.write("test",key, vectorText);
	}
	vectorText.set(vector);
	context.write(key, vectorText);
}
}
  public static class getDataFromHDFS{
	  FileSystem fileSystem;
	  FSDataInputStream in=null;
	  public getDataFromHDFS(String uri, Configuration conf) throws IOException, InterruptedException{
		  fileSystem=FileSystem.get(URI.create(uri), conf); 
		  in=fileSystem.open(new Path(uri));
	}

	
	  
	  public String getstring()throws IOException, InterruptedException {		 	          
	        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));    
	        String strout= "";	
	        String lineStr;
	        while ((lineStr = bufferedReader.readLine()) != null){
	        	strout=strout+lineStr+"\n\n";
	        	//System.out.println("aaaaa"+lineStr);
	        }
	        IOUtils.closeStream(in); 
	        return strout;
	        
	  }
  }
  
  public static class SVMModel{
	  Configuration conf;
	  String SVMtrainSetPath;
	  String modelPath;
	  public SVMModel(String trainSetPath,Configuration config) throws IOException{
		  SVMtrainSetPath=trainSetPath;
		  modelPath="SVMmodel";
          conf=config;
	  }
	  
	  public void SVMtrain() throws IOException, InterruptedException{
		  	String uri="hdfs://localhost:9000";
		  	String trainset="trainsetSVM";
		  	
		  	File trainsetpath=new File(trainset);
		  	if(!trainsetpath.exists()){
		  		trainsetpath.createNewFile();
		  	}
		  	String SVMtrainSetPath1="/user/lza/"+SVMtrainSetPath;  
	        FileSystem fs=FileSystem.get(URI.create(uri), conf);  
	        InputStream in = fs.open(new Path(uri+SVMtrainSetPath1));
	        OutputStream out = new FileOutputStream(trainset);  
	        IOUtils.copyBytes(in, out, 4096, true);		       
		    String[] SVMarg = { "-t","0" ,trainset,modelPath};
		    svm_train.main(SVMarg);
		    
		    
	  }
	  
	  public void setParameter() throws IOException, InterruptedException{
		  File file = new File(modelPath); 
		  InputStreamReader reader = new InputStreamReader( new FileInputStream(file)); // 建立一个输入流对象reader  
		  BufferedReader br = new BufferedReader(reader);
		  String line = ""; 
		  line = br.readLine(); 
		  line = br.readLine(); 
		  line = br.readLine(); 
		  line = br.readLine(); 
		  String[] totalSV=line.split(" ");
		  conf.set("totalSV", totalSV[1]);
		  line = br.readLine(); 
		  String[] rho=line.split(" ");
		  conf.set("rho1", rho[1]);
		  conf.set("rho2", rho[2]);
		  conf.set("rho3", rho[3]);
		  line = br.readLine(); 
		  String[] label=line.split(" ");
		  conf.set("label1", label[1]);
		  conf.set("label2", label[2]);
		  conf.set("label3", label[3]);
		  line = br.readLine(); 
		  System.out.println(line);
		  String[] nSV=line.split(" ");
		  System.out.println(nSV[1]);
		  conf.set("nSV1", nSV[1]);
		  conf.set("nSV2", nSV[2]);
		  conf.set("nSV3", nSV[3]);
		  System.out.println(line);
		  line = br.readLine(); 
		  for(int i=0;i<3;i++){
			  String SV=label[i+1]+"SV";
			  for(int j=0;j<Integer.parseInt(nSV[i+1]);j++){
				  line = br.readLine(); 
				  conf.set(SV+j,line);
			  }
		  }
		  System.out.println(line);
		  //SupportVector b=new SupportVector(line,1);
	  }
  }
  
  
  public static class KNNMapper extends
  		Mapper<Object,   Text, Text, Text> {
		
		private Text testDoc = new Text();
		private Text trainDocDist = new Text();
		String trainData;
		private String[] TrainsetVector;
		public void setup(Context context) throws IOException, InterruptedException {  
	        System.out.println("Loading trainSet..."); 
	        Configuration conf=context.getConfiguration();
	        trainData=conf.get("TrainData");
	        TrainsetVector =trainData.split("\n\n");	    	
	    } 
		
		public double Dist(String vector1,String vector2) throws IOException, InterruptedException {  
	        double dist=0;
	        ArrayList<Integer> Vector1Index = new ArrayList<Integer>();
			ArrayList<Double> Vector1Value = new ArrayList<Double>();
			
			//System.out.println(vector1);
			String[] v1=vector1.split("\t");	
			if(v1.length==1){
				dist=Math.random()*10000;
				return dist;
			}
			String[] Vector1Item=v1[1].split(" ");
			for(int i=1;i<Vector1Item.length;i++){
				String[] Item=Vector1Item[i].split(":");
				Vector1Index.add(Integer.parseInt(Item[0]));
				Vector1Value.add(Double.parseDouble(Item[1]));
			}
			
			String[] v2=vector2.split("\t");	
			String[] Vector2Item=v2[1].split(" ");
			for(int i=1;i<Vector2Item.length;i++){
				String[] Item=Vector2Item[i].split(":");
				int index=Vector1Index.indexOf(Integer.parseInt(Item[0]));
				if(index==-1){
	        		 continue;  		  
	        	}else{
	        		dist=dist+Math.pow(Vector1Value.get(index)-Double.parseDouble(Item[1]),2);
	        	}
			}
	        return dist;    	
	    }
		
		public void map(Object key, Text value, Context context)
		    throws IOException, InterruptedException {
			 String doc1,doc2;
			 double dist;
			 String TestsetVector = value.toString();
			 for(int i=0;i<TrainsetVector.length;i++){
				 dist=Dist(TestsetVector,TrainsetVector[i]);
				 doc1=TestsetVector.split("\t")[0];
				 doc2=TrainsetVector[i].split("\t")[0];
				 testDoc.set(doc1);
				 trainDocDist.set(doc2+":"+Double.toString(dist));
				 context.write(testDoc, trainDocDist);
			 }
			 
		}
}
 
  public static class KNNReducer extends
		  Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		int id ;
		@Override  
		public void setup(Context context) throws IOException, InterruptedException {
			id = 0;
		}  
		
		@Override  
		public void cleanup(Context context) throws IOException, InterruptedException {  
			
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
			double mindist=999999;
			double dist;
			String label;
			String nearestdoc=null;
			Text predictLabel=new Text();
			for (Text val : values) {
				dist=Double.parseDouble(val.toString().split(":")[1]);
				if(dist<mindist){
					mindist=dist;
					nearestdoc=val.toString().split(":")[0];
				}
			}
			if(nearestdoc.contains("P")){
				label="positive";
			}else if(nearestdoc.contains("N")){
				label="negative";
			}else{
				label="neutral";
			}
			predictLabel.set(label);
			context.write(key, predictLabel);
		}
  }
  
 
  public static class SupportVector{
	  
	  String type;
	  double[] coef =new double[2];
	  svm_node[] node;
	  public SupportVector(String strvector,String label) throws IOException, InterruptedException{
		  type=label;
		  StringTokenizer st = new StringTokenizer(strvector," \t\n\r\f:");
		  coef[0]=Double.parseDouble(st.nextToken());
		  coef[1]=Double.parseDouble(st.nextToken());
		  int m=st.countTokens()/2;
		  node=new svm_node[m];
		  for(int i=0;i<m;i++){
			  node[i] = new svm_node();
			  node[i].index = Integer.parseInt(st.nextToken());
			  node[i].value = Double.parseDouble(st.nextToken());
		  }		  
	  }
  }
  
  
  public static class svm_node implements java.io.Serializable
  {
  	public int index;
  	public double value;
  }
  public static class SVMMapper extends
	Mapper<Object,   Text, Text, Text> {
	  
	
	String trainData;
	private SupportVector[] sv1;
	private SupportVector[] sv2;
	private SupportVector[] sv3;
	private int nsv1,nsv2,nsv3;
	private String label1,label2,label3;
	Configuration conf;
	public void setup(Context context) throws IOException, InterruptedException {  
	      System.out.println("Loading trainSet..."); 
	      
	      conf=context.getConfiguration();
	      label1=conf.get("label1");
	      label2=conf.get("label2");
	      label3=conf.get("label3");
	      nsv1=Integer.parseInt(conf.get("nSV1"));
	      nsv2=Integer.parseInt(conf.get("nSV2"));
	      nsv3=Integer.parseInt(conf.get("nSV3"));
	      sv1=new SupportVector[nsv1];
	      sv2=new SupportVector[nsv2];
	      sv3=new SupportVector[nsv3];
	      for(int i=0;i<nsv1;i++){
	    	  sv1[i]=new SupportVector(conf.get(label1+"SV"+i),label1);
	      }
	      for(int i=0;i<nsv2;i++){
	    	  sv2[i]=new SupportVector(conf.get(label2+"SV"+i),label2);
	      }
	      for(int i=0;i<nsv3;i++){
	    	  sv3[i]=new SupportVector(conf.get(label3+"SV"+i),label3);
	      }
	      //String model=conf.get("SVMmodel");
	          	
  } 
	
	public double multiply(svm_node[] node1,svm_node[] node2) throws IOException, InterruptedException { 
		double sum=0;
		for(int i=0;i<node1.length;i++){
			for(int j=0;j<node2.length;j++){
				if(node1[i].index==node2[j].index){
					sum=sum+node1[i].value*node2[j].value;
				}
			}	
		}
		return sum;
	}
	
	public void map(Object key, Text value, Context context)
	    throws IOException, InterruptedException {
		 Text f=new Text();		 
		 Text doc=new Text();
		 String TestsetVector = value.toString();
		 String[] v1=TestsetVector.split("\t");	
		 doc.set(v1[0]);
		 if(v1.length==1){
			 return;
		 }
		 String[] Vector1Item=v1[1].split(" ");
		 
		 svm_node[] node=new svm_node[(Vector1Item.length-1)];
			for(int i=1;i<Vector1Item.length;i++){
				String[] Item=Vector1Item[i].split(":");
				node[i-1]=new svm_node();
				node[i-1].index=Integer.parseInt(Item[0]);
				node[i-1].value=Double.parseDouble(Item[1]);
			}
			double alpha1,alpha2,f12=0,f13=0,f21=0,f23=0,f31=0,f32=0;
			for(int i=0;i<nsv1;i++){
				alpha1=sv1[i].coef[0];
				alpha2=sv1[i].coef[1];				
				f12=f12+alpha1*multiply(node,sv1[i].node);
				f13=f13+alpha2*multiply(node,sv1[i].node);
			}
			
			
			for(int i=0;i<nsv2;i++){
				alpha1=sv2[i].coef[0];
				alpha2=sv2[i].coef[1];				
				f21=f21+alpha1*multiply(node,sv2[i].node);
				f23=f23+alpha2*multiply(node,sv2[i].node);
			}
			
			
			for(int i=0;i<nsv3;i++){
				alpha1=sv3[i].coef[0];
				alpha2=sv3[i].coef[1];				
				f31=f31+alpha1*multiply(node,sv3[i].node);
				f32=f32+alpha2*multiply(node,sv3[i].node);
			}
			f.set(label1+"v"+label2+":"+Double.toString(f12+f21));
			context.write(doc, f);
			f.set(label1+"v"+label3+":"+Double.toString(f13+f31));
			context.write(doc, f);
			f.set(label2+"v"+label3+":"+Double.toString(f23+f32));
			context.write(doc, f);
		 }
  	}
  
  public static class SVMReducer extends
  Reducer<Text, Text, Text, Text> {
	    private Text result = new Text();
	    public int getMax(double[] arr){
	    	int i=0;
	    	double max=-1;
	          for(i=0;i<arr.length;i++){
	              if(arr[i]>max){
	                   max=arr[i];
	              }
	         }
	          return i;
	     }
	    public void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
	    	double[] vote=new double[3];
	    	double f;
	     
	      for (Text val : values) {
	    	  String[] v=val.toString().split(":");
	    	  f=Double.parseDouble(v[1]);
	    	  String[] label=v[0].split("v");
	    	  if(f>0){
	    		  vote[Integer.parseInt(label[0])-1]+=f;
	    	  }else{
	    		  vote[Integer.parseInt(label[1])-1]+=-f;
	    	  }
	      }
	      if(getMax(vote)==0){
	    	  result.set("Positive");
	      }else if(getMax(vote)==1){
	    	  result.set("Negative");
	      }else{
	    	  result.set("Neutral");
	      }	      
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
	    String out1=otherArgs[1]+"y";
	    conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true"); 
	    Job job = Job.getInstance(conf, "WordCount & InvertedIndex");
	    job.setJarByClass(train.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(out1));
	    
	    job.waitForCompletion(true);
	    Job job2 = Job.getInstance(conf, "Tf-idf & Vectorization");
	    job2.setJarByClass(train.class);
	    job2.setMapperClass(TfidfMapper.class);
	    job2.setReducerClass(VectorizeReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    //训练集 测试集 的文本向量分别输出
	    //训练SVM和KNN的数据格式稍有不同 这里为了方便分别输出
	    MultipleOutputs.addNamedOutput(job2, "trainKNN", TextOutputFormat.class, Text.class, Text.class);
	    MultipleOutputs.addNamedOutput(job2, "trainSVM", TextOutputFormat.class, Text.class, Text.class);                                                                                                                      
        MultipleOutputs.addNamedOutput(job2, "test", TextOutputFormat.class, Text.class, Text.class); 	    
	    FileInputFormat.addInputPath(job2, new Path(out1));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
	    
	    job2.waitForCompletion(true);	    
	    String trainSetPath=otherArgs[1]+"/trainKNN-r-00000";  //训练集 
	    String testSetPath=otherArgs[1]+"/test-r-00000";    //测试集
	    getDataFromHDFS getData=new getDataFromHDFS(trainSetPath, conf);  
        String trainData=getData.getstring();
        conf.set("TrainData", trainData); //训练集 数据存为全局变量 为Mapper提供 
        //System.out.println(trainData);
	    //
	    
	    Job job3 = Job.getInstance(conf, "KNN");
	    job3.setJarByClass(train.class);
	    job3.setMapperClass(KNNMapper.class);
	    job3.setReducerClass(KNNReducer.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job3, new Path(testSetPath));
	    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+"x"));
	    
	    job3.waitForCompletion(true);
	    String SVMtrainSetPath=otherArgs[1]+"/trainSVM-r-00000";  //训练集
	    SVMModel svmModel=new SVMModel(SVMtrainSetPath,conf);
	    svmModel.SVMtrain();
	    svmModel.setParameter();
	    Job job4 = Job.getInstance(conf, "SVM");
	    job4.setJarByClass(train.class);
	    job4.setMapperClass(SVMMapper.class);
	    job4.setReducerClass(SVMReducer.class);
	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job4, new Path(testSetPath));
	    FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1]+"z"));
	    
	    
	    System.exit(job4.waitForCompletion(true) ? 0 : 1);
	    /*String testSetPath=otherArgs[1]+"/test-r-00000";
	    getDataFromHDFS getData2=new getDataFromHDFS(testSetPath, conf);  
        String testData=getData2.getstring();
        String a=testData.split("\n\n")[0];
        System.out.println(a);*/
	  }
	}