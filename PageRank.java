/*Vivekananda Adepu
	Page-Rank Implmentation
*/

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PageRank extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( PageRank.class);

   public static void main( String[] args) throws  Exception {
      int pageRankRes  = ToolRunner .run( new PageRank(), args);
      System .exit(pageRankRes);
   }

   public int run( String[] args) throws  Exception {
//job is for the LinkGraph part of the program
      Job job  = Job .getInstance(getConf(), " pagerank ");
      job.setJarByClass( this .getClass());
      
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]+"/LinkGraph"));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( IntWritable .class);
      job.setOutputValueClass(  Text.class);
      job.waitForCompletion( true);
//job1 is for the iterations in calculating PageRank of the pages
      Job job1,job2;
      int i=0;
      for(i=0;i<10;i++)
      {
          job1  = Job .getInstance(getConf(), " pagerank ");
          job1.setJarByClass( this .getClass());
          
          if(i==0)
        	  FileInputFormat.addInputPaths(job1,  args[1]+"/LinkGraph");
          else
        	  FileInputFormat.addInputPaths(job1,  args[1]+"/ranks"+(i-1));
          FileOutputFormat.setOutputPath(job1,  new Path(args[1]+"/ranks"+i));
          job1.setMapperClass( Map1 .class);
          job1.setReducerClass( Reduce1 .class);
          job1.setOutputKeyClass( Text .class);
          job1.setOutputValueClass(  Text.class);
          job1.waitForCompletion( true);
      }
//job2 is for the Cleanup and Sorting of the PageRank values
      job2  = Job .getInstance(getConf(), " pagerank ");
      job2.setJarByClass( this .getClass());
  	  FileInputFormat.addInputPaths(job2,  args[1]+"/ranks"+(i-1));
      FileOutputFormat.setOutputPath(job2,  new Path(args[1]+"/CleanSort"));
      job2.setMapperClass( Map2 .class);
      job2.setReducerClass( Reduce2 .class);
      job2.setOutputKeyClass( DoubleWritable .class);
      job2.setOutputValueClass(  Text.class);
// Remove all the temporary files
      Configuration conf = getConf();
      FileSystem f=FileSystem.get(conf);
      Path p=new Path(args[1]+"/LinkGraph");
      if(f.exists(p))
    	  f.delete(p,true);
      for(int j=0;j<9;j++)
      {
          Path p1=new Path(args[1]+"/ranks"+j);
          if(f.exists(p1))
        	  f.delete(p1,true);
      }
      if(job2.waitForCompletion( true))
    	  {
          Path p2=new Path(args[1]+"/ranks"+9);
          if(f.exists(p2))
        	  f.delete(p2,true);
    	  return 0;
    	  }
      else
    	  return 1;
   }
   
   
   public static class Map extends Mapper<LongWritable ,  Text ,  IntWritable ,  Text > {
	      private final static IntWritable one  = new IntWritable( 1);
	      private Text word  = new Text();
//Pattern is used to retrieve the data using regular expressions. Since the input is in XML format
	      private static final Pattern title_tag_data = Pattern .compile("(<title>)(.*)(</title)");
	     private static final Pattern title_tag_data1 = Pattern .compile("(\\[\\[)(.*?)(\\]\\])");
	      
	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	          String line  = lineText.toString();
	          
	          Text currentWord  = new Text();
	          String word = null,word2 = null;
//matcher is used to match the given pattern to the data
	          Matcher matcher = title_tag_data.matcher(line);
	          Matcher matcher1 = title_tag_data1.matcher(line);
	          while(matcher.find())
	          {
	        	  int x=0;
	        	  word= matcher.group(2)+"-!>";
	        	  while( matcher1.find())
	        	  {
	        		  x++;
	        		  word2= matcher1.group(2);
//appending sequence of characters to the intermediate data helps improve the output for further calculations
	        		  if(!word.endsWith("-!>"))
	                  	  word= word.toLowerCase()+"#!#"+word2.toLowerCase();
	        		  else
	        			  word= word.toLowerCase()+word2.toLowerCase();
	        	  }
//tried using unicode sequencing(considering arabic text) but did not work for i have to append the above sequence of characters to the end of the line
	        	  //word=word+"\u200E"+"=!!="+x;
	        	  currentWord  = new Text(word);
	             context.write(one,currentWord);
	          }
	      }
	   }

	      public static class Reduce extends Reducer<IntWritable ,Text,  Text ,  DoubleWritable  > {
	          public void reduce(   IntWritable counts ,Iterable<Text> word,  Context context)
	             throws IOException,  InterruptedException {
//Store the iterable text in an array List 
	        	 double x=0; 
	        	 ArrayList<String> source= new ArrayList<String>();
	        	 for(Text word1:word)
	             {
	        		 x++;
	        		 source.add(word1.toString());
	             }
	        	 double n;
	        	 n=1/x;
//Finding the number of pages and calculating the default PageRank value
	        	 for(int i=0;i<source.size();i++)
	        		 context.write(new Text(source.get(i)), new DoubleWritable(n));
	             
	          }
	       }
	      	public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	    	private Text word  = new Text();

	          private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

	          public void map( LongWritable offset,  Text lineText,  Context context)
	            throws  IOException,  InterruptedException {

	             String line  = lineText.toString();
	             String word, pageRank, value;
	             String  outLink, outLinks , node;
	             int x=0;
	             double NoL;
	             double d=0.0;
// For the iterations we have to first split the line to get the pagerank and node values
	             outLink=line.split("\\t")[0];
	             pageRank=line.split("\\t")[1];
	             node=outLink.split("-!>")[0];
	             double PR=Double.parseDouble(pageRank);
//we calculate the key value pairss only when there are valid outlinks
	             if(outLink.split("-!>").length>1)
	             {
		             outLink=outLink.split("-!>")[1];
		            	 x=0;
	            	 String[] url=outLink.split("#!#");
	            	 for(String word1:url)
	            	 {
	            		 x++;
	            	 }
	            	 NoL=x;
	            	 d =PR/NoL;
	            	 value=Double.toString(d);
		        	 	 for(String word1:url)
		            	 	{
			                 if(!node.equals(outLink))
			            	 {
	            			 context.write(new Text(word1),new Text(value));
		            	 	}
			                 if(!node.equals(outLink))
			                	 context.write(new Text(node), new Text(outLink+"#!#"));
		            	 }
	             }
	             else 
		             context.write(new Text(node), new Text("#!#"));
	          }
	       }
	       
	      public static class Reduce1 extends Reducer<Text ,  Text ,  Text ,  Text > {
	          public void reduce( Text word,  Iterable<Text> counts,  Context context)
	             throws IOException,  InterruptedException {
	            	 double d=0.85,PR=0.0, sum=0.0;
	            	 String outLink = null ,value;
	            	 for(Text word1:counts)
	            	 {
	            		 String S=word1.toString();
	            		 if(S.contains("#!#"))
	            			 outLink = word.toString()+"-!>"+S.substring(0, S.length()-3);
	            		 else
	            		 {
	            			 PR=Double.parseDouble(word1.toString());
	            			 sum=PR+sum;
	            		 }
	            	 }
//calculating the pageranks for incoming links using the formula (1-d)+d(PR(in)/outlinks)
	            	 sum=0.15+0.85*sum;
	            	 value=Double.toString(sum);
	            	 if(outLink!=null)
	            		 context.write(new Text(outLink), new Text(value));
	            	 }
	      }
	      public static class Map2 extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
		      private final static IntWritable one  = new IntWritable( 1);
		      private Text word  = new Text();
		      
		      public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {
		             String line  = lineText.toString();
		             String pageRank, outLink, node;
// For cleaning and sorting the pagerank values we have to pass negation of each value to the reducer 
		             outLink=line.split("\\t")[0];
		             pageRank=line.split("\\t")[1];
		             node=outLink.split("-!>")[0];
		             double PR=Double.parseDouble(pageRank);
		             PR=-PR ;
		             context.write(new DoubleWritable(PR),new Text(node));
		             }
	      }
	      public static class Reduce2 extends Reducer<DoubleWritable ,Text,  Text ,  DoubleWritable  > {
		          public void reduce(   DoubleWritable counts ,Iterable<Text> word,  Context context)
		             throws IOException,  InterruptedException {
// The negative values are sorted in reverse order by defult reducer and now we have to convert the value from negative to positive
		        	 double x=-counts.get(); 
		        	 for(Text word1:word)
		             {
		        		 context.write(word1, new DoubleWritable(x));
		             }
		          }
		       }
}
