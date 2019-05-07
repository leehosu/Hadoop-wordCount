package airline_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

public class dayAvg { 

  public static void main(String[] args) throws Exception { 
    if (args.length != 2) { 
      System.err.println("Usage: AvgTemp <input path> <output path>"); 
      System.exit(-1); 
    } 
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "avg date"); 
    job.setJarByClass(dayAvg.class); 

    job.setMapperClass(airlineMapper.class); 
    job.setReducerClass(airlineReducer.class); 
  
    job.setMapOutputKeyClass(Text.class);  //setMapOutputKeyClass() 
    job.setMapOutputValueClass(IntWritable.class);   //setMapOutputValueClass() 

    job.setOutputKeyClass(Text.class);  //setMapOutputKeyClass() //Default
    job.setOutputValueClass(DoubleWritable.class);   //setMapOutputValueClass() 

    FileInputFormat.addInputPath(job, new Path(args[0])); 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); 

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
  }
} 