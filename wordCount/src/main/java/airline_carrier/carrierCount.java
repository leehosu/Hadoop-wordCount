package airline_carrier;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class carrierCount {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();			//객체 생성
		Job job = Job.getInstance(conf, "항공사별 카운트");			//job 객체도 생성
		job.setJarByClass(carrierCount.class);
		job.setMapperClass(airlineMapper.class);
		job.setReducerClass(airlineReducer.class);
		job.setOutputKeyClass(Text.class);	//reducer의 출력 key 타입
		job.setOutputValueClass(IntWritable.class);	//reducer의 출력 value 타입
		job.setInputFormatClass(TextInputFormat.class);  //입력포맷 지정 
		job.setOutputFormatClass(TextOutputFormat.class);  //출력포맷 지정 
		FileInputFormat.addInputPath(job, new Path(args[0]));		//input 경로로(입력 경로)!!!
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		//output 경로로(출력 경로)!!!
		System.exit(job.waitForCompletion(true)? 0:1);
 	}
}
