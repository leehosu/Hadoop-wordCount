package airline_test;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import common.Parser;

public class airlineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Parser parser = new Parser(value);
		
		String carrier  = parser.getAirlineName();
		
		int DepDelay = parser.getArrDelay();
		
		if (DepDelay > 0) {
			context.write(new Text(carrier), new IntWritable(DepDelay));
		}
	}
}
