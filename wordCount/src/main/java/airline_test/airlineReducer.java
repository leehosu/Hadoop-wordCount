package airline_test;

import java.io.IOException; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 

public class airlineReducer	extends Reducer<Text, IntWritable, Text, DoubleWritable> { 

	  public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		  throws IOException, InterruptedException { 

		 int sum = 0;
		 int tmp =0;
		 double avg =0.0;
		 
		 for(IntWritable value :values) {
			 sum += value.get();
			 tmp++;
		 }
		 
		 avg = (double)sum/tmp;
		 
	    context.write(key, new DoubleWritable(avg)); 
	  } 
}
