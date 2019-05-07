package airline_carrier;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.Parser;

public class airlineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outKey = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Parser parser = new Parser(value);
		outKey.set(parser.getAirlineName());
		context.write(outKey, one);
		
//		if (parser.getDepartureDelayTime() > 0) {
//		}
	}
}