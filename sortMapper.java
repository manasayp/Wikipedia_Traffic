import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class sortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
 public void map(LongWritable key, Text value, Context context)
   throws IOException, InterruptedException {

  String[] splits = value.toString().trim().split("\\s+");
  context.write(new LongWritable(Long.parseLong(splits[1])), new Text(
    splits[0]));
 }

}