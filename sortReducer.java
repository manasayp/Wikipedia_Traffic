import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class sortReducer extends
  Reducer<LongWritable, Text, Text, LongWritable> {

 @Override
 public void reduce(LongWritable key, Iterable<Text> values, Context context)
   throws IOException, InterruptedException {

  for (Text val : values) {
   context.write(val, key);
  }
 }
}
