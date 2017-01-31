import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FilterMap extends Mapper<LongWritable , Text, Text, LongWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String content = value.toString();
        String[] lines = content.split("\\n");
		String[] tokens = null;

		for(int i =0; i < lines.length; i++)
		{
		    tokens = lines[i].split("`");
                    word.set(tokens[4]);
                    context.write(word, new LongWritable(Long.parseLong(tokens[5])));
        }
    }
 } 