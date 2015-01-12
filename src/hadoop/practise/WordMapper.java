package hadoop.practise;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Mapper java file for the Hello World  - Word Count project
 * Created by DPN on 1/7/15.
 */
public class WordMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    //We can also define the followings for the Output
    //private final static IntWritable countOne = new IntWritable(1);
    //private final Text reusableText = new Text();
    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> Output, Reporter r) throws IOException {
        // LongWritable key = 0, Text value = Hi How are you,
        String s = value.toString();
        for (String word : s.split(" ")) {
            if (word.length() > 0) {

                Output.collect(new Text(word), new IntWritable(1));
            }
        }
    }
}
