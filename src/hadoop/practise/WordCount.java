package hadoop.practise; /**
 * DriverCode for the HelloWord Job: hadoop.practise.WordCount
 * Created by DPN on 1/7/15.
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
    public int run(String args[]) throws Exception {
        if (args.length < 2) {
            System.out.println("Please give Input and Output directories properly");
            return -1;
        }
        //We must create object for this class. pass DriverCode
        JobConf conf = new JobConf(WordCount.class);
        //conf.setJobName("WordCount");

        //Configure Output and Input source
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //Configure Mapper and Reducer
        conf.setMapperClass(WordMapper.class);
        conf.setReducerClass(WordReducer.class);

        //Configure Output Keys and Values
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        JobClient.runJob(conf);
        //   return job.waitForCompletion(true) ? 0 : 1;
        return 0;
    }

    // Write main method
    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }
}