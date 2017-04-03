package BDataP3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by lsolorzano on 3/24/2017.
 */
public class TwitterScreenNameDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TwitterScreenNameDriver <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(BDataP3.TwitterScreenNameDriver.class);
        job.setJobName("Count TweetsbyScreenName");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TwitterScreenNameCountMapper.class);
        job.setReducerClass(TwitterScreenNameReducer.class);
        //job.setCombinerClass(edu.uprm.cse.bigdata.mrsp02.TwitterReduceByScreenName.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}