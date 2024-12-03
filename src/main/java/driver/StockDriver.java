package driver;

import mapper.Mapper1;
import mapper.Mapper2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reducer.StockReducer;
import java.io.IOException;


public class StockDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // initialization and configuration
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "JoinExample");

        // set parameters
        String SecurityIDQueried = args[0]; // stock number
        String TWindow = args[1]; // time window; unit: second
        conf.set("filter.param", SecurityIDQueried);
        conf.set("timeWindow.param", TWindow);

        // set classes
        job.setJarByClass(StockDriver.class);
        // set mapper for order data
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Mapper1.class);
        // set mapper for trade data
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, Mapper2.class);
        job.setReducerClass(StockReducer.class);

        // set output type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        // set output path
        TextOutputFormat.setOutputPath(job, new Path(args[4]));

        // submit task and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
