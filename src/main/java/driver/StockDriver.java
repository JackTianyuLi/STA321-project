package driver;//package driver;import mapper.Mapper1;import mapper.Mapper2;import org.apache.hadoop.conf.Configuration;import org.apache.hadoop.fs.FileSystem;import org.apache.hadoop.fs.Path;import org.apache.hadoop.io.DoubleWritable;import org.apache.hadoop.io.Text;import org.apache.hadoop.mapreduce.Job;import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;import reducer.StockReducer;import java.io.IOException;import java.util.ArrayList;import java.util.HashMap;public class StockDriver {    public static HashMap<String, Long> orderMap = new HashMap<>();    public static ArrayList<String> tradeTable = new ArrayList<>();    public static Configuration conf = new Configuration();    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {        // initialization and configuration        Job job = Job.getInstance(conf, "JoinExample");        job.setJarByClass(StockDriver.class);//        conf.set("mapreduce.output.textoutputformat.separator", " ");        conf.set("mapreduce.framework.name", "local"); // 1G        // set parameters        String SecurityIDQueried = "000001"; // stock number        String TWindow = "600"; // time window; unit: second        conf.set("filter.param", SecurityIDQueried);        conf.set("timeWindow.param", TWindow);        Path orderPath = new Path("data/am_hq_order_spot.txt");        Path tradePath = new Path("data/am_hq_trade_spot.txt");        Path outputPath = new Path("output");//        String SecurityIDQueried = args[0]; // stock number//        String TWindow = args[1]; // time window; unit: second//        conf.set("filter.param", SecurityIDQueried);//        conf.set("timeWindow.param", TWindow);//        Path orderPath = new Path(args[2]);//        Path tradePath = new Path( args[3]);//        Path outputPath = new Path(args[4]);//        STA321-project//        hadoop jar STA321-project.jar 000001 600 /data/am_hq_order_spot.txt /data/am_hq_trade_spot.txt /output        // set classes        MultipleInputs.addInputPath(job, tradePath, TextInputFormat.class, Mapper2.class);        // set mapper for order data        MultipleInputs.addInputPath(job, orderPath, TextInputFormat.class, Mapper1.class);        // set mapper for trade data        job.setReducerClass(StockReducer.class);        job.setNumReduceTasks(10);        job.setOutputKeyClass(Text.class);        job.setOutputValueClass(Text.class);        // set output path        // 获取文件系统        FileSystem fs = FileSystem.get(conf);//        System.out.println("delete output path: " + outputPath);        // 检查路径是否存在，如果存在则删除        if (fs.exists(outputPath)) {            fs.delete(outputPath, true); // true 表示递归删除        }        TextOutputFormat.setOutputPath(job, outputPath);//         System.out.println("output 252path: " + outputPath);        // submit task and wait for completion        try {            System.exit(job.waitForCompletion(true) ? 0 : 1);        } catch (Exception e) {            e.printStackTrace(); // 打印完整的异常信息            System.exit(1);        }    }}