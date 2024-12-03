package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private String filter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // read filter condition from configuration
        filter = context.getConfiguration().get("filter.param");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // import order data
        String[] fields = value.toString().split(",");
        String SecurityID = fields[8];  // StockID
        if (SecurityID.equals(filter)){  // filter out order data
            String ApplSeqNum = fields[7];
            String TransactTime = fields[12];
            context.write(new Text(ApplSeqNum), new Text(TransactTime+" 0"));
        }
    }
}

