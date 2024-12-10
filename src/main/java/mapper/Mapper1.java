package mapper;

import driver.StockDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private String filter;
//处理order
    @Override
    public void setup(Context context) {
        filter = StockDriver.conf.get("filter.param");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // import order data
        String[] fields = value.toString().trim().split("\\s+|\\t+");
        String SecurityID = fields[8];  // StockID
        if (SecurityID.equals(filter)){  // filter out order data
            String ApplSeqNum = fields[7];
            String TransactTime = fields[12];
            Long transactTime = Long.parseLong(TransactTime);//委托时间
            StockDriver.orderMap.put(ApplSeqNum, transactTime);//(委托编号，委托时间)的映射关系
//
        }
    }
}

