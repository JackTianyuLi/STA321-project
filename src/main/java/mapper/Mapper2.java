package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Integer TimeWindow;
    private String filter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // read time window and filter from configuration
        TimeWindow = Integer.parseInt(context.getConfiguration().get("timeWindow.param"));
        filter = context.getConfiguration().get("filter.param");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // load and process trade data
        String[] fields = value.toString().split("\t");
        String SecurityID = fields[8];
        String ExecType = fields[14];
        if (SecurityID.equals(filter) && ExecType.equals("F")){ // filter out executed trade data
            String ApplSeqNum = fields[7];  // trade ID
            String BidApplSeqNum = fields[10];  // Purchase order ID
            String OfferApplSeqNum = fields[11]; // Sell order ID
            String Price = fields[12];
            String TradeQty = fields[13]; // trade quantity
            String tradeTime = fields[15];
            // create key for each record according to trade time and time window(s)
            String timeWindowKey = createTimeKey(tradeTime);
            context.write(new Text(timeWindowKey),
                    new Text(ApplSeqNum+" "+BidApplSeqNum+" "+OfferApplSeqNum+" "+Price+" "+TradeQty+" 2"));
        }
    }
    private String createTimeKey(String tradeTime){
        // trade time format: yyyyMMddHHmmSSsss
        // extract y, M, d, H, m, S, s
        int year = Integer.parseInt(tradeTime.substring(0,4));
        int month = Integer.parseInt(tradeTime.substring(4,6));
        int day = Integer.parseInt(tradeTime.substring(6,8));
        int hour = Integer.parseInt(tradeTime.substring(8,10));
        int minute = Integer.parseInt(tradeTime.substring(10,12));
        int second = Integer.parseInt(tradeTime.substring(12,14));  // 获取秒部分
        int millisecond = Integer.parseInt(tradeTime.substring(14,17));

        // total number of seconds
        int totalSeconds = (hour * 3600 + minute * 60 + second);

        // key for time slot
        int timeSlot = totalSeconds / TimeWindow;

        // key for ms
        int ms = millisecond / (TimeWindow * 1000);

        // generate key
        return String.format("%d%d%d%d%d", year, month, day, timeSlot, ms);
    }
}
