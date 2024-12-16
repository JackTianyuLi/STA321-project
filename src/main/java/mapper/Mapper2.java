package mapper;

import driver.StockDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private Integer TimeWindow;
    private String filter;

    @Override
    public void setup(Context context) throws IOException, InterruptedException { //get conf. param
        String timeWindowStr = StockDriver.conf.get("timeWindow.param");
        if (timeWindowStr != null) {
            TimeWindow = Integer.parseInt(timeWindowStr); // convert to integer
        }
        filter = StockDriver.conf.get("filter.param");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().trim().split("\\s+|\\t+");// split w.r.t. blank space or \t
                String SecurityID = fields[8];
                String ExecType = fields[14]; // execution type
                if (SecurityID.equals(filter) && ExecType.equals("F")) { // filter out executed trade data
                    String ApplSeqNum = fields[7];  // trade ID
                    String BidApplSeqNum = fields[10];  // Purchase order ID
                    String OfferApplSeqNum = fields[11]; // Sell order ID
                    String Price = fields[12];
                    String TradeQty = fields[13]; // trade quantity
                    String tradeTime = fields[15];
                    // create key for each record according to trade time and time window(sec)
                    String timeWindowKey = createTimeKey(tradeTime);
                    if (timeWindowKey != null) {
                        context.write(new Text(timeWindowKey),//set key according to time window
                                new Text(ApplSeqNum + " " + BidApplSeqNum + " " + OfferApplSeqNum + " " + Price + " " + TradeQty));
                    }
                    // return all trade data in the given period of time
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String createTimeKey(String tradeTime) { //convert input time to the start of a time window
        // trade time format: yyyyMMddHHmmSSsss
        // extract y, M, d, H, m, S, s
        int year = Integer.parseInt(tradeTime.substring(0, 4));
        int month = Integer.parseInt(tradeTime.substring(4, 6));
        int day = Integer.parseInt(tradeTime.substring(6, 8));
        int hour = Integer.parseInt(tradeTime.substring(8, 10));
        int minute = Integer.parseInt(tradeTime.substring(10, 12));

        // set base time as 9:30
        int baseHour;
        int baseMinute;
        if(hour == 15||(hour == 9 && minute<30)){
            return null;
        }
        if(hour <=12){
             baseHour = 9;
             baseMinute = 30;
        } else{
            baseHour = 13;
            baseMinute = 0;
        }
        // convert input time to # of minutes
        int inputTotalMinutes = (hour * 60 + minute);

        // convert base time to # of minutes
        int baseTotalMinutes = (baseHour * 60 + baseMinute);

        // calculate time interval
        int timeSlotIndex;
        if (inputTotalMinutes < baseTotalMinutes) {
            // before 9:30: set base time w.r.t. current time interval
            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes - (TimeWindow / 60)) / (TimeWindow / 60);
        } else {
            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes) / (TimeWindow / 60);
        }
        // calculate new # of minutes, return the start time of next interval
        int newBaseMinute = baseMinute + timeSlotIndex * (TimeWindow / 60);
        // if newBaseMinute > 59: convert to an hour
        int finalHour = baseHour + newBaseMinute / 60;
        newBaseMinute = newBaseMinute % 60;
        // output with accurate to minute
        return String.format("%d年%02d月%02d日%02d点%02d分", year, month, day, finalHour, newBaseMinute);
    }
}
