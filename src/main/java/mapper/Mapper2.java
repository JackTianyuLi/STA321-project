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
    public void setup(Context context) throws IOException, InterruptedException {
        String timeWindowStr = StockDriver.conf.get("timeWindow.param");
        if (timeWindowStr != null) {
            TimeWindow = Integer.parseInt(timeWindowStr); // 转换为整数
        }
        filter = StockDriver.conf.get("filter.param");
    }//得到配置参数

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().trim().split("\\s+|\\t+");// 按空格或制表符分割
                String SecurityID = fields[8];
                String ExecType = fields[14];
                if (SecurityID.equals(filter) && ExecType.equals("F")) { // filter out executed trade data
                    String ApplSeqNum = fields[7];  // trade ID
                    String BidApplSeqNum = fields[10];  // Purchase order ID
                    String OfferApplSeqNum = fields[11]; // Sell order ID
                    String Price = fields[12];
                    String TradeQty = fields[13]; // trade quantity
                    String tradeTime = fields[15];
                    // create key for each record according to trade time and time window(s)
                    String timeWindowKey = createTimeKey(tradeTime);
                    context.write(new Text(timeWindowKey),//以时间窗口作为key
                            new Text(ApplSeqNum + " " + BidApplSeqNum + " " + OfferApplSeqNum + " " + Price + " " + TradeQty));
//            返回一段时间内所有的成交数据
                }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String createTimeKey(String tradeTime) {//此方法用于将输入的时间转换为时间窗口的起始时间
        // trade time format: yyyyMMddHHmmSSsss
        // extract y, M, d, H, m, S, s
        int year = Integer.parseInt(tradeTime.substring(0, 4));
        int month = Integer.parseInt(tradeTime.substring(4, 6));
        int day = Integer.parseInt(tradeTime.substring(6, 8));
        int hour = Integer.parseInt(tradeTime.substring(8, 10));
        int minute = Integer.parseInt(tradeTime.substring(10, 12));
        // 基准时间设置为 9:30
        int baseHour = 9;
        int baseMinute = 30;

        // 将输入的时间转换为绝对分钟数
        int inputTotalMinutes = (hour * 60 + minute);

        // 将 9:30 转换为绝对分钟数
        int baseTotalMinutes = (baseHour * 60 + baseMinute);

        // 计算时间间隔
        int timeSlotIndex;
        if (inputTotalMinutes < baseTotalMinutes) {
            // 输入时间在 9:30 之前，时间间隔的计算方式
            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes - (TimeWindow / 60)) / (TimeWindow / 60);; // -10 表示时间需要向前推进到所在的时间段
        } else {
            timeSlotIndex = (inputTotalMinutes - baseTotalMinutes) / (TimeWindow / 60);; // 正常情况下
        }
        // 计算新的分钟数，返回的时间点为下一个时间段的起始时间
        int newBaseMinute = baseMinute + timeSlotIndex * (TimeWindow / 60);;
        // 如果 newBaseMinute 超过 59 分钟，需要进位到小时
        int finalHour = baseHour + newBaseMinute / 60;
        newBaseMinute = newBaseMinute % 60;
        // 格式化输出到分钟
        return String.format("%d年%02d月%02d日%02d点%02d分", year, month, day, finalHour, newBaseMinute);
    }
}
// (TimeWindow / 60);


