package mapper;

import driver.StockDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.List;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    public static Integer TimeWindow;
    public static String filter;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
                    if(timeWindowKey!= null &&!timeWindowKey.contains("null") ){
                        context.write(new Text(timeWindowKey),//set key according to time window
                                new Text(ApplSeqNum + " " + BidApplSeqNum + " " + OfferApplSeqNum + " " + Price + " " + TradeQty));
                    }

                    // return all trade data in the given period of time
                }

    }
//    private static String createTimeKey(String tradeTime) {
//        // 解析输入时间
//        int hour = Integer.parseInt(tradeTime.substring(8, 10));
//        int minute = Integer.parseInt(tradeTime.substring(10, 12));
//
//        // 获取输入时间的字符串表示
//        String inputTradeTime = String.format("%04d%02d%02d%02d%02d00000",
//                Integer.parseInt(tradeTime.substring(0, 4)),
//                Integer.parseInt(tradeTime.substring(4, 6)),
//                Integer.parseInt(tradeTime.substring(6, 8)),
//                hour,
//                minute);
//
//        // 遍历时间窗口比较
//        String previousSlot = null; // 用于存储上一个时间点
//        for (String slot : timeSlots) {
//            // 比较输入时间和时间窗口的时间
//            if (inputTradeTime.compareTo(slot) < 0) {
//                // 如果找到第一个大于等于输入时间的时间点，返回上一个时间点
//                return previousSlot+" to "+slot;
//            }
//
//            // 更新上一个时间点
//            previousSlot = slot;
//
//        }
//        // 如果没有找到合适的时间点，返回最早的时间点或 null
//        return previousSlot+" to "+"20190102150000000"; // 返回最后一个有效的时间点
//    }
//



    private String createTimeKey(String tradeTime) { // convert input time to the start of a time window
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
        if (hour == 15) {
            hour = 14;
            minute = 59;
        }
        if (hour < 9 || (hour == 9 && minute < 30)) {
            return null; // 早于9:30返回null
        }

        if (hour <= 12) {
            baseHour = 9;
            baseMinute = 30;
        } else {
            baseHour = 13;
            baseMinute = 0;
        }

        // convert input time to total minutes
        int inputTotalMinutes = (hour * 60 + minute);
        int baseTotalMinutes = (baseHour * 60 + baseMinute);

        // calculate time slot index
        int timeSlotIndex = (inputTotalMinutes - baseTotalMinutes) / (TimeWindow / 60);

        // calculate current and next time slots
        int startMinutes = baseTotalMinutes + timeSlotIndex * (TimeWindow / 60);
        int endMinutes = startMinutes + (TimeWindow / 60);

        // converting back to hours and minutes
        int finalStartHour = startMinutes / 60;
        int finalStartMinute = startMinutes % 60;

        int finalEndHour = endMinutes / 60;
        int finalEndMinute = endMinutes % 60;

        if(finalEndHour >= 15){
            finalEndHour = 15;
            finalEndMinute = 0;
        }
        // output in the desired format
        return String.format("%d%02d%02d%02d%02d00000 to %d%02d%02d%02d%02d00000",
                year, month, day, finalStartHour, finalStartMinute,
                year, month, day, finalEndHour, finalEndMinute);
    }

}
