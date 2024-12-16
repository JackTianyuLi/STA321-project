package mapper;

import driver.StockDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Map_Only extends Mapper<LongWritable, Text, Text, Text> {
    private List<String> currentTimeWindowKey = new ArrayList<>();
    private HashMap<String, String> buyMap = new HashMap<>(); // 买单记录
    private HashMap<String, String> sellMap = new HashMap<>(); // 卖单记录
    static long total_in = 0; // main inflow
   static long total_out = 0; // main outflow
    static long super_in = 0; // super-large purchased quantity
     static long super_in_price = 0; // super-large purchased amount
    static long super_out = 0; // super-large sold quantity
    static long super_out_price = 0; // super-large sold amount
    static long large_in = 0; // large purchased quantity
    static long large_in_price = 0; // large purchased amount
    static long large_out = 0; // large sold quantity
    static long large_out_price = 0; // large sold amount
    static long medium_in = 0; // medium purchased quantity
    static  long medium_in_price = 0; // medium purchased amount
    static  long medium_out = 0; // medium sold quantity
    static  long medium_out_price = 0; // medium sold amount
    static  long small_in = 0; // small purchased quantity
    static long small_in_price = 0; // small purchased amount
    static    long small_out = 0; // small sold quantity
    static    long small_out_price = 0; // small sold amount
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
            String[] fields = value.toString().trim().split("\\s+|\\t+");
            String SecurityID = fields[8];
            String ExecType = fields[14];
            if (SecurityID.equals(filter) && ExecType.equals("F")) {
                String ApplSeqNum = fields[7];
                String BidApplSeqNum = fields[10];
                String OfferApplSeqNum = fields[11];
                String Price = fields[12];
                String TradeQty = fields[13];
                String tradeTime = fields[15];
                float price = Float.parseFloat(Price); // price
                int tradeQty = Integer.parseInt(TradeQty); // quantity
                // 创建以时间窗口为键的 key
                String timeWindowKey = createTimeKey(tradeTime);
                currentTimeWindowKey.add(timeWindowKey);
                if (!timeWindowKey.equals(currentTimeWindowKey.get(currentTimeWindowKey.size() - 1))) {
                    for (String bidKey : buyMap.keySet()) { // iterate over every proactively purchased order
                        String data = buyMap.get(bidKey);
                        String[] parts = data.split(" ");
                        int totalTradeQty = Integer.parseInt(parts[0]); // merged trade quantity
                        float totalPrice = Float.parseFloat(parts[1]); // merged trade price
                        String orderType = classifyOrder(totalTradeQty, totalPrice); // classify according to quantity and price
                        if (orderType.equals("extra-large")) { // extra-large purchased order
                            super_in += totalTradeQty;
                            super_in_price += totalPrice;
                        } else if (orderType.equals("large")) { // large purchased order
                            large_in += totalTradeQty;
                            large_in_price += totalPrice;
                        } else if (orderType.equals("medium")) { // medium purchased order
                            medium_in += totalTradeQty;
                            medium_in_price += totalPrice;
                        } else if (orderType.equals("small")) { // small purchased order
                            small_in += totalTradeQty;
                            small_in_price += totalPrice;
                        }
                        context.write(new Text(key.toString()), new Text(bidKey + " " + totalTradeQty + " " + totalPrice + " " + orderType));
                    }
                    total_in = super_in_price + large_in_price;

                    for (String sellKey : sellMap.keySet()) { // iterate over every proactively sold order
                        String data = sellMap.get(sellKey);
                        String[] parts = data.split(" ");
                        int totalTradeQty = Integer.parseInt(parts[0]); // merged trade quantity
                        float totalPrice = Float.parseFloat(parts[1]); // merged trade price
                        String orderType = classifyOrder(totalTradeQty, totalPrice); // classify according to quantity and price
                        if (orderType.equals("extra-large")) { // extra-large sold order
                            super_out += totalTradeQty;
                            super_out_price += totalPrice;
                        } else if (orderType.equals("large")) { // large sold order
                            large_out += totalTradeQty;
                            large_out_price += totalPrice;
                        } else if (orderType.equals("medium")) { // medium sold order
                            medium_out += totalTradeQty;
                            medium_out_price += totalPrice;
                        } else if (orderType.equals("small")) { // small sold order
                            small_out += totalTradeQty;
                            small_out_price += totalPrice;
                        }
                        context.write(new Text(key.toString()), new Text(sellKey + " " + totalTradeQty + " " + totalPrice + " " + orderType));
                    }
                    total_out = super_out_price + large_out_price;
                    context.write(new Text(currentTimeWindowKey.get(currentTimeWindowKey.size() - 1)), new Text((total_in - total_out) + "," + total_in + "," + total_out + ","
                            + super_in + "," + super_in_price + "," + super_out + "," + super_out_price + ","
                            + large_in + "," + large_in_price + "," + large_out + "," + large_out_price + ","
                            + medium_in + "," + medium_in_price + "," + medium_out + "," + medium_out_price + ","
                            + small_in + "," + small_in_price + "," + small_out + "," + small_out_price));
                    buyMap.clear(); // clear buyMap
                    sellMap.clear(); // clear sellMap
                    total_in = 0; // main inflow
                    total_out = 0; // main outflow
                    super_in = 0; // super-large purchased quantity
                    super_in_price = 0; // super-large purchased amount
                    super_out = 0; // super-large sold quantity
                    super_out_price = 0; // super-large sold amount
                    large_in = 0; // large purchased quantity
                    large_in_price = 0; // large purchased amount
                    large_out = 0; // large sold quantity
                    large_out_price = 0; // large sold amount
                    medium_in = 0; // medium purchased quantity
                    medium_in_price = 0; // medium purchased amount
                    medium_out = 0; // medium sold quantity
                    medium_out_price = 0; // medium sold amount
                    small_in = 0; // small purchased quantity
                    small_in_price = 0; // small purchased amount
                    small_out = 0; // small sold quantity
                    small_out_price = 0; // small sold amount

                }

                Long bidTransactTime = Long.parseLong(BidApplSeqNum);//purchase time
                Long offerTransactTime = Long.parseLong(OfferApplSeqNum);//sell time

                String direction = (bidTransactTime > offerTransactTime) ? "BUY" : "SELL";// set trade direction
                if (direction.equals("BUY")) { // proactive purchased order
                    // merge order with a same purchase order ID
                    if (buyMap.containsKey(BidApplSeqNum)) {
                        String data[] = buyMap.get(BidApplSeqNum).split(" ");


                        // get previous trade price and quantity
                        int previousTradeQty = Integer.parseInt(data[0]);
                        float previousPrice = Float.parseFloat(data[1]);

                        // update trade price and quantity
                        int newTradeQty = previousTradeQty + tradeQty;
                        float newPrice = previousPrice + price * tradeQty;

                        // store in a HashMap
                        buyMap.put(BidApplSeqNum, newTradeQty + " " + newPrice);
                    } else { // order with distinct purchase order ID
                        String data = tradeQty + " " + price * tradeQty;
                        buyMap.put(BidApplSeqNum, data); // store in a HashMap
                    }

                }
                if (direction.equals("SELL")) {  // proactive sold order
                    // merge order with a same sell order ID
                    if (sellMap.containsKey(OfferApplSeqNum)) {
                        String data[] = sellMap.get(OfferApplSeqNum).split(" ");

                        // get previous trade price and quantity
                        int previousTradeQty = Integer.parseInt(data[0]);
                        float previousPrice = Float.parseFloat(data[1]);

                        // update trade price and quantity
                        int newTradeQty = previousTradeQty + tradeQty;
                        float newPrice = previousPrice + price * tradeQty;

                        // store in a HashMap
                        sellMap.put(OfferApplSeqNum, newTradeQty + " " + newPrice);
                    } else { // order with distinct sell order ID
                        String data = tradeQty + " " + price * tradeQty;
                        sellMap.put(OfferApplSeqNum, data);  // store in a HashMap
                    }
                }


            }
            } catch(Exception e){
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
        int baseHour = 9;
        int baseMinute = 30;

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
    private String classifyOrder(int tradeQty, float price) {// classify orders
        if (tradeQty >= 200000 || price >= 1000000 || ((double) tradeQty * 100 / 17170245800L) >= 0.3) {
            return "extra-large"; // extra-large order
        } else if (tradeQty >= 60000 || price >= 300000 || ((double) tradeQty * 100 / 17170245800L) >= 0.1) {
            return "large"; // large order
        } else if (tradeQty >= 10000 || price >= 50000 || ((double) tradeQty * 100) / 17170245800L >= 0.017) {
            return "medium"; // medium order
        } else if (price < 50000 && (double) tradeQty * 100 / 17170245800L < 0.017) {
            return "small"; // small order
        }
        return "unknown"; // unknown order type
    }

}
