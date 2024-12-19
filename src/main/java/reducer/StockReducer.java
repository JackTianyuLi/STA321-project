package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

public class StockReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, String> buyMap = new HashMap<>(); // proactively purchased order records
        HashMap<String, String> sellMap = new HashMap<>(); // proactively sold order records

        // iterate over every trade record
        for (Text value : values) {
            String s = value.toString();
            String[] fields = s.split(" ");
            String bidApplSeqNum = fields[1]; // purchase order ID
            String offerApplSeqNum = fields[2]; // sell order ID
            float price = Float.parseFloat(fields[3]); // price
            int tradeQty = Integer.parseInt(fields[4]); // quantity
            double bidTransactTime = Double.parseDouble(bidApplSeqNum) ;//purchase time
            double offerTransactTime = Double.parseDouble(offerApplSeqNum) ;//sell time

                String direction = (bidTransactTime > offerTransactTime) ? "BUY" : "SELL";// set trade direction
               // merge orders with same IDs
                if (direction.equals("BUY")) { // proactive purchased order
                    // merge order with a same purchase order ID
                    if (buyMap.containsKey(bidApplSeqNum)) {
                        String data[] = buyMap.get(bidApplSeqNum).split(" ");

                        // get previous trade price and quantity
                        int previousTradeQty = Integer.parseInt(data[0]);
                        float previousPrice = Float.parseFloat(data[1]);

                        // update trade price and quantity
                        int newTradeQty = previousTradeQty + tradeQty;
                        float newPrice = previousPrice + price * tradeQty;

                        // store in a HashMap
                        buyMap.put(bidApplSeqNum, newTradeQty + " " + newPrice);
                    } else { // order with distinct purchase order ID
                        String data = tradeQty + " " + price * tradeQty;
                        buyMap.put(bidApplSeqNum, data); // store in a HashMap
                    }

                }
                if (direction.equals("SELL")) {  // proactive sold order
                    // merge order with a same sell order ID
                    if (sellMap.containsKey(offerApplSeqNum)) {
                        String data[] = sellMap.get(offerApplSeqNum).split(" ");

                        // get previous trade price and quantity
                        int previousTradeQty = Integer.parseInt(data[0]);
                        float previousPrice = Float.parseFloat(data[1]);

                        // update trade price and quantity
                        int newTradeQty = previousTradeQty + tradeQty;
                        float newPrice = previousPrice + price * tradeQty;

                        // store in a HashMap
                        sellMap.put(offerApplSeqNum, newTradeQty + " " + newPrice);
                    } else { // order with distinct sell order ID
                        String data = tradeQty + " " + price * tradeQty;
                        sellMap.put(offerApplSeqNum, data);  // store in a HashMap
                    }
                }
        }

        // sum up the data
        double total_in = 0; // main inflow
        double total_out = 0; // main outflow
        double super_in = 0; // super-large purchased quantity
        double super_in_price = 0; // super-large purchased amount
        double super_out = 0; // super-large sold quantity
        double super_out_price = 0; // super-large sold amount
        double large_in = 0; // large purchased quantity
        double large_in_price = 0; // large purchased amount
        double large_out = 0; // large sold quantity
        double large_out_price = 0; // large sold amount
        double medium_in = 0; // medium purchased quantity
        double medium_in_price = 0; // medium purchased amount
        double medium_out = 0; // medium sold quantity
        double medium_out_price = 0; // medium sold amount
        double small_in = 0; // small purchased quantity
        double small_in_price = 0; // small purchased amount
        double small_out = 0; // small sold quantity
        double small_out_price = 0; // small sold amount

        //calculation after merging data
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
        }
        total_out = super_out_price + large_out_price;

        // output: time window, main inflow, extra-large purchased order quantity, extra-large purchased order amount,
        // large purchased order quantity, large purchased order amount, medium purchased order quantity, medium purchased order amount,
        // small purchased order quantity, small purchased order amount
        // output: time window, main net inflow, main outflow, extra-large sold order quantity, extra-large sold order amount,
        // large sold order quantity, large sold order amount, medium sold order quantity, medium sold order amount,
        // small sold order quantity, small sold order amount
        // output: time window, main net inflow, main inflow, main outflow, extra-large purchased order quantity,
        // extra-large purchased order amount, extra-large sold order quantity, extra-large sold order amount,
        // large purchased order quantity, large purchased order amount, large sold order quantity, large sold order amount,
        // medium purchased order quantity, medium purchased order amount, medium sold order quantity, medium sold order amount,
        // small purchased order quantity, small purchased order amount, small sold order quantity, small sold order amount

// 创建一个 DecimalFormat 实例



// 在输出结果时格式化每个数值

        // create a DecimalFormat instance
        // convert to format when output

        context.write(null, new Text(
                formatValue(total_in - total_out) + "," +
                        formatValue(total_in) + "," +
                        formatValue(total_out) + "," +
                        formatValue(super_in) + "," +
                        formatValue(super_in_price) + "," +
                        formatValue(super_out) + "," +
                        formatValue(super_out_price) + "," +
                        formatValue(large_in) + "," +
                        formatValue(large_in_price) + "," +
                        formatValue(large_out) + "," +
                        formatValue(large_out_price) + "," +
                        formatValue(medium_in) + "," +
                        formatValue(medium_in_price) + "," +
                        formatValue(medium_out) + "," +
                        formatValue(medium_out_price) + "," +
                        formatValue(small_in) + "," +
                        formatValue(small_in_price) + "," +
                        formatValue(small_out) + "," +
                        formatValue(small_out_price)+","+
                        key.toString()
        ));




    }

    public String formatValue(double value) {
        DecimalFormat df = new DecimalFormat("#.00");
        // integer or 0
        if (value == Math.floor(value) && !Double.isInfinite(value)) {
            return String.valueOf((int)value); // integer
        }
        return df.format(value); // float

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
