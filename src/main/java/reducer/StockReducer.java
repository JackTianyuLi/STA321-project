package reducer;

import driver.StockDriver;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
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
            String applSeqNum = fields[0]; // trade ID
            String bidApplSeqNum = fields[1]; // purchase order ID
            String offerApplSeqNum = fields[2]; // sell order ID
            float price = Float.parseFloat(fields[3]); // price
            int tradeQty = Integer.parseInt(fields[4]); // quantity
            Long bidTransactTime = StockDriver.orderMap.get(bidApplSeqNum);//purchase time
            Long offerTransactTime = StockDriver.orderMap.get(offerApplSeqNum);//sell time

            if (bidTransactTime != null && offerTransactTime != null) {
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
        }

        // sum up the data
        long total_in = 0; // main inflow
        long total_out = 0; // main outflow
        long super_in = 0; // super-large purchased quantity
        long super_in_price = 0; // super-large purchased amount
        long super_out = 0; // super-large sold quantity
        long super_out_price = 0; // super-large sold amount
        long large_in = 0; // large purchased quantity
        long large_in_price = 0; // large purchased amount
        long large_out = 0; // large sold quantity
        long large_out_price = 0; // large sold amount
        long medium_in = 0; // medium purchased quantity
        long medium_in_price = 0; // medium purchased amount
        long medium_out = 0; // medium sold quantity
        long medium_out_price = 0; // medium sold amount
        long small_in = 0; // small purchased quantity
        long small_in_price = 0; // small purchased amount
        long small_out = 0; // small sold quantity
        long small_out_price = 0; // small sold amount

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
//        context.write(new Text(key.toString()), new Text("\n主力流入: " + total_in + " \n超大买单量: " + super_in + "," + "\n超大买单额: " + super_in_price
//                + "\n大买单量: " + large_in + "," + "\n大买单额: " + large_in_price + "\n 中买单量: " + medium_in + ","
//                + "\n中买单额: " + medium_in + "," + "\n小买单量: " + small_in + "," + "\n小买单额:" + small_in_price));

        // output: time window, main net inflow, main outflow, extra-large sold order quantity, extra-large sold order amount,
        // large sold order quantity, large sold order amount, medium sold order quantity, medium sold order amount,
        // small sold order quantity, small sold order amount
//        context.write(new Text(key.toString()), new Text("\n主力净流入：" + (total_in - total_out) + "\n主力流出: " + total_out + "\n超大卖单量: " + super_out + "," + "\n超大卖单额: " +
//                super_out_price + "," + "\n大卖单量: " + large_out + "," + "\n大卖单额: " + large_out_price + "," + "\n中卖单量: " + medium_out + ","
//                + "\n中卖单额: " + medium_out_price + "," + "\n小卖单量: " + small_out + "," + "\n小卖单额: " + small_out_price));

        // output: time window, main net inflow, main inflow, main outflow, extra-large purchased order quantity,
        // extra-large purchased order amount, extra-large sold order quantity, extra-large sold order amount,
        // large purchased order quantity, large purchased order amount, large sold order quantity, large sold order amount,
        // medium purchased order quantity, medium purchased order amount, medium sold order quantity, medium sold order amount,
        // small purchased order quantity, small purchased order amount, small sold order quantity, small sold order amount

        context.write(new Text(key.toString()), new Text((total_in - total_out) + "," + total_in + "," + total_out + ","
                + super_in + "," + super_in_price + "," + super_out + "," +  super_out_price + ","
                + large_in + "," + large_in_price + "," + large_out + "," +  large_out_price + ","
                + medium_in + "," + medium_in_price + "," + medium_out + "," + medium_out_price + ","
                + small_in + "," + small_in_price + "," + small_out + ","  + small_out_price));
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
