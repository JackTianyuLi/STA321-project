package reducer;

import driver.StockDriver;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class StockReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, String> buyMap = new HashMap<>();
        HashMap<String, String> sellMap = new HashMap<>();
        for (Text value : values) {
            String s = value.toString();
            String[] fields = s.split(" ");
            String applSeqNum = fields[0]; // trade ID
            String bidApplSeqNum = fields[1]; // 购买订单 ID
            String offerApplSeqNum = fields[2]; // 卖出订单 ID
            float price = Float.parseFloat(fields[3]); // 价格
            int tradeQty = Integer.parseInt(fields[4]); // 成交数量
            Long bidTransactTime = StockDriver.orderMap.get(bidApplSeqNum);//得到买方时间
            Long offerTransactTime = StockDriver.orderMap.get(offerApplSeqNum);//得到卖方时间

            if (bidTransactTime != null && offerTransactTime != null) {
                String direction = (bidTransactTime > offerTransactTime) ? "BUY" : "SELL";// 确定主动买卖方向

                if (direction.equals("BUY")) { // 对于所有主动买单来说
                    if (buyMap.containsKey(bidApplSeqNum)) { // 合并同一个主动买方委托单号的成交
                        String data[] = buyMap.get(bidApplSeqNum).split(" ");

                        // 提取以前的数量和价格
                        int previousTradeQty = Integer.parseInt(data[0]);
                        float previousPrice = Float.parseFloat(data[1]);

                        // 更新累加的数量和价格
                        int newTradeQty = previousTradeQty + tradeQty; // 更新数量
                        float newPrice = previousPrice + price * tradeQty; // 更新价格

                        // 将新的数量和价格存回 HashMap
                        buyMap.put(bidApplSeqNum, newTradeQty + " " + newPrice);

                    } else {
                        String data = tradeQty + " " + price * tradeQty; // 如果没有，则新建

                        buyMap.put(bidApplSeqNum, data);
                    }
                }
                if (direction.equals("SELL")) {  // 对于所有主动卖单来说
                    if (sellMap.containsKey(offerApplSeqNum)) { // 合并同一个主动卖方委托单号的成交
                        String data[] = sellMap.get(offerApplSeqNum).split(" ");

                        // 提取以前的数量和价格
                        int previousTradeQty = Integer.parseInt(data[0]);
                        float previousPrice = Float.parseFloat(data[1]);

                        // 更新累加的数量和价格
                        int newTradeQty = previousTradeQty + tradeQty; // 更新数量
                        float newPrice = previousPrice + price * tradeQty; // 更新价格（如果您需要以某种方式更新）

                        // 将新的数量和价格存回 HashMap
                        sellMap.put(offerApplSeqNum, newTradeQty + " " + newPrice);

                    } else {
                        String data = tradeQty + " " + price * tradeQty; // 如果没有，则新建
                        sellMap.put(offerApplSeqNum, data);
                    }

                }
            }
        }


        // 数据累加
        long total_in = 0; // 主力流入
        long total_out = 0; // 主力流出
        long super_in = 0; // 超大单买单量
        long super_in_price = 0; // 超大单买入额
        long super_out = 0; // 超大买单量
        long super_out_price = 0; // 超大单卖出额
        long large_in = 0; // 大单买入量
        long large_in_price = 0; // 大单买入额
        long large_out = 0; // 大单卖出量
        long large_out_price = 0; // 大单卖出额
        long medium_in = 0; // 中单买入量
        long medium_in_price = 0; // 中单买入额
        long medium_out = 0; // 中单卖出量
        long medium_out_price = 0; // 中单卖出额
        long small_in = 0; // 小单买入量
        long small_in_price = 0; // 小单买入额
        long small_out = 0; // 小单卖出量
        long small_out_price = 0; // 小单卖出额

        //遍历完了成交数据，并且把他们都合并了，现在到了计算环节
        for (String bidKey : buyMap.keySet()) {//遍历每一个主动买单的成交数据
            String data = buyMap.get(bidKey);
            String[] parts = data.split(" ");
            int totalTradeQty = Integer.parseInt(parts[0]); // 获取合并后的成交数量
            float totalPrice = Float.parseFloat(parts[1]); // 获取合并后的成交价格
            String orderType = classifyOrder(totalTradeQty, totalPrice); // 进行分类
            if (orderType.equals("extra-large")) { // 超大单
                super_in += totalTradeQty;
                super_in_price += totalPrice;
            } else if (orderType.equals("large")) { // 大单
                large_in += totalTradeQty;
                large_in_price += totalPrice;
            } else if (orderType.equals("medium")) { // 中单
                medium_in += totalTradeQty;
                medium_in_price += totalPrice;
            } else if (orderType.equals("small")) { // 小单
                small_in += totalTradeQty;
                small_in_price += totalPrice;
            }
        }
        total_in = super_in_price + large_in_price;

        context.write(new Text(key.toString()), new Text("\n主力流入: " + total_in + " \n超大买单量: " + super_in + "," + "\n超大买单额: " + super_in_price
                + "\n大买单量: " + large_in + "," + "\n大买单额: " + large_in_price + "\n 中买单量: " + medium_in + ","
                + "\n中买单额: " + medium_in + "," + "\n小买单量: " + small_in + "," + "\n小买单额:" + small_in_price));
        for (String sellKey : sellMap.keySet()) {
            String data = sellMap.get(sellKey);
            String[] parts = data.split(" ");
            int totalTradeQty = Integer.parseInt(parts[0]); // 获取合并后的成交数量
            float totalPrice = Float.parseFloat(parts[1]); // 获取合并后的成交价格
            String orderType = classifyOrder(totalTradeQty, totalPrice); // 进行分类
            if (orderType.equals("extra-large")) { // 超大单
                super_out += totalTradeQty;
                super_out_price += totalPrice;
            } else if (orderType.equals("large")) { // 大单
                large_out += totalTradeQty;
                large_out_price += totalPrice;
            } else if (orderType.equals("medium")) { // 中单
                medium_out += totalTradeQty;
                medium_out_price += totalPrice;
            } else if (orderType.equals("small")) { // 小单
                small_out += totalTradeQty;
                small_out_price += totalPrice;
            }
        }
        total_out = super_out_price + large_out_price;
        context.write(new Text(key.toString()), new Text("\n主力净流入：" + (total_in - total_out) + "\n主力流出: " + total_out + "\n超大卖单量: " + super_out + "," + "\n超大卖单额: " +
                super_out_price + "," + "\n大卖单量: " + large_out + "," + "\n大卖单额: " + large_out_price + "," + "\n中卖单量: " + medium_out + ","
                + "\n中卖单额: " + medium_out_price + "," + "\n小卖单量: " + small_out + "," + "\n小卖单额: " + small_out_price));
    }

    private String classifyOrder(int tradeQty, float price) {// 分类订单类型
        if (tradeQty >= 200000 || price >= 1000000 || ((double) tradeQty * 100 / 17170245800L) >= 0.3) {
            return "extra-large"; // 超大单
        } else if (tradeQty >= 60000 || price >= 300000 || ((double) tradeQty * 100 / 17170245800L) >= 0.1) {
            return "large"; // 大单
        } else if (tradeQty >= 10000 || price >= 50000 || ((double) tradeQty * 100) / 17170245800L >= 0.017) {
            return "medium"; // 中单
        } else if (price < 50000 && (double) tradeQty * 100 / 17170245800L < 0.017) {
            return "small"; // 小单
        }
        return "unknown"; // 未知类型
    }
}
