package reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

// TO DO: calculation of data for each time window
public class StockReducer extends Reducer<Text, Text, Text, Text> {
    // HashMap for ApplSeqNum and TransactTime (order)
    private HashMap<String, Long> orderMap = new HashMap<>();
    // Table for storing filtered trade data
    private ArrayList<String> tradeTable = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String s = value.toString();
            // extract table source
            char tableId = s.charAt(s.length() - 1);
            // extract other information
            String fields = s.substring(0, s.length() - 2);
            // check source of record (order/trade)
            if (tableId == '0'){ // order
                Long TransactTime = Long.getLong(fields.split(" ")[0]);
                orderMap.put(key.toString(), TransactTime);
            } else { // trade
                tradeTable.add(fields);
            }
        }
        for (String field : tradeTable) {
            String[] fields = field.split(" ");
            String ApplSeqNum = fields[0];  // trade ID
            String BidApplSeqNum = fields[1];  // Purchase order ID
            String OfferApplSeqNum = fields[2]; // Sell order ID
            String Price = fields[3];
            String TradeQty = fields[4]; // trade quantity
            Long BidTransactTime = orderMap.get(BidApplSeqNum);
            Long OfferTransactTime = orderMap.get(OfferApplSeqNum);
            String direction; // 判断并记录主动单方向，后续可以调整
            if (BidTransactTime > OfferTransactTime){
                direction = "BUY";
            } else {
                direction = "SELL";
            }
            field = field.concat(direction);
        }

        // TODO: 剩余所有其他数据计算
    }
}
