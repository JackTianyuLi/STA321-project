package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

// TO DO: calculation of data for each time window
public class StockReducer extends Reducer<Text, Text, Text, Text> {
    private HashMap<String, Long> orderMap = new HashMap<>();
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
        // TODO: 查询买卖委托时间，确定主动单方向
        // TODO: 剩余所有其他数据计算
    }
}
