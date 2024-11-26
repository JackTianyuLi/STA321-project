package mapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class StockMapper extends Mapper<LongWritable, Text, Text, Text> {
    //define a hashmap between ApplNumSeq and TransactionTime
    private HashMap<Integer, String> OrderMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException{
        //create path for cache file (order data)
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //acquire system file and open an input stream for it
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //convert to reader for read line by line
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        String line;
        //write to the hashmap between ApplNumSeq and TransactionTime
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] split = line.split("\t"); // split data lines
            String ApplSeqNum = split[7].trim(); // Order sequence number
            String TransactTime = split[12].trim(); // Transaction time
            OrderMap.put(Integer.parseInt(ApplSeqNum), TransactTime);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // load and process trade data
        String[] fields = value.toString().split("\t");
        String BidApplSeqNum = fields[10];
        String BidTransactTime = OrderMap.get(Integer.parseInt(BidApplSeqNum));
        String OfferApplSeqNum = fields[11];
        String OfferTransactTime = OrderMap.get(Integer.parseInt(OfferApplSeqNum));
        String Price = fields[12];
        String TradeQty = fields[13];
        String ExecType = fields[14];
        String tradeTime = fields[15];
        if (ExecType.equals("F")) {// select successfully executed trades
            if (Integer.parseInt(BidTransactTime) > Integer.parseInt(OfferTransactTime)) {
                context.write(new Text(BidApplSeqNum),
                        new Text(Price + " " + TradeQty + " " + tradeTime));// Proactively purchased order
            } else {
                context.write(new Text(OfferApplSeqNum),
                        new Text(Price + " " + TradeQty + " " + tradeTime));// Proactively sold order
            }
        }
    }
}
