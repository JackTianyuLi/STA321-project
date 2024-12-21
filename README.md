## overall technical scheme

## project structure
```
src
└── main
└── java
├── driver
├── mapper
├── reducer
├── webApp

```

Directory Breakdown


**driver**

This package is responsible for managing the Hadoop job configurations. 
The driver class initializes the MapReduce job, sets up input/output formats, and invokes the mapper and reducer classes. 
It orchestrates the execution of the entire job.

**mapper**

This package contains classes related to the Map phase of the MapReduce framework.
Mapper classes implement the logic to process input data, transform it into key-value pairs, and emit intermediate results.
Each mapper corresponds to a portion of the dataset being processed.

**reducer**

This package includes classes responsible for the Reduce phase of the MapReduce process.
Reducer classes aggregate and process intermediate key-value pairs emitted by the mappers. 
They execute the logic to summarize or aggregate data, producing the final output.



**webApp**

This package is used to visualize our output


## 3.2 Detailed design

### 3.2.1 Driver

**Job Configuration:**

The class initializes a Hadoop Job instance named "JoinExample".
It sets the framework name to "local" for local execution, which is useful for testing and debugging.

**Parameters and Path Setup:**

The class defines a specific stock identifier (SecurityIDQueried) and a time window (Mapper2.TimeWindow).
It specifies input paths for trade data and an output path where results will be stored.
Multiple Input Paths:******

The MultipleInputs.addInputPath method is used to specify multiple input sources, allowing the job to handle various data files—in this case, trade data processed by the Mapper2 class.

**Reducer Configuration:**

The StockReducer class is set as the reducer for the job, which will process the key-value pairs outputted by the mappers.

**Output Format Configuration:**

The output key-value types are set to Text, allowing for structured textual outputs.

**Job Submission:**

The job is submitted for execution, with the program waiting for its completion. In the event of an error, it prints the stack trace and exits with a non-zero status.

### 3.2.2 Mapper

**Static Parameters:**

The class defines two static variables: TimeWindow and filter, which are used to filter trade data based on specified criteria. 
These parameters are set externally, typically by the driver class (StockDriver).

**Map Method:**

The map method processes each record of trade data:
It splits the input text into fields based on whitespace or tab characters to extract relevant information.
It retrieves the SecurityID (from the trade) and the ExecType (execution type), filtering for completed trades (ExecType equals "F") for the specified security ID.
It extracts additional fields such as trade ID, purchase order ID, sell order ID, price, trade quantity, and trade time.
A time window key is generated using the createTimeKey method, which converts the trade time into a formatted string reflecting the appropriate time window.

**Context Output:**

If the generated timeWindowKey is valid, the method writes the key-value pair to the context,
associating the time window with trade details (trade ID, order IDs, price, and quantity). 


**Time Key Generation**

Key Method: createTimeKey(String tradeTime)

```
private String createTimeKey(String tradeTime) {
// Method implementation
}
```


**Description**

This method converts the input trade time into a time window format, which allows for time-based aggregation of trade data. The implementation includes:
Parsing the trade time to extract year, month, day, hour, and minute.
Setting a base time of 9:30 AM, ensuring that inputs before this time return null.
Calculating the appropriate time slot index based on the TimeWindow.
Generating formatted strings representing the start and end of the time window.

**Output Format**

The method returns the time window as a string in the format:

yyyyMMddHHmmSSsss to yyyyMMddHHmmSSsss

### 3.2.3 Reducer

**Reduce Method:**

The main method in this class, reduce, processes each unique key (representing a time window) and its associated values (trade records).
Data Structures:
Two HashMaps, buyMap and sellMap, are used to store proactive purchase and sale order records respectively.

**Processing Trade Records:**

The method iterates through each trade record, splitting the string data into relevant fields—such as order ID, price, trade quantity, and transaction times.
It determines the direction of the trade (BUY or SELL) based on transaction times and merges order records with the same IDs to aggregate data.

**Calculating Totals:**

The method calculates total inflows and outflows by iterating through both buyMap and sellMap.
It categorizes trade orders into different classes (extra-large, large, medium, and small) based on predefined criteria using the classifyOrder method.

**Formatting Output:**

The final calculation results include the main net inflow, main inflow, main outflow, and categorized trade quantities and amounts.
The results are formatted using the formatValue method, which ensures a consistent numerical output format.


**Supporting Methods**

1. formatValue(double value)
```
   public String formatValue(double value) {
   DecimalFormat df = new DecimalFormat("#.00");
   if (value == Math.floor(value) && !Double.isInfinite(value)) {
   return String.valueOf((int)value); // return as integer if applicable
   }
   return df.format(value); // return formatted string for float
   }
```

   This method formats numerical values to two decimal places for float representations and returns integers as whole numbers when appropriate.



2. classifyOrder(int tradeQty, float price)
```
   private String classifyOrder(int tradeQty, float price) {
   // Order classification logic
   }
```

   The method categorizes orders based on their quantity and price, returning labels such as "extra-large," "large," "medium," "small," or "unknown."