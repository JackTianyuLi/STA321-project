# STA321-project
SUSTech 2024 Fall STA321 Distributed Stroarge and Parallel Computing Project

Score: 38.5/40 

Project details and requirements：https://github.com/JackTianyuLi/STA321-project/blob/master/project-v1.pdf

## Project description

1. Introduction：

   Basic requirement:

   In the stock market, it is a crucial task to identify the main flow of capital with real-time order and trade data to help investors make decisions. However, the order and trade data are often of so large volume that the computation flow and program of main capital flow should be carefully designed in order to fulfill such demand. 

   In this project, we design a mapReduce parallel computing program that can run on Hadoop, thus not only the need to compute the main capital flow is met, but also volumes of different categories of orders are derived, and a visualization page is provided to demonstrate the results more intuitively.

   Implementation: 

   We uses **JAVA** for this project.

3. Project structure

   ```
./data # too large for upload on repository, if needed please contact the repository owner   
./src 
├── ./resources
└── ./java
    ├── ./driver
    │   └──./driver/StockDriver.java
    ├── ./mapper
    │   └──./mapper/Mapper2.java
    ├── ./reducer
    │   └──./reducer/StockReducer.java
    └── ./webApp
        ├── ./css
        │   └──./css/style.css // style of the webpage display
        ├── ./js
        │   └──./js/chart.js   // imported template package
        ├──./app.js            // read and import result data
        ├──./index.html        // setting the layout of the webpage
        └──./output.csv
   ```

3. Implemented functions

   - Compute the main capital flow is met and volumes of different categories of orders witg mapReduce.

     Mapper: implement the logic to process input data, transform it into key(time window)-value(trade record) pairs, and emit intermediate results.

     Reducer:  aggregate and process intermediate key-value pairs emitted by the mappers, derive the desired results with the rules of calculation and categorizing. 

   - The `chart.js` package is used to visualize our computing results, and it would fetch the derived results every 10s for real-time display.

4. To be improved
    
   - More configurations on the execution of map jobs can be done to improve speed.

   - The design and the real-time display mode of our data visualization web page can be refined.

   - ...

## Project report

https://github.com/JackTianyuLi/STA321-project/blob/master/report/report.pdf
