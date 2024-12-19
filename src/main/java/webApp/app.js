let chart; // save chart instance

// parse file content
function parseFileContent(content){
    const lines = content.trim().split('\n');
    const timeLabels = [];
    const mainNetInflow = [];
    const mainInflow = [];
    const mainOutflow = [];
    const extraLargeBuyVolume = [];
    const extraLargeBuyPrice = [];
    const extraLargeSellVolume = [];
    const extraLargeSellPrice = [];
    const largeBuyVolume = [];
    const largeBuyPrice = [];
    const largeSellVolume = [];
    const largeSellPrice = [];
    const mediumBuyVolume = [];
    const mediumBuyPrice = [];
    const mediumSellVolume = [];
    const mediumSellPrice = [];
    const smallBuyVolume = [];
    const smallBuyPrice = [];
    const smallSellVolume = [];
    const smallSellPrice = [];

    lines.forEach(line => {
        const parts = line.split("\t");
        const timeWindow = parts[0]; // time window
        const timePart = timeWindow.slice(-6); // 获取最后 5 个字符 "xx时xx分"
        const hhmm = timePart.replace("点", "").replace("分", "");

        const data = parts[1]
        const fields = data.split(",")
        // set data values
        timeLabels.push(hhmm); // time window

        // main flow
        mainNetInflow.push(parseFloat(fields[0]))
        mainInflow.push(parseFloat(fields[1]));
        mainOutflow.push(-parseFloat(fields[2]));

        // extra large orders
        extraLargeBuyVolume.push(parseFloat(fields[3]));
        extraLargeBuyPrice.push(parseFloat(fields[4]));
        extraLargeSellVolume.push(-parseFloat(fields[5]));
        extraLargeSellPrice.push(-parseFloat(fields[6]));

        // large orders
        largeBuyVolume.push(parseFloat(fields[7]));
        largeBuyPrice.push(parseFloat(fields[8]));
        largeSellVolume.push(-parseFloat(fields[9]));
        largeSellPrice.push(-parseFloat(fields[10]));

        // medium orders
        mediumBuyVolume.push(parseFloat(fields[11]));
        mediumBuyPrice.push(parseFloat(fields[12]));
        mediumSellVolume.push(-parseFloat(fields[13]));
        mediumSellPrice.push(-parseFloat(fields[14]));

        // small orders
        smallBuyVolume.push(parseFloat(fields[15]));
        smallBuyPrice.push(parseFloat(fields[16]));
        smallSellVolume.push(-parseFloat(fields[17]));
        smallSellPrice.push(-parseFloat(fields[18]));
    });

    return { timeLabels, mainNetInflow, mainInflow, mainOutflow,
        extraLargeBuyVolume, extraLargeBuyPrice, extraLargeSellVolume, extraLargeSellPrice,
        largeBuyVolume, largeBuyPrice, largeSellVolume, largeSellPrice,
        mediumBuyVolume, mediumBuyPrice, mediumSellVolume, mediumSellPrice,
        smallBuyVolume, smallBuyPrice, smallSellVolume, smallSellPrice};
}

// visualize data
function visualizeData(data) {
    const ctxMainInflow = document.getElementById('mainInflowChart').getContext('2d');

    const ctxExtraLargeVolume = document.getElementById('extraLargeVolumeChart').getContext('2d');
    const ctxExtraLargePrice = document.getElementById('extraLargePriceChart').getContext('2d');

    const ctxLargeVolume = document.getElementById('largeVolumeChart').getContext('2d');
    const ctxLargePrice = document.getElementById('largePriceChart').getContext('2d');

    const ctxMediumVolume = document.getElementById('mediumVolumeChart').getContext('2d');
    const ctxMediumPrice = document.getElementById('mediumPriceChart').getContext('2d');

    const ctxSmallVolume = document.getElementById('smallVolumeChart').getContext('2d');
    const ctxSmallPrice = document.getElementById('smallPriceChart').getContext('2d');

    // 主力流入
    createChart(ctxMainInflow, 'Main flow', data.timeLabels, data.mainInflow, data.mainOutflow, data.mainNetInflow);

    // 超大订单
    createDualChart(ctxExtraLargeVolume, ctxExtraLargePrice, 'Extra-large', data.timeLabels,
        data.extraLargeBuyVolume, data.extraLargeSellVolume,
        data.extraLargeBuyPrice, data.extraLargeSellPrice);

    // 大订单
    createDualChart(ctxLargeVolume, ctxLargePrice, 'Large', data.timeLabels,
        data.largeBuyVolume, data.largeSellVolume,
        data.largeBuyPrice, data.largeSellPrice);

    // 中订单
    createDualChart(ctxMediumVolume, ctxMediumPrice, 'Medium', data.timeLabels,
        data.mediumBuyVolume, data.mediumSellVolume,
        data.mediumBuyPrice, data.mediumSellPrice);

    // 小订单
    createDualChart(ctxSmallVolume, ctxSmallPrice, 'Small', data.timeLabels,
        data.smallBuyVolume, data.smallSellVolume,
        data.smallBuyPrice, data.smallSellPrice);
}

function createChart(ctx, label, timeLabels, data1, data2, data3) {
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: timeLabels,
            datasets: [
                {
                    label: `Inflow`,
                    data: data1,
                    borderColor: 'green',
                    fill: false,
                },
                {
                    label: `Outflow`,
                    data: data2,
                    borderColor: 'red',
                    fill: false,
                },
                {
                    label: `Net inflow`,
                    data: data3,
                    borderColor: 'blue',
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Time',
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: 'Amount',
                    },
                },
            },
        },
    });
}


function createDualChart(ctxVolume, ctxPrice, category, timeLabels,
                         buyVolume, sellVolume, buyPrice, sellPrice) {
    // 买入/卖出量图表
    new Chart(ctxVolume, {
        type: 'line',
        data: {
            labels: timeLabels,
            datasets: [
                {
                    label: `${category} purchased order quantity`,
                    data: buyVolume,
                    borderColor: 'green',
                    fill: false,
                },
                {
                    label: `${category} sold order quantity`,
                    data: sellVolume,
                    borderColor: 'red',
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Time',
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: 'Quantity',
                    },
                },
            },
        },
    });

    // 买入/卖出金额图表
    new Chart(ctxPrice, {
        type: 'line',
        data: {
            labels: timeLabels,
            datasets: [
                {
                    label: `${category} purchased order quantity`,
                    data: buyPrice,
                    borderColor: 'rgba(100,255,0,0.8)',
                    fill: false,
                },
                {
                    label: `${category} sold order quantity`,
                    data: sellPrice,
                    borderColor: 'rgba(255,100,0,0.8)',
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Time',
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: 'Amount',
                    },
                },
            },
        },
    });
}


// read local file
function fetchAndUpdateData() {
    fetch('part-r-00000.txt') // 从本地 HTTP 服务器加载文件
        .then(response => response.text())
        .then(content => {
            const parsedData = parseFileContent(content); // 解析文件内容
            visualizeData(parsedData); // 更新图表
        })
        .catch(error => {
            console.error('Error fetching data:', error);
        });
}

// update chart over a period of time
setInterval(fetchAndUpdateData, 60000); // 每 60 秒更新一次

// first used when fetching the page
fetchAndUpdateData();
