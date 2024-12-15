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
        const data = parts[1]
        const fields = data.split(",")
        // set data values
        timeLabels.push(timeWindow); // time window

        // main flow
        mainNetInflow.push(parseFloat(fields[0]))
        mainInflow.push(parseFloat(fields[1]));
        mainOutflow.push(parseFloat(fields[2]));

        // extra large orders
        extraLargeBuyVolume.push(parseFloat(fields[3]));
        extraLargeBuyPrice.push(parseFloat(fields[4]));
        extraLargeSellVolume.push(parseFloat(fields[5]));
        extraLargeSellPrice.push(parseFloat(fields[6]));

        // large orders
        largeBuyVolume.push(parseFloat(fields[7]));
        largeBuyPrice.push(parseFloat(fields[8]));
        largeSellVolume.push(parseFloat(fields[9]));
        largeSellPrice.push(parseFloat(fields[10]));

        // medium orders
        mediumBuyVolume.push(parseFloat(fields[11]));
        mediumBuyPrice.push(parseFloat(fields[12]));
        mediumSellVolume.push(parseFloat(fields[13]));
        mediumSellPrice.push(parseFloat(fields[14]));

        // small orders
        smallBuyVolume.push(parseFloat(fields[15]));
        smallBuyPrice.push(parseFloat(fields[16]));
        smallSellVolume.push(parseFloat(fields[17]));
        smallSellPrice.push(parseFloat(fields[18]));
    });

    return { timeLabels, mainNetInflow, mainInflow, mainOutflow,
        extraLargeBuyVolume, extraLargeBuyPrice, extraLargeSellVolume, extraLargeSellPrice,
        largeBuyVolume, largeBuyPrice, largeSellVolume, largeSellPrice,
        mediumBuyVolume, mediumBuyPrice, mediumSellVolume, mediumSellPrice,
        smallBuyVolume, smallBuyPrice, smallSellVolume, smallSellPrice};
}

// visualize data
function visualizeData(data) {
    const ctx = document.getElementById('myChart').getContext('2d');

    // if chart already exists, destroy it
    if (chart) {
        chart.destroy();
    }

    // create new chart instance
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.timeLabels, // 时间标签
            datasets: [
                {
                    label: '主力流入',
                    data: data.mainInflow,
                    borderColor: 'blue',
                    fill: false,
                },
                {
                    label: '超大买单成交量',
                    data: data.largeBuyVolume,
                    borderColor: 'green',
                    fill: false,
                },
                {
                    label: '大卖单成交量',
                    data: data.largeSellVolume,
                    borderColor: 'red',
                    fill: false,
                }
            ],
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: '时间',
                    },
                },
                y: {
                    title: {
                        display: true,
                        text: '金额/成交量',
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
