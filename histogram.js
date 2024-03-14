export function drawWorkerTaskHistogram(workerInfo) {
    // 计算每个worker的task数量
    let taskCounts = [];
    let workerIndex = 1; // 用于生成刻度的序号
    let workerIDs = Object.keys(workerInfo).filter(key => key.startsWith('worker'));
    workerIDs.forEach(workerID => {
        let tasks = workerInfo[workerID].slots;
        let count = 0;
        let totalDuration = 0;
        for (let slotId in tasks) {
            count += tasks[slotId].length;
            tasks[slotId].forEach(task => totalDuration += task[2] - task[1]);
        }
        let avgDuration = count > 0 ? totalDuration / count : 0;
        // 使用序号作为x轴的标签
        taskCounts.push({ workerId: workerIndex.toString(), count, avgDuration });
        workerIndex++; // 更新序号
    });

    let totalTasks = d3.sum(taskCounts, d => d.count);
    taskCounts.forEach(d => {
        d.percentage = (d.count / totalTasks) * 100;
    });

    // 设置绘图参数
    const container = document.getElementById('histogramContainer')
    const containerWidth = container.clientWidth;
    const containerHeight = container.clientHeight;

    const svgWidth = containerWidth; // 使用视窗宽度
    const svgHeight = containerHeight; // 高度为宽度的60%
    const padding = { top: 30, right: 100, bottom: 60, left: 100 };
    
    // 选择已存在的SVG元素并设置其大小
    const svg = d3.select("#histogram")
        .attr('viewBox', `0 0 ${containerWidth} ${containerHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${padding.left}, ${padding.top})`);

    const width = svgWidth - padding.left - padding.right;
    const height = svgHeight - padding.top - padding.bottom;

    // 绘制X轴
    let initialFontSize = calculateFontSize(window.innerWidth);
    let xScale = drawX(svg, taskCounts, width, height, padding, initialFontSize);
    // 绘制Y轴
    let {yScale, yScaleRight} = drawY(svg, taskCounts, width, height, padding, initialFontSize);
    // 绘制直方图
    drawHistogram(svg, taskCounts, xScale, yScale, height, padding);
    // 绘制平均执行时间
    drawAvgDuration(svg, taskCounts, xScale, yScaleRight);
    

    window.addEventListener('resize', function() {
            let newFontSize = calculateFontSize(window.innerWidth);
            d3.select("#histogram").selectAll("text")
                .style("font-size", newFontSize + "px");
            d3.select("#histogram").selectAll(".x-axis")
                .style("font-size", newFontSize + "px");
            d3.select("#histogram").selectAll(".y-axis")
                .style("font-size", newFontSize + "px");
        });
}

// 根据窗口宽度计算字体大小的函数
function calculateFontSize(innerWidth) {
    return Math.max(12, innerWidth / 100);
}

function drawX(svg, taskCounts, width, height, padding, initialFontSize) {
    const xScale = d3.scaleBand()
        .range([0, width])
        .padding(0.3)
        .domain(taskCounts.map(d => d.workerId)); // 使用序号作为domain
    const xAxis = d3.axisBottom(xScale);
    svg.append("g")
        .attr("class", "x-axis")
        .call(xAxis)
        .attr("transform", `translate(0,${height})`);

    // 计算x轴刻度字体大小
    const xTicks = xScale.domain().length; // x轴的刻度数量
    const availableSpace = width / xTicks; // 每个刻度可用的平均空间
    let fontSize = Math.min(calculateFontSize(window.innerWidth), availableSpace / 1.5); // 根据可用空间计算字体大小，这里假设最小字体大小为12px
    // 应用计算出的字体大小到x轴刻度
    svg.selectAll(".x-axis text")
        .style("font-size", `${fontSize}px`);

    // set x axis label
    svg.append("text")
        .attr("transform",
              "translate(" + (width / 2) + " ," + 
                             (height + padding.bottom / 1.5) + ")")
        .style("text-anchor", "middle") // 根据标签的具体位置，可能需要调整对齐方式
        .style("font-size", initialFontSize + "px")
        .text("Worker");

    return xScale;
}

function drawY(svg, taskCounts, width, height, padding, initialFontSize) {
    const maxAverageDuration = d3.max(taskCounts, d => d.avgDuration);
    console.log(maxAverageDuration);
    const yScale = d3.scaleLinear()
        .domain([0, d3.max(taskCounts, d => d.count)])
        .range([height, 0]);
    const yScaleRight = d3.scaleLinear()
        .domain([0, maxAverageDuration * 1.2])
        .range([height, 0]);

    const yAxis = d3.axisLeft(yScale);
    svg.append("g")
        .attr("class", "y-axis")
        .call(yAxis)
        .style("font-size", calculateFontSize(window.innerWidth) + "px");
    const yAxisRight = d3.axisRight(yScaleRight);
    svg.append("g")
        .attr("class", "y-axis-right")
        .attr("transform", `translate(${width},${0})`) // 移动到SVG右侧
        .call(yAxisRight)
        .style("font-size", calculateFontSize(window.innerWidth) + "px");
    
    // set y axis label
    svg.append("text")
        .attr("transform",
              "translate(" + (-padding.left / 1.5) + " ," + 
                             (height / 2) + ")" + "rotate(-90)")
        .style("text-anchor", "middle")
        .style("font-size", initialFontSize + "px")
        .text("Number of Tasks");
    svg.append("text")
        .attr("transform",
              "translate(" + (width + padding.right / 1.5) + " ," + 
                             (height / 2) + ")" + "rotate(-90)")
        .style("text-anchor", "middle")
        .style("font-size", initialFontSize + "px")
        .text("Average Execution Time (s)");

    return { yScale, yScaleRight };
}

function drawHistogram(svg, taskCounts, xScale, yScale, height, padding) {
    // 绘制直方图柱状图
    let barColor = "rgba(140, 34, 32, 0.7)";
    svg.selectAll(".bar")
        .data(taskCounts)
        .enter().append("rect")
        .attr("x", d => xScale(d.workerId))
        .attr("y", d => yScale(d.count))
        .attr("width", xScale.bandwidth())
        .attr("height", d => height - yScale(d.count))
        .attr("fill", barColor)
        .on("mouseover", function(event, d) {
            d3.select(event.currentTarget) // 正确选择当前元素
                .attr("fill", "orange"); // 将当前元素的填充色改为 orange
            d3.select("#histogramTooltip")
                .style("visibility", "visible")
                .html(`Tasks: ${d.count} (${d.percentage.toFixed(2)}%)<br>Worker: ${d.workerId}`);
        })
        .on("mousemove", function(event) {
            d3.select("#histogramTooltip")
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 10) + "px");
        })
        .on("mouseout", function(event) {
            d3.select(event.currentTarget) // 使用 event.currentTarget 选择当前元素
                .attr("fill", barColor)
            d3.select("#histogramTooltip")
                .style("visibility", "hidden");
        });
}

function drawAvgDuration(svg, taskCounts, xScale, yScaleRight) {
    let pointColor = "rgba(168, 9, 7, 1)";
    // 定义提示框的选择器，便于使用
    const tooltip = d3.select("#avgDurationTooltip");

    svg.selectAll(".avgDurationPoint")
        .data(taskCounts)
        .enter().append("circle")
        .attr("class", "avgDurationPoint")
        .attr("cx", d => xScale(d.workerId) + xScale.bandwidth() / 2)
        .attr("cy", d => yScaleRight(d.avgDuration))
        .attr("r", 5)
        .attr("fill", pointColor)
        .on("mouseover", function(event, d) {
            // 高亮点
            d3.select(event.currentTarget)
                .attr("r", 10) // 例如，通过增加半径大小来高亮
                .attr("fill", "orange"); // 或改变颜色
            
            // 显示提示框并设置内容为百分比数值
            tooltip.style("visibility", "visible")
                .html(`Avg Execution Time: ${d.avgDuration.toFixed(2)}s<br>Worker: ${d.workerId}`) // 保留两位小数
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY + 10) + "px");
        })
        .on("mouseout", function(event, d) {
            // 恢复点的原始样式
            d3.select(event.currentTarget)
                .attr("r", 5) // 恢复原始半径大小
                .attr("fill", pointColor); // 恢复原始填充颜色
            
            // 隐藏提示框
            tooltip.style("visibility", "hidden");
        });
    
    // 生成连接点的线
    const lineGenerator = d3.line()
        .x(d => xScale(d.workerId) + xScale.bandwidth() / 2) // 使用同样的计算方式来确定x坐标
        .y(d => yScaleRight(d.avgDuration)); // 使用百分比来确定y坐标

    // 绘制连接所有点的线
    svg.append("path")
        .datum(taskCounts) // 这里使用datum而不是data，因为我们要绘制的是一条连接所有点的线
        .attr("fill", "none")
        .attr("stroke", pointColor)
        .attr("stroke-width", 2)
        .attr("d", lineGenerator); // 使用lineGenerator来生成"d"属性的值
}