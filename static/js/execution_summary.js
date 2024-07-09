export function plotExecutionSummary(taskInfoCSV) {

    // calculate the number of tasks and average duration for each worker
    let workerTaskData = [];
    let workerIndex = 1;
    let workerIDs = Object.keys(workerInfo).filter(key => key.startsWith('worker'));
    workerIDs.forEach(workerID => {
        let tasks = workerInfo[workerID].slots;
        let count = 0;
        let totalDuration = 0;
        for (let slotId in tasks) {
            count += tasks[slotId].length;
            for (let task in tasks[slotId]) {
                // exclude library tasks
                if (tasks[slotId][task][3].includes("library")) {
                    count--;
                    continue;
                }
                totalDuration += tasks[slotId][task][2] - tasks[slotId][task][1];
            }
        }
        let avgDuration = count > 0 ? totalDuration / count : 0;
        // use workerIndex as workerId
        workerTaskData.push({ workerId: workerIndex.toString(), count, avgDuration });
        workerIndex++;
    });

    let totalTasks = d3.sum(workerTaskData, d => d.count);
    workerTaskData.forEach(d => {
        d.percentage = (d.count / totalTasks) * 100;
    });

    d3.select('#histogram').selectAll('*').remove();
    drawComponents(workerTaskData);


    window.addEventListener('resize', function() {
        d3.select('#histogram').selectAll('*').remove();
        drawComponents(workerTaskData);
    });
    const selector = document.getElementById('histogramSelector');
    selector.addEventListener('change', function(event) {
        const selectedMode = event.target.value;
        if (selectedMode === 'mode0') {
            d3.select('#histogram').selectAll('*').remove();
            workerTaskData.sort((a, b) => a.workerId - b.workerId);
            drawComponents(workerTaskData);
        }
        if (selectedMode === 'mode1') {
            d3.select('#histogram').selectAll('*').remove();
            workerTaskData.sort((a, b) => a.avgDuration - b.avgDuration);
            drawComponents(workerTaskData);
        } else if (selectedMode === 'mode2') {
            d3.select('#histogram').selectAll('*').remove();
            workerTaskData.sort((a, b) => b.count - a.count);
            drawComponents(workerTaskData);
        }
      });
}

function drawComponents(workerTaskData) {
    const container = document.getElementById('histogramContainer')
    const containerWidth = container.clientWidth;
    const containerHeight = container.clientHeight;

    const svgWidth = containerWidth;
    const svgHeight = containerHeight;
    const padding = { top: containerHeight * 0.05, right: containerWidth * 0.1, bottom: containerHeight * 0.1, left: containerWidth * 0.1 };
    
    // 选择已存在的SVG元素并设置其大小
    const svg = d3.select("#histogram")
        .attr('viewBox', `0 0 ${containerWidth} ${containerHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${padding.left}, ${padding.top})`);

    const width = svgWidth - padding.left - padding.right;
    const height = svgHeight - padding.top - padding.bottom;

    // draw X axis
    let xScale = drawX(svg, workerTaskData, width, height, padding);
    // draw Y axis
    let {yScale, yScaleRight} = drawY(svg, workerTaskData, width, height, padding);
    // draw histogram
    drawHistogram(svg, workerTaskData, xScale, yScale, height);
    // draw average duration points
    drawAvgDuration(svg, width, workerTaskData, xScale, yScaleRight);
}


function drawX(svg, workerTaskData, width, height, padding) {
    const xTicks = workerTaskData.length;            // number of workers
    const widthPerTick = width / xTicks;             // width per tick
    let fontX = widthPerTick / 1.4;
    const container = document.getElementById('histogramContainer');
    fontX = Math.min(fontX, container.clientWidth / 50);
    const xLabelFontSize = height / 40;

    const xScale = d3.scaleBand()
        .range([0, width])
        .padding(0.3)
        .domain(workerTaskData.map(d => d.workerId));  // use workerId as x axis domain

    const xAxis = d3.axisBottom(xScale);
    svg.append("g")
        .attr("class", "x-axis")
        .call(xAxis)
        .attr("transform", `translate(0,${height})`);

    // apply font size to x axis
    svg.selectAll(".x-axis text")
        .style("font-size", `${fontX}px`);

    // set x axis label
    svg.append("text")
        .attr("transform",
              "translate(" + (width / 2) + " ," + 
                             (height + padding.bottom / 1.2) + ")")
        .style("text-anchor", "middle")
        .style("font-size", xLabelFontSize + "px")
        .text("Worker");

    return xScale;
}

function drawY(svg, workerTaskData, width, height, padding) {
    const maxTaskCount = d3.max(workerTaskData, d => d.count);
    const maxAverageDuration = d3.max(workerTaskData, d => d.avgDuration);

    // set y scale
    const yScale = d3.scaleLinear()
        .domain([0, maxTaskCount])
        .range([height, 0]);
    const yScaleRight = d3.scaleLinear()
        .domain([0, maxAverageDuration * 1.2])
        .range([height, 0]);
    const yAxisLeft = d3.axisLeft(yScale).ticks(10);

    // calculate font sizes
    const container = document.getElementById('histogramContainer')
    const fontYLeft = container.clientHeight / 50;
    const fontYRight = container.clientHeight / 50;
    const yLabelFontSize = container.clientHeight / 40;

    svg.append("g")
        .attr("class", "y-axis")
        .call(yAxisLeft)
        .style("font-size", fontYLeft + "px");
    const yAxisRight = d3.axisRight(yScaleRight);
    svg.append("g")
        .attr("class", "y-axis-right")
        .attr("transform", `translate(${width},${0})`)
        .call(yAxisRight)
        .style("font-size", fontYRight + "px");
    
    // set y axis label
    svg.append("text")
        .attr("transform",
              "translate(" + (-padding.left / 1.5) + " ," + 
                             (height / 2) + ")" + "rotate(-90)")
        .style("text-anchor", "middle")
        .style("font-size", yLabelFontSize + "px")
        .text("Number of Tasks");
    svg.append("text")
        .attr("transform",
              "translate(" + (width + padding.right / 1.5) + " ," + 
                             (height / 2) + ")" + "rotate(-90)")
        .style("text-anchor", "middle")
        .style("font-size", yLabelFontSize + "px")
        .text("Average Execution Time (s)");

    return { yScale, yScaleRight };
}

function drawHistogram(svg, workerTaskData, xScale, yScale, height) {
    // this is the color of the bars
    let barColor = "rgba(140, 34, 32, 0.7)";

    svg.selectAll(".bar")
        .data(workerTaskData)
        .enter().append("rect")
        .attr("x", d => xScale(d.workerId))
        .attr("y", d => yScale(d.count))
        .attr("width", xScale.bandwidth())
        .attr("height", d => height - yScale(d.count))
        .attr("fill", barColor)
        .on("mouseover", function(event, d) {
            d3.select(event.currentTarget) // choose the current element
                .attr("fill", "orange");   // set the color to orange
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
            d3.select(event.currentTarget) // select the current element by using the event
                .attr("fill", barColor)
            d3.select("#histogramTooltip")
                .style("visibility", "hidden");
        });
}

function drawAvgDuration(svg, width, workerTaskData, xScale, yScaleRight) {
    let pointColor = "rgba(168, 9, 7, 1)";
    // create a tooltip
    const tooltip = d3.select("#avgDurationTooltip");

    const xTicks = workerTaskData.length;            // number of workers
    const widthPerTick = width / xTicks;             // width per tick
    const fontX = widthPerTick / 1.4;
    const container = document.getElementById('histogramContainer');
    const pointRadius = Math.min(fontX * 0.6, container.clientWidth / 60);

    svg.selectAll(".avgDurationPoint")
        .data(workerTaskData)
        .enter().append("circle")
        .attr("class", "avgDurationPoint")
        .attr("cx", d => xScale(d.workerId) + xScale.bandwidth() / 2)
        .attr("cy", d => yScaleRight(d.avgDuration))
        .attr("r", pointRadius)
        .attr("fill", pointColor)
        .on("mouseover", function(event, d) {
            // highlight the point
            d3.select(event.currentTarget)
                .attr("r", pointRadius * 2) // change the radius
                .attr("fill", "orange");    // change the color
            
            // show tooltip
            tooltip.style("visibility", "visible")
                .html(`Avg Execution Time: ${d.avgDuration.toFixed(2)}s<br>Worker: ${d.workerId}`)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY + 10) + "px");
        })
        .on("mouseout", function(event, d) {
            // recover the point
            d3.select(event.currentTarget)
                .attr("r", pointRadius)     // recover the radius
                .attr("fill", pointColor);  // recover the color
            
            // hide the tooltip
            tooltip.style("visibility", "hidden");
        });
    
    // generate a line 
    const lineGenerator = d3.line()
        .x(d => xScale(d.workerId) + xScale.bandwidth() / 2) // determine x coordinate
        .y(d => yScaleRight(d.avgDuration)); // determine y coordinate

    // draw the line
    svg.append("path")
        .datum(workerTaskData)
        .attr("fill", "none")
        .attr("stroke", pointColor)
        .attr("stroke-width", 2)
        .attr("d", lineGenerator);
}
