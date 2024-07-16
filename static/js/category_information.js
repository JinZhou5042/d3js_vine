/*
<h2 id="tasks-category-information-title">Tasks Category Information</h2>
<div id="task-category-information-preface" class="preface">
</div>
<div class="report-toolbox">
    <button id="button-reset-task-category-information" class="report-button">Reset</button>
    <button id="button-task-category-information-sort-by-avg-time" class="report-button">Sort by Avg Execution Time</button>
    <button id="button-download-task-category-information" class="report-button">Download SVG</button>
</div>
<div id="task-category-information-container" class="container-alpha" >
    <svg id="task-category-information-svg" xmlns="http://www.w3.org/2000/svg">
    </svg>
</div>
*/

import { downloadSVG, setupZoomAndScroll } from './tools.js';

const buttonReset = document.getElementById('button-reset-task-category-information');
const buttonDownload = document.getElementById('button-download-task-category-information');
const buttonSortByAvgExecutionTime = document.getElementById('button-task-category-information-sort-by-avg-time');
const svgContainer = document.getElementById('task-category-information-container');
const svgElement = d3.select('#task-category-information-svg');

const barColor = "#065fae";
const lineAvgExecutionTimeColor = "#f0be41";
const lineMaxExecutionTimeColor = "#8c1a11";
const lineStrokeWidth = 1.5;
const highlightColor = "orange";
const tooltip = document.getElementById('vine-tooltip');
const maxBarWidth = 50;

export function plotTaskCategoryInformation({ sortByAvgExecutionTime = false } = {}) {
    svgElement.selectAll('*').remove();
    const margin = {top: 40, right: 80, bottom: 40, left: 80};
    const svgWidth = svgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = svgContainer.clientHeight - margin.top - margin.bottom;

    const svg = svgElement
        .attr('viewBox', `0 0 ${svgContainer.clientWidth} ${svgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    var taskDone = window.taskDone;
    taskDone.forEach(function(d) {
        d.task_id = +d.task_id;
        d.execution_time = +d.execution_time;
    });

    const categories = d3.group(taskDone, d => d.category);
    let categoryToId = new Map();
    let categoryId = 1;
    taskDone = Array.from(categories, ([categoryName, tasks]) => {
        if (!categoryToId.has(categoryName)) {
            categoryToId.set(categoryName, categoryId++);
        }
        return { categoryId: categoryToId.get(categoryName), tasks: tasks, categoryName: categoryName, taskCount: tasks.length };
    });
    
    taskDone.forEach(category => {
        const avgExecutionTime = d3.mean(category.tasks, d => d.execution_time);
        const maxExecutionTime = d3.max(category.tasks, d => d.execution_time);
        const minExecutionTime = d3.min(category.tasks, d => d.execution_time);
        category.avgExecutionTime = avgExecutionTime;
        category.maxExecutionTime = maxExecutionTime;
        category.minExecutionTime = minExecutionTime;
    });
    
    if (sortByAvgExecutionTime) {
        // Sort taskDone by avgExecutionTime in descending order
        taskDone.sort((a, b) => a.avgExecutionTime - b.avgExecutionTime);
    } else {
        // Sort taskDone by taskCount in descending order
        taskDone.sort((a, b) => a.taskCount - b.taskCount);
    }
    
    // Reassign category IDs based on sorted order
    categoryId = 1;
    taskDone.forEach(category => {
        categoryToId.set(category.categoryName, categoryId);
        category.categoryId = categoryId;
        categoryId++;
    });
    const maxExecutionTime = d3.max(taskDone, d => d.maxExecutionTime);
    taskDone.forEach(category => {
        const avgExecutionTime = d3.mean(category.tasks, d => d.execution_time);
        category.avgExecutionTime = avgExecutionTime;
    });
    const maxAvgExecutionTime = d3.max(taskDone, d => d.avgExecutionTime);
    const maxTaskCount = d3.max(taskDone, d => d.taskCount);
    
    // Setup scaleBand and xAxis with dynamic tick values
    const categoryIds = taskDone.map(d => d.categoryId);
    let tickValues = [];
    if (categoryIds.length >= 4) {
        tickValues = [
            categoryIds[0],  // First element
            categoryIds[Math.floor((categoryIds.length - 1) / 3)],  // One-third
            categoryIds[Math.floor((categoryIds.length - 1) * 2 / 3)],  // Two-thirds
            categoryIds[categoryIds.length - 1]  // Last element
        ];
    } else {
        tickValues = categoryIds;
    }
    const xScale = d3.scaleBand()
        .domain(categoryIds)
        .range([0, svgWidth])
        .padding(0.2);

    const xAxis = d3.axisBottom(xScale)
        .tickSizeOuter(0)
        .tickValues(tickValues)
        .tickFormat(d => taskDone.find(e => e.categoryId === d).categoryId);

    // Append and transform the x-axis on the SVG
    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end");

    // Setup yScale and yAxis
    const yScaleLeft = d3.scaleLinear()
        .domain([0, maxTaskCount])
        .range([svgHeight, 0]);
    const yAxisLeft = d3.axisLeft(yScaleLeft)
        .tickSizeOuter(0)
        .tickValues([
            yScaleLeft.domain()[0],
            yScaleLeft.domain()[0] + (yScaleLeft.domain()[1] - yScaleLeft.domain()[0]) * 0.25,
            yScaleLeft.domain()[0] + (yScaleLeft.domain()[1] - yScaleLeft.domain()[0]) * 0.5,
            yScaleLeft.domain()[0] + (yScaleLeft.domain()[1] - yScaleLeft.domain()[0]) * 0.75,
            yScaleLeft.domain()[1]
        ])
        .tickFormat(d => d.toString());
    svg.append("g")
        .call(yAxisLeft);
    const yScaleRight = d3.scaleLinear()
        .domain([0, maxExecutionTime])
        .range([svgHeight, 0]);
    const yAxisRight = d3.axisRight(yScaleRight)
        .tickSizeOuter(0)
        .tickValues([
            yScaleRight.domain()[0],
            yScaleRight.domain()[0] + (yScaleRight.domain()[1] - yScaleRight.domain()[0]) * 0.25,
            yScaleRight.domain()[0] + (yScaleRight.domain()[1] - yScaleRight.domain()[0]) * 0.5,
            yScaleRight.domain()[0] + (yScaleRight.domain()[1] - yScaleRight.domain()[0]) * 0.75,
            yScaleRight.domain()[1]
        ])
        .tickFormat(d3.format(".2f"));
    svg.append("g")
        .attr("transform", `translate(${svgWidth}, 0)`)
        .call(yAxisRight);

    ////////////////////////////////////////////////////////////
    // labels
    svg.append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", (0 - margin.left) * (2 / 3))
        .attr("x", 0 - (svgHeight / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .style("font-size", "14px")
        .text("Task Count");
    svg.append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", svgWidth + margin.right * (2 / 3))
        .attr("x", 0 - (svgHeight / 2))
        .attr("dy", "1em")
        .style("text-anchor", "middle")
        .style("font-size", "14px")
        .text("Execution Time (s)");

    ////////////////////////////////////////////////////////////
    // legend
    const legendData = [
        { color: barColor, label: "TaskCount", type: "line" },
        { color: lineAvgExecutionTimeColor, label: "Average Execution Time", type: "line" },
        { color: lineMaxExecutionTimeColor, label: "Max Execution Time", type: "line" },
    ];
    const legendX = 10;
    const legendY = 0;
    const legendWidth = 50;
    const legendSpacing = 20;

    const legend = svg.append("g")
        .attr("class", "legend")
        .attr("transform", `translate(${legendX},${legendY})`);
    legend.selectAll(".legend-item")
        .data(legendData)
        .enter()
        .append("g")
        .attr("class", "legend-item")
        .attr("transform", (d, i) => `translate(0, ${i * legendSpacing})`)
        .each(function(d) {
            if (d.type === "line") {
                d3.select(this).append("line")
                    .attr("x1", 0)
                    .attr("y1", 0)
                    .attr("x2", legendWidth)
                    .attr("y2", 0)
                    .attr("stroke", d.color)
                    .attr("stroke-width", 2);
            } else if (d.type === "bar") {
                d3.select(this).append("rect")
                    .attr("x", 0)
                    .attr("y", -5)
                    .attr("width", legendWidth)
                    .attr("height", 10)
                    .attr("fill", d.color);
            }
            d3.select(this).append("text")
                .attr("x", legendX + legendWidth + 2)
                .attr("y", 0)
                .attr("dy", "0.35em")
                .style("fill", d.color)
                .style("font-weight", "bold")
                .style("text-anchor", "start")
                .style("font-size", "14px")
                .text(d.label);
        });
    ////////////////////////////////////////////////////////////
    /*
    svg.selectAll(".bar")
        .data(taskDone)
        .enter()
        .append("rect")
        .classed("bar", true)
        .attr("x", d => xScale(d.categoryId) + (xScale.bandwidth() - Math.min(xScale.bandwidth(), maxBarWidth)) / 2)
        .attr("y", d => yScaleLeft(d.taskCount))
        .attr("width", d => Math.min(xScale.bandwidth(), maxBarWidth)) 
        .attr("height", d => svgHeight - yScaleLeft(d.taskCount))
        .attr("fill", barColor)
        .on("mouseover", function(event, d) {
            d3.select(this)
                .attr('fill', highlightColor);
            tooltip.innerHTML = `
                Category ID: ${d.categoryId}<br/>
                Category Name: ${d.categoryName}<br/>
                Number of tasks: ${d.taskCount}<br/>
                Avg Execution Time: ${d.avgExecutionTime.toFixed(2)}<br/>
                Max Execution Time: ${d.maxExecutionTime.toFixed(2)}<br/>
                Min Execution Time: ${d.minExecutionTime.toFixed(2)}
            `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on("mousemove", function(event) {
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on("mouseout", function() {
            d3.select(this)
                .attr('fill', barColor);
            tooltip.style.visibility = 'hidden';
        });
    */

    var lineGenerator = d3.line()
        .x(d => xScale(d.categoryId) + xScale.bandwidth() / 2)
        .y(d => yScaleRight(d.avgExecutionTime));
    svg.append("path")
        .datum(taskDone)
        .attr("fill", "none")
        .attr("stroke", lineAvgExecutionTimeColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", lineGenerator);

    lineGenerator = d3.line()
        .x(d => xScale(d.categoryId) + xScale.bandwidth() / 2)
        .y(d => yScaleRight(d.maxExecutionTime));
    svg.append("path")
        .datum(taskDone)
        .attr("fill", "none")
        .attr("stroke", lineMaxExecutionTimeColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", lineGenerator);

    lineGenerator = d3.line()
        .x(d => xScale(d.categoryId) + xScale.bandwidth() / 2)
        .y(d => yScaleLeft(d.taskCount));
    svg.append("path")
        .datum(taskDone)
        .attr("fill", "none")
        .attr("stroke", barColor)
        .attr("stroke-width", lineStrokeWidth)
        .attr("d", lineGenerator);
}

function handleSortByAvgExecutionTimeClick() {
    this.classList.toggle('report-button-active');
    plotTaskCategoryInformation({sortByAvgExecutionTime: this.classList.contains('report-button-active')});
}
function handleDownloadClick() {
    downloadSVG('task-category-information-svg');
}
function handleResetClick() {
    if (buttonSortByAvgExecutionTime.classList.contains('report-button-active')) {
        buttonSortByAvgExecutionTime.classList.remove('report-button-active');
    }
    plotTaskCategoryInformation({sortByAvgExecutionTime: false});
}

window.parent.document.addEventListener('dataLoaded', function() {
    buttonSortByAvgExecutionTime.classList.remove('report-button-active');
    plotTaskCategoryInformation({sortByAvgExecutionTime: false});

    buttonDownload.removeEventListener('click', handleDownloadClick);
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);

    buttonSortByAvgExecutionTime.removeEventListener('click', handleSortByAvgExecutionTimeClick);
    buttonSortByAvgExecutionTime.addEventListener('click', handleSortByAvgExecutionTimeClick);
});