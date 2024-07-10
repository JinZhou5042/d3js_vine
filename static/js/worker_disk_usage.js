import { formatUnixTimestamp, downloadSVG } from './tools.js';

export function plotWorkerDiskUsage({ displayDiskUsageByPercentage = false, highlightWorkerID = null, displayAccumulationOnly = false } = {}) {
    // first remove all the elements in the svg
    d3.select('#worker-disk-usage').selectAll('*').remove();

    const groupedworkerDiskUpdate = d3.group(window.workerDiskUpdate, d => d.worker_id);

    const workerSummary = window.workerSummary;
    
    // get the minTime, maxTime and maxDiskUsage
    const minTime = window.time_manager_start;
    const maxTime = window.manager_time_end;
    let columnNameMB = 'disk_usage(MB)';
    let columnNamePercentage = 'disk_usage(%)';
    if (displayAccumulationOnly) {
        columnNameMB = 'disk_usage_accumulation(MB)';
        columnNamePercentage = 'disk_usage_accumulation(%)';
    }
    let maxDiskUsage;
    if (displayDiskUsageByPercentage) {
        maxDiskUsage = d3.max(window.workerDiskUpdate, function(d) { return +d[columnNamePercentage]; });
    } else {
        maxDiskUsage = d3.max(window.workerDiskUpdate, function(d) { return +d[columnNameMB]; });
    }

    const container = document.getElementById('worker-disk-usage-container');
    const margin = {top: 20, right: 20, bottom: 40, left: 60};
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    const svg = d3.select('#worker-disk-usage')
        .attr('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append("g")
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    // Setup scales
    const xScale = d3.scaleLinear()
        .domain([0, maxTime - minTime])
        .range([0, svgWidth]);
    const yScale = d3.scaleLinear()
        .domain([0, maxDiskUsage])
        .range([svgHeight, 0]);

    // Draw axes
    const xAxis = d3.axisBottom(xScale)
        .tickSizeOuter(0)
        .tickValues([
            xScale.domain()[0],
            xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.25,
            xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.5,
            xScale.domain()[0] + (xScale.domain()[1] - xScale.domain()[0]) * 0.75,
            xScale.domain()[1]
        ])
        .tickFormat(d3.format(".1f"));
    svg.append('g')
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis);
    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0)
        .tickValues([
            yScale.domain()[0],
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.25,
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.5,
            yScale.domain()[0] + (yScale.domain()[1] - yScale.domain()[0]) * 0.75,
            yScale.domain()[1]
        ])
        .tickFormat(displayDiskUsageByPercentage === true ? d3.format(".4f") : d3.format(".2f"));
    svg.append('g')
        .call(yAxis);

    // Create line generator
    const line = d3.line()
        .x(d => {
            if (d.time - minTime < 0) {
                console.log('d.time - minTime < 0, d.time = ', d.time, 'minTime = ', minTime);
            }
            if (isNaN(d.time - minTime)) {
                console.log('d.time - minTime is NaN', d);
            }
            return xScale(d.time - minTime);
        })
        .y(d => {
            const diskUsage = displayDiskUsageByPercentage 
                ? d[columnNamePercentage]
                : d[columnNameMB];
            if (isNaN(diskUsage)) {
                console.log('diskUsage is NaN', d);
            }
            return yScale(diskUsage);
        });

    // Draw accumulated disk usage
    const tooltip = document.getElementById('vine-tooltip');
    groupedworkerDiskUpdate.forEach((value, key) => {
        key = +key;
        let lineColor;
        let strokeWidth = 0.8;
        if (highlightWorkerID !== null && key === highlightWorkerID) {
            lineColor = 'orange';
            strokeWidth = 2;
        } else if (highlightWorkerID !== null) {
            lineColor = 'lightgray';
        } else {
            lineColor = d3.schemeCategory10[key % 10];
        }
        const path = svg.append("path")
            .datum(value)
            .attr("class", "line")
            .attr("fill", "none")
            .attr("stroke", lineColor)  // Assign color using a categorical scheme
            .attr("original-color", lineColor)
            .attr("stroke-width", strokeWidth)
            .attr("original-stroke-width", strokeWidth)
            .attr("d", line);
        
        if (highlightWorkerID === null) {
            path.on("mouseover", function(event, d) {
                // change color
                d3.selectAll("path.line").attr("stroke", "lightgray");
                d3.select(this)
                    .raise()
                    .attr("stroke", "orange")
                    .attr("stroke-width", 2);
                // show tooltip
                const svgRect = svg.node().getBoundingClientRect();
                const xPosition = (event.clientX - svgRect.left) * (svgWidth / svgRect.width);
                const yPosition = (event.clientY - svgRect.top) * (svgHeight / svgRect.height);
                const xValue = xScale.invert(xPosition);
                const yValue = yScale.invert(yPosition);
                tooltip.innerHTML = `
                    worker id: ${key}<br>
                    time: ${xValue.toFixed(2)}<br>
                    disk usage: ${yValue.toFixed(2)}<br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on("mousemove", function(event) {
                const svgRect = svg.node().getBoundingClientRect();
                const xPosition = (event.clientX - svgRect.left) * (svgWidth / svgRect.width);
                const yPosition = (event.clientY - svgRect.top) * (svgHeight / svgRect.height);
                const xValue = xScale.invert(xPosition);
                const yValue = yScale.invert(yPosition);
                tooltip.innerHTML = `
                    worker id: ${key}<br>
                    time: ${xValue.toFixed(2)}s<br>
                    disk usage: ${yValue.toFixed(2)}<br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on("mouseout", function(d) {
                // remove highlight
                d3.selectAll("path.line").each(function() {
                    const originalColor = d3.select(this).attr("original-color");
                    d3.select(this).attr("stroke", originalColor);
                });
                const originalColor = d3.select(this).attr("original-color");
                d3.select(this)
                    .attr("stroke", originalColor)
                    .attr("stroke-width", 0.8);
                // hide tooltip
                tooltip.style.visibility = 'hidden';
            });
        }
    });
    
    // traverse the lines and raise the highlighted line
    let highlightedLine = null;
    if (highlightWorkerID) {
        d3.selectAll("path.line").each(function() {
            const workerID = +d3.select(this).datum()[0].worker_id;
            if (workerID === highlightWorkerID) {
                d3.select(this).raise();
                highlightedLine = d3.select(this);
            }
        });
    }

    if (highlightedLine) {
        d3.select('#worker-disk-usage').on("mousemove", function(event) {
            const [mouseX, mouseY] = d3.pointer(event, this);
            const positionX = xScale.invert(mouseX - margin.left);
            const positionY = yScale.invert(mouseY - margin.top);
    
            let minDistance = Infinity;
            let closestPoint = null;
    
            const lineData = highlightedLine.datum();

            lineData.forEach(point => {
                const pointX = point['time'] - minTime;
                const pointY = point['disk_usage(MB)'];
    
                const distance = Math.sqrt(Math.pow(positionX - pointX, 2) + Math.pow(positionY - pointY, 2));

                if (distance < minDistance) {
                    minDistance = distance;
                    closestPoint = point;
                }
            });
    
            if (closestPoint) {
                const pointX = xScale(closestPoint['time'] - minTime);
                const pointY = yScale(closestPoint[displayDiskUsageByPercentage ? columnNamePercentage : columnNameMB]);
                tooltip.innerHTML = `
                    worker id: ${closestPoint.worker_id}<br>
                    filename: ${closestPoint.filename}<br>
                    time from start: ${(+closestPoint.time - minTime).toFixed(2)}s<br>
                    time in human: ${formatUnixTimestamp(+closestPoint.time)}<br>
                    disk contribute: ${(+closestPoint['size(MB)']).toFixed(4)}MB<br>
                    disk usage: ${(+closestPoint[displayDiskUsageByPercentage ? columnNamePercentage : columnNameMB]).toFixed(2)}${displayDiskUsageByPercentage ? '%' : 'MB'}<br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (pointY + margin.top + container.getBoundingClientRect().top + window.scrollY + 5) + 'px';
                tooltip.style.left = (pointX + margin.left + container.getBoundingClientRect().left + window.scrollX + 5) + 'px';
            }
        });
    } else {
        tooltip.style.visibility = 'hidden';
        d3.select('#worker-disk-usage').on("mousemove", null);
    }
}

document.getElementById('button-display-worker-disk-usage-by-percentage').addEventListener('click', async function() {
    this.classList.toggle('report-button-active');
    // first clean the plot
    d3.select('#worker-disk-usage').selectAll('*').remove();
    // get the highlight worker id
    let highlightWorkerID = null;
    let buttonAnalyzeWorker = document.getElementById('button-highlight-worker-disk-usage');
    if (buttonAnalyzeWorker.classList.contains('report-button-active')) {
        highlightWorkerID = getHighlightWorkerID();
    }
    plotWorkerDiskUsage({displayDiskUsageByPercentage: this.classList.contains('report-button-active'),
        highlightWorkerID: highlightWorkerID,
        displayAccumulationOnly: document.getElementById('button-display-accumulated-only').classList.contains('report-button-active'),
    });
});

document.getElementById('button-display-accumulated-only').addEventListener('click', async function() {
    this.classList.toggle('report-button-active');
    // first clean the plot
    d3.select('#worker-disk-usage').selectAll('*').remove();
    // get the highlight worker id
    let highlightWorkerID = null;
    let buttonAnalyzeWorker = document.getElementById('button-highlight-worker-disk-usage');
    if (buttonAnalyzeWorker.classList.contains('report-button-active')) {
        highlightWorkerID = getHighlightWorkerID();
    }
    let buttonDisplayPercentages = document.getElementById('button-display-worker-disk-usage-by-percentage');
    plotWorkerDiskUsage({displayDiskUsageByPercentage: buttonDisplayPercentages.classList.contains('report-button-active'),
        highlightWorkerID: highlightWorkerID,
        displayAccumulationOnly: this.classList.contains('report-button-active'),
    });
});

function getHighlightWorkerID() {
    let workerID = document.getElementById('input-highlight-worker-disk-usage').value;
    if (!window.workerDiskUpdate.some(d => d.worker_id === workerID)) {
        workerID = null;
    } else {
        workerID = +workerID;
    }
    return workerID;
}

document.getElementById('button-highlight-worker-disk-usage').addEventListener('click', async function() {
    // get the highlight worker id
    let workerID = getHighlightWorkerID();
    if (this.classList.contains('report-button-active')) {
        if (workerID === null) {
            this.classList.toggle('report-button-active');
        }
    } else if (!this.classList.contains('report-button-active') && workerID !== null) {
        this.classList.toggle('report-button-active');
    }

    // get the percentage button
    const buttonDisplayPercentages = document.getElementById('button-display-worker-disk-usage-by-percentage');

    plotWorkerDiskUsage({displayDiskUsageByPercentage: buttonDisplayPercentages.classList.contains('report-button-active'),
        highlightWorkerID: workerID,
        displayAccumulationOnly: document.getElementById('button-display-accumulated-only').classList.contains('report-button-active'),
    });
});

window.parent.document.addEventListener('dataLoaded', function() {
    const buttonDisplayPercentages = document.getElementById('button-display-worker-disk-usage-by-percentage');
    const buttonDisplayAccumulatedOnly = document.getElementById('button-display-accumulated-only');
    const buttonHighlightWorker = document.getElementById('button-highlight-worker-disk-usage');

    // deactivating the buttons
    buttonDisplayPercentages.classList.remove('report-button-active');
    buttonDisplayAccumulatedOnly.classList.remove('report-button-active');
    buttonHighlightWorker.classList.remove('report-button-active');

    function handleDownloadClick() {
        downloadSVG('worker-disk-usage', 'worker_disk_usage.svg');
    }
    var button = document.getElementById('button-download-worker-disk-usage');
    button.removeEventListener('click', handleDownloadClick); 
    button.addEventListener('click', handleDownloadClick);
});

