
export function plotWorkerDiskUsage(displayDiskUsageByPercentage = false, highlightWorkerID = null) {
    // first remove all the elements in the svg
    d3.select('#worker-disk-usage').selectAll('*').remove();

    window.workerDiskUpdate.forEach(function(d) {
        d.start_time = +d.time;
    });
    const groupedworkerDiskUpdate = d3.group(window.workerDiskUpdate, d => d.worker_id);

    const workerSummary = window.workerSummary;
    
    // get the minTime, maxTime and maxDiskUsage
    const minTime = window.manager_time_start;
    const maxTime = window.manager_time_end;
    let maxDiskUsage;
    if (displayDiskUsageByPercentage) {
        maxDiskUsage = d3.max(workerSummary, d => +d['peak_disk_usage(%)']);
    } else {
        maxDiskUsage = d3.max(workerSummary, d => +d['peak_disk_usage(MB)']);
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
            if (d.start_time - minTime < 0) {
                console.log('d.start_time - minTime < 0, d.start_time = ', d.start_time, 'minTime = ', minTime);
            }
            if (isNaN(d.start_time - minTime)) {
                console.log('d.start_time - minTime is NaN', d);
            }
            return xScale(d.start_time - minTime);
        })
        .y(d => {
            const diskUsage = displayDiskUsageByPercentage 
                ? d['disk_usage(%)']
                : d['disk_usage(MB)'];
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
        console.log('strokeWidth = ', strokeWidth);
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
                const pointX = point['start_time'] - minTime;
                const pointY = point['disk_usage(MB)'];
    
                const distance = Math.sqrt(Math.pow(positionX - pointX, 2) + Math.pow(positionY - pointY, 2));

                if (distance < minDistance) {
                    minDistance = distance;
                    closestPoint = point;
                }
            });
    
            if (closestPoint) {
                const pointX = xScale(closestPoint['start_time'] - minTime);
                const pointY = yScale(closestPoint[displayDiskUsageByPercentage ? 'disk_usage(%)' : 'disk_usage(MB)']);
                tooltip.innerHTML = `
                    worker id: ${closestPoint.worker_id}<br>
                    filename: ${closestPoint.filename}<br>
                    time: ${(+closestPoint.start_time - minTime).toFixed(2)}s<br>
                    disk contribute: ${(+closestPoint['size(MB)']).toFixed(4)}MB<br>
                    disk usage: ${(+closestPoint[displayDiskUsageByPercentage ? 'disk_usage(%)' : 'disk_usage(MB)']).toFixed(2)}${displayDiskUsageByPercentage ? '%' : 'MB'}<br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (pointY + margin.top + container.getBoundingClientRect().top + window.scrollY + 5) + 'px';
                tooltip.style.left = (pointX + margin.left + container.getBoundingClientRect().left + window.scrollX + 5) + 'px';
            }
        });
    }
}

document.getElementById('button-display-worker-disk-usage-by-percentage').addEventListener('click', async function() {
    this.classList.toggle('report-button-active');
    // first clean the plot
    d3.select('#worker-disk-usage').selectAll('*').remove();
    plotWorkerDiskUsage(this.classList.contains('report-button-active'));
});

document.getElementById('button-display-worker-disk-usage-by-worker-id').addEventListener('click', async function() {
    let workerID = document.getElementById('input-display-worker-disk-usage-by-worker-id').value;
    if (!window.workerDiskUpdate.some(d => d.worker_id === workerID)) {
        workerID = null;
    } else {
        workerID = +workerID;
    }
    plotWorkerDiskUsage(false, workerID);
});