
export function plotAccumulatedFiles(workerDiskUpdateCSV, workerSummaryCSV, manager_time_start, manager_time_end, displayDiskUsageByPercentage) {
    // parse and preprocess data
    const workerDiskUpdate = d3.csvParse(workerDiskUpdateCSV);

    workerDiskUpdate.forEach(function(d) {
        d.start_time = +d.time;
    });
    const groupedworkerDiskUpdate = d3.group(workerDiskUpdate, d => d.worker_id);

    const workerSummary = d3.csvParse(workerSummaryCSV);
    
    // get the minTime, maxTime and maxDiskUsage
    const minTime = manager_time_start;
    const maxTime = manager_time_end;
    let maxDiskUsage;
    if (displayDiskUsageByPercentage) {
        maxDiskUsage = d3.max(workerSummary, d => +d['peak_disk_usage(%)']);
    } else {
        maxDiskUsage = d3.max(workerSummary, d => +d['peak_disk_usage(MB)']);
    }

    const container = document.getElementById('per-worker-disk-usage-container');
    const margin = {top: 20, right: 20, bottom: 40, left: 60};
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    const svg = d3.select('#per-worker-disk-usage')
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
        const originalColor = d3.schemeCategory10[key % 10];
        const path = svg.append("path")
            .datum(value)
            .attr("class", "line")
            .attr("fill", "none")
            .attr("stroke", originalColor)  // Assign color using a categorical scheme
            .attr("data-original-color", originalColor)
            .attr("stroke-width", 0.8)
            .attr("d", line)
            .on("mouseover", function(event, d) {
                // change color
                d3.select(this).raise();
                d3.selectAll("path.line").attr("stroke", "lightgray");
                d3.select(this)
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
                    const originalColor = d3.select(this).attr("data-original-color");
                    d3.select(this).attr("stroke", originalColor);
                });
                const originalColor = d3.select(this).attr("data-original-color");
                d3.select(this)
                    .attr("stroke", originalColor)
                    .attr("stroke-width", 0.8);
                // hide tooltip
                tooltip.style.visibility = 'hidden';
            });

        const totalLength = path.node().getTotalLength();
    
        // add animation
        path.attr("stroke-dasharray", `${totalLength} ${totalLength}`)
            .attr("stroke-dashoffset", totalLength)
            .transition()
            .duration(0) 
            .ease(d3.easeLinear)
            .attr("stroke-dashoffset", 0);
    });


}