
export function plotAccumulatedFiles(workerDiskUpdateCSV, taskInfoCSV, workerSummaryCSV, useDiskUtilization = false) {
    // parse and preprocess data
    const workerDiskUpdate = d3.csvParse(workerDiskUpdateCSV);
    workerDiskUpdate.forEach(function(d) {
        d.start_time = +d.start_time;
        d.disk_increament_in_mb = +d.disk_increament_in_mb;
        d.disk_usage_in_mb = +d.disk_usage_in_mb;
    });
    const groupedworkerDiskUpdate = d3.group(workerDiskUpdate, d => d.worker_id);

    const workerSummary = d3.csvParse(workerSummaryCSV);
    const workerDiskSize = new Map();
    workerSummary.forEach(d => {
        workerDiskSize.set(d.worker_id, +d['disk(MB)']);
    });
    
    // get the minTime, maxTime and maxDiskUsage
    const taskInfo = d3.csvParse(taskInfoCSV);
    const minTime = d3.min(taskInfo, d => +d.when_ready);
    const maxTime = d3.max(taskInfo, d => +d.when_done);
    let maxDiskUsage;
    if (useDiskUtilization) {
        maxDiskUsage = d3.max(workerSummary, d => +d['peak_disk_usage(%)']);
    } else {
        maxDiskUsage = d3.max(Array.from(groupedworkerDiskUpdate.values()), group =>
            d3.max(group, d => d.disk_usage_in_mb)
        );
    }
    console.log('maxDiskUsage:', maxDiskUsage); 

    const container = document.getElementById('worker-accumulated-cached-files-container');
    const margin = {top: 20, right: 20, bottom: 40, left: 90};
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    const svg = d3.select('#worker-accumulated-cached-files')
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
        .tickFormat(d3.format(".6f"));
    svg.append('g')
        .call(yAxis);

    // Create line generator
    const line = d3.line()
        .x(d => xScale(d.start_time - minTime))
        .y(d => {
            const diskUsage = useDiskUtilization 
                ? d.disk_usage_in_mb / workerDiskSize.get(d.worker_id)
                : d.disk_usage_in_mb;
            return yScale(diskUsage);
        });

    // Draw lines for each worker
    const tooltip = document.getElementById('vine-tooltip');
    groupedworkerDiskUpdate.forEach((value, key) => {
        const originalColor = d3.schemeCategory10[key % 10];
        const path = svg.append("path")
            .datum(value)
            .attr("class", "line")
            .attr("fill", "none")
            .attr("stroke", originalColor)  // Assign color using a categorical scheme
            .attr("stroke-width", 0.8)
            .attr("d", line)
            .attr("data-original-color", originalColor)
            .on("mouseover", function(event, d) {
                // change color
                d3.select(this).raise();
                d3.selectAll("path.line").attr("stroke", "lightgray");
                d3.select(this)
                    .attr("stroke", "orange")
                    .attr("stroke-width", 2);
                // show tooltip
                tooltip.innerHTML = `
                    worker id: ${key}<br>
                `;

                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on("mousemove", function(event) {
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
            .duration(300) 
            .ease(d3.easeLinear)
            .attr("stroke-dashoffset", 0);
    });


}