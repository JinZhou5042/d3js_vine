export function plotExecutionDetails(taskInfoCSV, workerInfoJson) {
    const taskInfo = d3.csvParse(taskInfoCSV);
    const workerInfo = JSON.parse(workerInfoJson);

    const container = document.getElementById('execution-details-container');
    const margin = {top: 20, right: 20, bottom: 40, left: 40};
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;
    const slotPadding = 0.2;

    const svg = d3.select('#execution-details')
        .attr('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const minTime = d3.min(taskInfo, d => +d.when_ready);
    const xScale = d3.scaleLinear()
        .domain([0, d3.max(taskInfo, d => +d.time_worker_end - minTime)])
        .range([0, svgWidth]);

    const sortedTaskInfo = taskInfo.slice().sort((a, b) => b.worker_id - a.worker_id);
    const yScale = d3.scaleBand()
        .domain(sortedTaskInfo.map(d => d.worker_id + '-' + d.worker_slot))
        .range([0, svgHeight])
        .padding(slotPadding);
    
    ////////////////////////////////////////////
    // create rectange for each worker
    const workerEntries = Object.entries(workerInfo);
    workerEntries.forEach(([worker, Info]) => {
        let worker_id = Info.worker_id;
        const rect = svg.append('rect')
            .attr('x', xScale(+Info.time_connected - minTime))
            .attr('y', yScale(worker_id + '-' + Info.slot_count))
            .attr('width', xScale(+Info.time_disconnected - minTime) - xScale(+Info.time_connected - minTime))
            .attr('height', yScale.bandwidth() * Info.slot_count + (yScale.step() - yScale.bandwidth()) * (Info.slot_count - 1))
            .attr('fill', 'lightgrey')
            .attr('opacity', 0.3)
            .on('mouseover', function(event, d) {
                d3.select(this).attr('fill', 'orange');
                // show tooltip
                const tooltip = document.getElementById('execution-details-tooltip');
                tooltip.innerHTML = `
                    worker id: ${Info.worker_id}<br>
                    when connected: ${(Info.time_connected - minTime).toFixed(2)}s<br>
                    when disconnected: ${(Info.time_disconnected - minTime).toFixed(2)}s<br>
                    life time: ${(Info.time_disconnected - Info.time_connected).toFixed(2)}s<br>`;
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', function(event, d) {
                d3.select(this).attr('fill', 'lightgrey');
                // hide tooltip
                const tooltip = document.getElementById('execution-details-tooltip');
                tooltip.style.visibility = 'hidden';
            });
    });
    ////////////////////////////////////////////

    // create rectange for each task (time extent: time_worker_start ~ time_worker_end)
    svg.selectAll('.task-worker-running-rect')
        .data(sortedTaskInfo)
        .enter()
        .append('rect')
        .attr('class', 'task-worker-running-rect')
        .attr('x', d => xScale(+d.time_worker_start - minTime))
        .attr('y', d => yScale(d.worker_id + '-' + d.worker_slot))
        .attr('width', d => xScale(+d.time_worker_end) - xScale(+d.time_worker_start))
        .attr('height', yScale.bandwidth())
        .attr('fill', 'steelblue')
        .on('mouseover', function(event, d) {
            // change color
            d3.select(this).attr('fill', 'orange');
            // show tooltip
            const tooltip = document.getElementById('execution-details-tooltip');
            tooltip.innerHTML = `
                task id: ${d.task_id}<br>
                worker: ${d.worker_id} (slot ${d.worker_slot})<br>
                execution time: ${(d.time_worker_end - d.time_worker_start).toFixed(2)}s<br>
                input size: ${(d.size_input_mgr - 0).toFixed(4)}MB<br>
                output size: ${(d.size_output_mgr - 0).toFixed(4)}MB<br>
                when ready: ${(d.when_ready - minTime).toFixed(2)}s<br>
                when running: ${(d.when_running - minTime).toFixed(20)}s<br>
                when actually running: ${(d.time_worker_start - minTime).toFixed(20)}s<br>
                when actually done: ${(d.time_worker_end - minTime).toFixed(2)}s<br>
                when waiting retrieval: ${(d.when_waiting_retrieval - minTime).toFixed(2)}s<br>
                when retrieved: ${(d.when_retrieved - minTime).toFixed(2)}s<br>
                when done: ${(d.when_done - minTime).toFixed(2)}s<br>`;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mouseout', function() {
            // restore color
            d3.select(this).attr('fill', 'steelblue');
            // hide tooltip
            const tooltip = document.getElementById('execution-details-tooltip');
            tooltip.style.visibility = 'hidden';
        });
    
    // create rectange for each task (time extent: when_ready ~ time_worker_start)
    /*
    svg.selectAll('.task-waiting-rect')
        .data(sortedTaskInfo)
        .enter()
        .append('rect')
        .attr('class', 'task-submitting-rect')
        .attr('x', d => xScale(+d.when_ready - minTime))
        .attr('y', d => yScale(d.worker_id + '-' + d.worker_slot))
        .attr('width', d => xScale(+d.time_worker_start) - xScale(+d.when_ready))
        .attr('height', yScale.bandwidth())
        .attr('fill', 'rgba(173, 216, 230, 0.2)');
    */


    // draw x axis
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
        .attr('transform', `translate(0, ${svgHeight + yScale.bandwidth()})`)
        .call(xAxis);
    
    // draw y axis
    const numWorkers = sortedTaskInfo.length;
    const maxTicks = 5;
    const tickInterval = Math.max(1, Math.floor(numWorkers / maxTicks));
    const yTicks = sortedTaskInfo
        .filter((_, i) => i % tickInterval === 0)
        .map(d => d.worker_id + '-' + d.worker_slot);

    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0)
        .tickValues(yTicks)
        .tickFormat(d => d.split('-')[0]);

    svg.append('g')
        .call(yAxis);
}
