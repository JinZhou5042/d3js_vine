
export function plotExecutionDetails() {
    const taskDone = window.taskDone;
    const taskFailedOnWorker = window.taskFailedOnWorker;
    const workerSummary = window.workerSummary;

    const minTime = window.manager_time_start;
    const maxTime = window.manager_time_end;

    const container = document.getElementById('execution-details-container');
    const margin = calculateMargin(container, workerSummary);
    
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    // initialize svg
    const svg = d3.select('#execution-details')
        .attr('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const { xScale, yScale } = plotAxis(svg, svgWidth, svgHeight, minTime, maxTime, workerSummary);

    const colors = {
        'worker': {
            'normal': 'lightgrey',
            'highlight': 'orange',
        },
        'ready-on-manager': {
            'normal': 'red',
            'highlight': '#72bbb0',
        },
        'waiting-to-execute-on-worker': {
            'normal': 'lightblue',
            'highlight': '#72bbb0',
        },
        'executing-on-worker': {
            'normal': 'steelblue',
            'highlight': 'orange',
        },
        'waiting-retrieval-on-worker': {
            'normal': '#40909f',
            'highlight': '#bed380',
        },
        'task-failed-on-worker-rect': {
            'normal': '#ad2c23',
            'highlight': 'orange',
        },
        'recovery-task': {
            'normal': '#ea67a9',
            'highlight': 'orange',
        },
    }

    ////////////////////////////////////////////
    // create rectanges for each worker
    const tooltip = document.getElementById('vine-tooltip');
    const workerEntries = workerSummary.map(d => ({
        worker: d.worker_hash,
        Info: {
            worker_id: +d.worker_id,
            time_connected: +d.time_connected,
            time_disconnected: +d.time_disconnected,
            cores: +d.cores,
        }
    }));
    workerEntries.forEach(({ worker, Info }) => {
        let worker_id = Info.worker_id;
        const rect = svg.append('rect')
            .attr('x', xScale(+Info.time_connected - minTime))
            .attr('y', yScale(worker_id + '-' + Info.cores))
            .attr('width', xScale(+Info.time_disconnected - minTime) - xScale(+Info.time_connected - minTime))
            .attr('height', yScale.bandwidth() * Info.cores + (yScale.step() - yScale.bandwidth()) * (Info.cores - 1))
            .attr('fill', colors.worker.normal)
            .attr('opacity', 0.3)
            .on('mouseover', function(event, d) {
                d3.select(this)
                    .attr('fill', colors.worker.highlight);
                // show tooltip
                tooltip.innerHTML = `
                    cores: ${Info.cores}<br>
                    worker id: ${Info.worker_id}<br>
                    when connected: ${(Info.time_connected - minTime).toFixed(2)}s<br>
                    when disconnected: ${(Info.time_disconnected - minTime).toFixed(2)}s<br>
                    life time: ${(Info.time_disconnected - Info.time_connected).toFixed(2)}s<br>`;
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mousemove', function(event) {
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', function(event, d) {
                d3.select(this)
                    .attr('fill', colors.worker.normal);
                // hide tooltip
                tooltip.style.visibility = 'hidden';
            });
    });    
    ////////////////////////////////////////////

    ////////////////////////////////////////////
    // create rectange for each successful task (time extent: when_running ~ time_worker_start)
    svg.selectAll('.task-rect')
        .data(taskDone)
        .enter()
        .append('g')
        .each(function(d) {
            var g = d3.select(this);
            if (true) {
                g.append('rect')
                .attr('class', 'waiting-to-execute-on-worker')
                .attr('x', d => xScale(+d.when_running - minTime))
                .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
                .attr('width', xScale(+d.time_worker_start) - xScale(+d.when_running)) 
                .attr('height', yScale.bandwidth())
                .attr('fill', colors['waiting-to-execute-on-worker'].normal);
            }
            g.append('rect')
                .attr('class', 'executing-on-worker')
                .attr('x', d => xScale(+d.time_worker_start - minTime))
                .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
                .attr('width', d => xScale(+d.time_worker_end) - xScale(+d.time_worker_start))
                .attr('height', yScale.bandwidth())
                .attr('fill', function(d) {
                    return d.is_recovery_task === "True" ? colors['recovery-task'].normal : colors['executing-on-worker'].normal;
                });            
            if (false) {
                g.append('rect')
                .attr('class', 'waiting-retrieval-on-worker')
                .attr('x', d => xScale(+d.time_worker_end - minTime))
                .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
                .attr('width', d => xScale(+d.when_waiting_retrieval) - xScale(+d.time_worker_end))
                .attr('height', yScale.bandwidth())
                .attr('fill', colors['waiting-retrieval-on-worker'].normal);
            }
        })
        .on('mouseover', function(event, d) {
            d3.select(this).selectAll('rect').each(function() {
                if (this.classList.contains('waiting-to-execute-on-worker')) {
                    d3.select(this).attr('fill', colors['waiting-to-execute-on-worker'].highlight);
                } else if (this.classList.contains('executing-on-worker')) {
                    d3.select(this).attr('fill', colors['executing-on-worker'].highlight);
                } else if (this.classList.contains('waiting-retrieval-on-worker')) {
                    d3.select(this).attr('fill', colors['waiting-retrieval-on-worker'].highlight);
                }
            });

            // show tooltip
            tooltip.innerHTML = `
                task id: ${d.task_id}<br>
                worker: ${d.worker_id} (core ${d.core_id})<br>
                category: ${d.category.replace(/^<|>$/g, '')}<br>
                execution time: ${(d.time_worker_end - d.time_worker_start).toFixed(2)}s<br>
                input size: ${(d.size_input_mgr - 0).toFixed(4)}MB<br>
                output size: ${(d.size_output_mgr - 0).toFixed(4)}MB<br>
                when ready: ${(d.when_ready - minTime).toFixed(2)}s<br>
                when running: ${(d.when_running - minTime).toFixed(2)}s<br>
                when actually running: ${(d.time_worker_start - minTime).toFixed(2)}s<br>
                when actually done: ${(d.time_worker_end - minTime).toFixed(2)}s<br>
                when waiting retrieval: ${(d.when_waiting_retrieval - minTime).toFixed(2)}s<br>
                when retrieved: ${(d.when_retrieved - minTime).toFixed(2)}s<br>
                when done: ${(d.when_done - minTime).toFixed(2)}s<br>`;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mousemove', function(event) {
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mouseout', function() {
            // hide tooltip
            tooltip.style.visibility = 'hidden';
            // restore color
            d3.select(this).selectAll('rect').each(function() {
                if (this.classList.contains('waiting-to-execute-on-worker')) {
                    d3.select(this).attr('fill', colors['waiting-to-execute-on-worker'].normal);
                } else if (this.classList.contains('executing-on-worker')) {
                    d3.select(this).attr('fill', function(d) {
                        return d.is_recovery_task === "True" ? colors['recovery-task'].normal : colors['executing-on-worker'].normal;
                    });  
                } else if (this.classList.contains('waiting-retrieval-on-worker')) {
                    d3.select(this).attr('fill', colors['waiting-retrieval-on-worker'].normal);
                }
            });
        });

    ////////////////////////////////////////////

    ////////////////////////////////////////////
    // create rectange for each failed task (time extent: time_worker_start ~ time_worker_end)
    svg.selectAll('.task-failed-on-worker-rect')
        .data(taskFailedOnWorker)
        .enter()
        .append('rect')
        .attr('class', 'task-failed-on-worker-rect')
        .attr('x', d => xScale(+d.when_running - minTime))
        .attr('y', d => yScale(d.worker_id + '-' + d.core_id))
        .attr('width', d => xScale(+d.when_next_ready) - xScale(+d.when_running))
        .attr('height', yScale.bandwidth())
        .attr('fill', colors['task-failed-on-worker-rect'].normal)
        .attr('opacity', 0.8)
        .on('mouseover', function(event, d) {
            // change color
            d3.select(this).attr('fill', colors['task-failed-on-worker-rect'].highlight);
            // show tooltip
            tooltip.innerHTML = `
                task id: ${d.task_id}<br>
                worker: ${d.worker_id} (core ${d.core_id})<br>
                execution time: ${(d.when_next_ready - d.when_running).toFixed(2)}s<br>
                input size: ${(d.size_input_mgr - 0).toFixed(4)}MB<br>
                when ready: ${(d.when_ready - minTime).toFixed(2)}s<br>
                when running: ${(d.when_running - minTime).toFixed(2)}s<br>
                when next ready: ${(d.when_next_ready - minTime).toFixed(2)}s<br>`;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (event.pageY + 10) + 'px';
            tooltip.style.left = (event.pageX + 10) + 'px';
        })
        .on('mouseout', function() {
            // restore color
            d3.select(this).attr('fill', colors['task-failed-on-worker-rect'].normal);
            // hide tooltip
            tooltip.style.visibility = 'hidden';
        });

    ////////////////////////////////////////////
}

function calculateMargin(container, workerSummary) {
    const margin = {top: 40, right: 30, bottom: 40, left: 30};
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    const tempSvg = d3.select('#execution-details')
        .attr('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const workerCoresMap = [];
    workerSummary.forEach(d => {
        for (let i = 1; i <= +d.cores; i++) {
            workerCoresMap.push(`${d.worker_id}-${i}`);
        }
    });

    const yScale = d3.scaleBand()
        .domain(workerCoresMap)
        .range([svgHeight, 0])
        .padding(0.1);

    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0);

    tempSvg.append('g').call(yAxis);

    const maxTickWidth = d3.max(tempSvg.selectAll('.tick text').nodes(), d => d.getBBox().width);
    tempSvg.remove();

    margin.left = maxTickWidth + 15;

    return margin
}

function plotAxis(svg, svgWidth, svgHeight, minTime, maxTime, workerSummary) {
    // set x scale
    const xScale = d3.scaleLinear()
        .domain([0, maxTime - minTime])
        .range([0, svgWidth]);
    // set y scale
    const workerCoresMap = [];
    workerSummary.forEach(d => {
        for (let i = 1; i <= +d.cores; i++) {
            workerCoresMap.push(`${d.worker_id}-${i}`);
        }
    });
    const yScale = d3.scaleBand()
        .domain(workerCoresMap)
        .range([svgHeight, 0])
        .padding(0.1);
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
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis);

    // draw y axis
    const totalWorkers = workerSummary.length;
    const maxTicks = 5;
    const tickInterval = Math.ceil(totalWorkers / maxTicks);
    const selectedTicks = [];
    for (let i = totalWorkers - 1; i >= 0; i -= tickInterval) {
        selectedTicks.unshift(`${workerSummary[i].worker_id}-${workerSummary[i].cores}`);
    }
    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0)
        .tickValues(selectedTicks)
        .tickFormat(d => d.split('-')[0]);
    svg.append('g')
        .call(yAxis);

    return { xScale, yScale };
}

function plotSVG() {

}