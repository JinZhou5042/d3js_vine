import { fetchFile, downloadSVG } from './tools.js';

const buttonDownload = document.getElementById('button-download-worker-connections');
const buttonReset = document.getElementById('button-reset-worker-connections');
const factoryDescriptionContainer = document.getElementById('factory-description-container');
const tooltip = document.getElementById('vine-tooltip');

function fillMgrDescription() {
    document.getElementById('start-time').textContent = window.generalStatisticsManager.time_start_human;
    document.getElementById('end-time').textContent = window.generalStatisticsManager.time_end_human;
    document.getElementById('lift-time').textContent = window.generalStatisticsManager['lifetime(s)'] + 's';
    document.getElementById('tasks-submitted').textContent = window.generalStatisticsManager.tasks_submitted;
    document.getElementById('tasks-done').textContent = window.generalStatisticsManager.tasks_done;
    document.getElementById('tasks-waiting').textContent = window.generalStatisticsManager.tasks_failed_on_manager;
    document.getElementById('tasks-failed').textContent = window.generalStatisticsManager.tasks_failed_on_worker;
    document.getElementById('workers-connected').textContent = window.generalStatisticsManager.total_workers;
    document.getElementById('workers-active').textContent = window.generalStatisticsManager.active_workers;
    document.getElementById('max-concurrent-workers').textContent = window.generalStatisticsManager.max_concurrent_workers;
}
async function fillFactoryDescription() {
    try {
        var factory = await fetchFile(`logs/${window.logName}/vine-logs/factory.json`);
        factory = JSON.parse(factory);
        factory = JSON.stringify(factory, null, 2);
        factoryDescriptionContainer.innerHTML = `<pre class="formatted-json"><b>factory.json</b> ${factory}</pre>`;
    } catch (error) {
        factoryDescriptionContainer.innerHTML = 'Error fetching factory.json';
    }
}

async function plotWorkerConnections() {
    const data = window.workerConnections;
    const minTime = window.time_manager_start;

    const container = document.getElementById('worker-connections-container');
    const margin = {top: 40, right: 30, bottom: 40, left: 30};

    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    // first remove the current svg
    d3.select('#worker-connections').selectAll('*').remove();
    // initialize svg
    const svg = d3.select('#worker-connections')
        .attr('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    data.forEach(function(d) {
        d.time = +d.time;
        d.parallel_workers = +d.parallel_workers;
    });
    const maxParallelWorkers = d3.max(data, d => d.parallel_workers);

    const xScale = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.time - minTime)])
        .range([0, svgWidth]);

    const yScale = d3.scaleLinear()
        .domain([0, maxParallelWorkers])
        .range([svgHeight, 0]);
    
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
    svg.append("g")
        .attr("transform", `translate(0,${svgHeight})`)
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end");
    
    let selectedTicks;
    if (maxParallelWorkers <= 3) {
        selectedTicks = d3.range(0, maxParallelWorkers + 1);
    } else {
        selectedTicks = [0, Math.round(maxParallelWorkers / 3), Math.round((2 * maxParallelWorkers) / 3), maxParallelWorkers];
    }
    const yAxis = d3.axisLeft(yScale)
                    .tickValues(selectedTicks)
                    .tickFormat(d3.format("d"))
                    .tickSizeOuter(0);
    
    svg.append("g")
        .call(yAxis);

    const line = d3.line()
        .x(d => xScale(d.time - minTime))
        .y(d => yScale(d.parallel_workers));

    svg.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "#5aa4ae")
        .attr("stroke-width", 1.5)
        .attr("d", line);

    svg.selectAll("circle")
        .data(data)
        .enter()
        .append("circle")
        .attr("cx", d => xScale(d.time - minTime))
        .attr("cy", d => yScale(d.parallel_workers))
        .attr("r", 1)
        .attr("fill", "#145ca0");


    d3.select('#worker-connections').on("mousemove", function(event) { 
        const [mouseX, mouseY] = d3.pointer(event, this);
        const positionX = xScale.invert(mouseX - margin.left);
        const positionY = yScale.invert(mouseY - margin.top);

        let minDistance = Infinity;
        let closestPoint = null;

        data.forEach(point => {
            const pointX = point['time'] - minTime;
            const pointY = point['parallel_workers'];
            const distance = Math.sqrt((pointX - positionX) ** 2 + (pointY - positionY) ** 2);
            if (distance < minDistance) {
                minDistance = distance;
                closestPoint = point;
            }
        });
        if (closestPoint) {
            const pointX = xScale(closestPoint['time'] - minTime);
            const pointY = yScale(closestPoint['parallel_workers']);

            tooltip.innerHTML = `
                Time: ${(closestPoint.time - minTime).toFixed(2)} s<br>
                Parallel Workers: ${closestPoint.parallel_workers}<br>
                Event: ${closestPoint.event}<br>
                Worker ID: ${closestPoint.worker_id}
            `;
            tooltip.style.visibility = 'visible';
            tooltip.style.top = (pointY + margin.top + container.getBoundingClientRect().top + window.scrollY + 5) + 'px';
            tooltip.style.left = (pointX + margin.left + container.getBoundingClientRect().left + window.scrollX + 5) + 'px';
        }
    });
    d3.select('#worker-connections').on("mouseout", function() {
        document.getElementById('vine-tooltip').style.visibility = 'hidden';
    });

}

function handleDownloadClick() {
    downloadSVG('worker-connections', 'worker_connections.svg');
}
function handleResetClick() {
    plotWorkerConnections();
}
window.parent.document.addEventListener('dataLoaded', function() {
    fillMgrDescription();
    fillFactoryDescription();
    plotWorkerConnections();

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);

    buttonReset.removeEventListener('click', handleResetClick);
    buttonReset.addEventListener('click', handleResetClick);
});