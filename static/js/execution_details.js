export function plotExecutionDetails(taskInfoCSV) {
    const taskInfo = d3.csvParse(taskInfoCSV);

    const container = document.getElementById('execution-details-container');
    const margin = {top: 20, right: 20, bottom: 40, left: 40};
    const svgWidth = container.clientWidth - margin.left - margin.right;
    const svgHeight = container.clientHeight - margin.top - margin.bottom;

    const svg = d3.select('#execution-details')
        .attr('viewBox', `0 0 ${container.clientWidth} ${container.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const minTime = d3.min(taskInfo, d => +d.time_worker_start);
    const xScale = d3.scaleLinear()
        .domain([0, d3.max(taskInfo, d => +d.time_worker_end - minTime)])
        .range([0, svgWidth]);

    const sortedTaskInfo = taskInfo.slice().sort((a, b) => b.worker_slot - a.worker_slot);
    const yScale = d3.scaleBand()
        .domain(sortedTaskInfo.map(d => d.worker_id + '-' + d.worker_slot))
        .range([0, svgHeight])
        .padding(0.1);

    svg.selectAll('.task-rect')
        .data(taskInfo)
        .enter()
        .append('rect')
        .attr('class', 'task-rect')
        .attr('x', d => xScale(+d.time_worker_start - minTime))
        .attr('y', d => yScale(d.worker_id + '-' + d.worker_slot))
        .attr('width', d => xScale(+d.time_worker_end) - xScale(+d.time_worker_start))
        .attr('height', yScale.bandwidth())
        .attr('fill', 'steelblue');

    const xAxis = d3.axisBottom(xScale)
        .tickSizeOuter(0)
        .tickValues(xScale.ticks().concat(xScale.domain()[1]))
        .tickFormat(d3.format(".1f"));
    svg.append('g')
        .attr('transform', `translate(0, ${svgHeight})`)
        .call(xAxis);

    const yAxis = d3.axisLeft(yScale)
        .tickSizeOuter(0)
        .tickFormat(d => `${d.split('-')[0]}-${d.split('-')[1]}`)
        .tickValues(yScale.domain().filter((d, i) => i % 8 === 0));
    svg.append('g')
        .call(yAxis);

}
