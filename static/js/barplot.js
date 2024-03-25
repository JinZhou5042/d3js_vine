export function drawBarChart(workerInfo) {
    // prepare data
    d3.select('#barchart').selectAll('*').remove();
    let allSlots = [];
    let workerIndex = 1;
    let workerIDs = Object.keys(workerInfo).filter(key => key.startsWith('worker'));

    workerIDs.forEach(workerID => {
        Object.keys(workerInfo[workerID].slots).forEach(slotID => {
            let slotTasks = workerInfo[workerID].slots[slotID];
            if (slotTasks[0][3].includes("library"))
                allSlots.push({ workerIndex: workerIndex, worker: workerID, slot: slotID, type: "library" });
            else
                allSlots.push({ workerIndex: workerIndex, worker: workerID, slot: slotID, type: "default" });
        });
        workerIndex += 1;
    });

    // create scales and draw slots
    const container = document.getElementById('barchartContainer');
    const containerWidth = container.clientWidth;
    const containerHeight = container.clientHeight;

    // suppose the width of the container is the width of the SVG
    const svgWidth = containerWidth;
    const svgHeight = containerHeight;
    
    const padding = { top: 10, right: 10, bottom: 30, left: 50 };

    const svg = d3.select('#barchart')
        .attr('viewBox', `0 0 ${svgWidth} ${svgHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet');

    // calculate time extent
    let first_task_dispatch = Infinity;
    let first_task_start = Infinity;
    let last_task_end = -Infinity;
    Object.values(workerInfo).forEach(worker => {
        if(worker.slots) {
            Object.values(worker.slots).forEach(slotTasks => {
                slotTasks.forEach(task => {
                    first_task_dispatch = Math.min(first_task_dispatch, task[0]);
                    first_task_start = Math.min(first_task_start, task[1]);
                    last_task_end = Math.max(last_task_end, task[2]);
                });
            });
        }
    });

    const xStartTime = first_task_start;
    let timeExtent = [0, (last_task_end - xStartTime)];
    
    // create scales
    const xScale = d3.scaleLinear()
                     .domain(timeExtent)
                     .range([padding.left, svgWidth - padding.right]);

    const yScale = d3.scaleBand()
                     .domain(allSlots.map(d => {
                        if (d.type === "library")
                            return `worker${d.workerIndex}_library`;
                        else
                            return `worker${d.workerIndex}_slot${d.slot}`;
                    }))
                    .range([svgHeight - padding.bottom, padding.top])
                    .padding(0.1);
    
    /*
    allSlots = [
        0: { slot: "1", worker: "worker-41aaf3xxx", workerIndex: 1, type: "default"},
        1: { slot: "2", worker: "worker-41aaf3xxx", workerIndex: 1, type: "default"},
        2: { slot: "3", worker: "worker-41aaf3xxx", workerIndex: 1, type: "default"},
        3: { slot: "4", worker: "worker-41aaf3xxx", workerIndex: 1, type: "default"},
        4: { slot: "5", worker: "worker-41aaf3xxx", workerIndex: 1, type: "library"},     // library slot

        5: { slot: "1", worker: "worker-7e5f4b5fbf3490d761992e7ebb404a56", workerIndex: 2},
        ......
    ]
    */
    // draw slots
    allSlots.forEach(slots => {
        const workerData = workerInfo[slots.worker];
        if(workerData && workerData.slots && workerData.slots[slots.slot]) {
            const slotTasks = workerData.slots[slots.slot];
            slotTasks.forEach(task => {
                const yLabel = task[3].includes("library") ? 
                    `worker${slots.workerIndex}(library)` : 
                    `worker${slots.workerIndex}(slot${slots.slot})`;
                svg.append("rect")
                   .attr("x", xScale(task[1] - xStartTime)) // task[1] represents the start time
                   .attr("y", task[3].includes("library") ? 
                        yScale(`worker${slots.workerIndex}_library`) : 
                        yScale(`worker${slots.workerIndex}_slot${slots.slot}`))
                   .attr("width", xScale(task[2]) - xScale(task[1])) // task[2] represents the end time
                   .attr("height", yScale.bandwidth())
                   .attr("fill", task[3].includes("library") ? "#6dd1c4" : "steelblue")
                   .on("mouseover", function(event, d) {
                        d3.select(this).attr("fill", "orange"); // highlight the bar
                        const durationInSeconds = ((task[2] - task[1])).toFixed(4);
                        // show tooltip
                        d3.select("#barTooltip")
                            .style("visibility", "visible")
                            .style("left", (event.pageX + 10) + "px")
                            .style("top", (event.pageY + 10) + "px")
                            .html(`${(durationInSeconds)}s  ${yLabel}`);
                    })
                    .on("mouseout", function() {
                        d3.select(this).attr("fill", task[3].includes("library") ? "#6dd1c4" : "steelblue"); // restore the color
                        // hide tooltip
                        d3.select("#barTooltip").style("visibility", "hidden");
                    });
            });
        }
    });
    // adjust font size and stroke width based on the number of ticks
    const numberOfTicks = allSlots.length;
    let fontSize;

    // set the base font size and the threshold for the number of ticks
    const baseFontSize = 10; // base font size
    const tickThreshold = 50; // threshold for the number of ticks

    if (numberOfTicks > tickThreshold) {
        // if the number of ticks exceeds the threshold, adjust the font size
        fontSize = baseFontSize * (tickThreshold / numberOfTicks);
    } else {
        // if the number of ticks does not exceed the threshold, use the base font size
        fontSize = baseFontSize;
    }

    // make sure the font size is at least 0.5 times the base font size
    fontSize = Math.max(fontSize, 0.5);

    // set the stroke width based on the font size
    let strokeWidth = fontSize / 20; // suppose the stroke width is 1/20 of the font size
    strokeWidth = `${Math.max(strokeWidth, 0.2)}px`; // ensure the stroke width is at least 0.2 pixels

    // add axis
    svg.append("g")
       .attr("transform", `translate(0,${svgHeight - padding.bottom})`)
       .call(d3.axisBottom(xScale));

    svg.append("g")
       .attr("transform", `translate(${padding.left},0)`)
       .call(d3.axisLeft(yScale).tickFormat(id => id))
       .selectAll(".tick text")
       .style("font-size", `${fontSize}px`); 

    // set the font size and stroke width for the axis labels
    svg.selectAll(".tick line") // select all the tick lines
        .style("stroke-width", strokeWidth);
}



