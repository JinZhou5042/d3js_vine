export function drawBarChart(workerInfo) {
    // prepare data
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
    // const svgWidth = 1200, svgHeight = 800;
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    // 假设SVG宽度为视窗宽度的80%，高度为视窗高度的60%
    const svgWidth = viewportWidth;
    const svgHeight = viewportWidth * 0.6;
    
    const padding = { top: 10, right: 10, bottom: 30, left: 100 };

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
    let timeExtent = [0, (last_task_end - first_task_dispatch)];
    
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
                   .attr("x", xScale(task[1] - first_task_dispatch)) // task[1]是开始时间
                   .attr("y", task[3].includes("library") ? 
                        yScale(`worker${slots.workerIndex}_library`) : 
                        yScale(`worker${slots.workerIndex}_slot${slots.slot}`))
                   .attr("width", xScale(task[2]) - xScale(task[1])) // task[2]是结束时间
                   .attr("height", yScale.bandwidth())
                   .attr("fill", task[3].includes("library") ? "#6dd1c4" : "steelblue")
                   .on("mouseover", function(event, d) {
                        d3.select(this).attr("fill", "orange"); // 高亮当前条形
                        const durationInSeconds = ((task[2] - task[1])).toFixed(4);
                        // 显示tooltip
                        d3.select("#barTooltip")
                            .style("visibility", "visible")
                            .style("left", (event.pageX + 10) + "px")
                            .style("top", (event.pageY + 10) + "px")
                            .html(`${(durationInSeconds)}s  ${yLabel}`);
                    })
                    .on("mouseout", function() {
                        d3.select(this).attr("fill", task[3].includes("library") ? "#6dd1c4" : "steelblue"); // 恢复原始颜色
                        // 隐藏tooltip
                        d3.select("#barTooltip").style("visibility", "hidden");
                    });
            });
        }
    });
    // 根据y轴的刻度数量调整字体大小
    const numberOfTicks = allSlots.length;
    let fontSize;

    // 假设一个基础字体大小和一个刻度数阈值来调整字体大小
    const baseFontSize = 13; // 基础字体大小
    const tickThreshold = 50; // 刻度数阈值

    if (numberOfTicks > tickThreshold) {
        // 如果刻度数量大于阈值，减小字体大小
        fontSize = baseFontSize * (tickThreshold / numberOfTicks);
    } else {
        // 如果刻度数量小于或等于阈值，使用基础字体大小
        fontSize = baseFontSize;
    }

    // 确保字体大小不会太小，设置一个最小字体大小
    fontSize = Math.max(fontSize, 1);

    // 根据字体大小设置刻度线的粗细
    let strokeWidth = fontSize / 20; // 假设基础线宽为1px时的字体大小为12px
    strokeWidth = `${Math.max(strokeWidth, 0.2)}px`; // 确保线宽至少为0.5px

    // add axis
    svg.append("g")
       .attr("transform", `translate(0,${svgHeight - padding.bottom})`)
       .call(d3.axisBottom(xScale));

    svg.append("g")
       .attr("transform", `translate(${padding.left},0)`)
       .call(d3.axisLeft(yScale).tickFormat(id => id))
       .selectAll(".tick text")
       .style("font-size", `${fontSize}px`); // 设置字体大小

    // 同时，设置刻度线的粗细
    svg.selectAll(".tick line") // 选择所有的刻度线
        .style("stroke-width", strokeWidth);
}



