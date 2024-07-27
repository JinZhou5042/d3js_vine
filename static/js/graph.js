import { downloadSVG, getTaskInnerHTML } from './tools.js';
import { createTable } from './draw_tables.js';

const errorTip = document.getElementById('dag-components-error-tip');
const dagSelector = document.getElementById('dag-id-selector');


const inputAnalyzeTask = document.getElementById('input-task-id-in-dag');
const buttonAnalyzeTask = document.getElementById('button-analyze-task-in-dag');

const inputAnalyzeFile = document.getElementById('input-filename-id-in-dag');
const buttonAnalyzeFile = document.getElementById('button-analyze-file-in-dag');

const buttonHighlightCriticalPath = document.getElementById('button-highlight-critical-path');
const buttonDownload = document.getElementById('button-download-dag');
const analyzeTaskDisplayDetails = document.getElementById('analyze-task-display-details');

const criticalPathSvgContainer = document.getElementById('critical-path-container');
const criticalPathSvgElement = d3.select('#critical-path-svg');

const highlightTaskColor = '#f69697';
const highlightcriticalInputFileColor = '#f69697';

const taskInformationDiv = document.getElementById('analyze-task-display-task-information');

var graphNodeMap = {};

const colorExecution = 'steelblue';
const colorHighlight = 'orange';

const tooltip = document.getElementById('vine-tooltip');

export async function plotDAGComponentByID(dagID) {
    try {
        if (typeof window.graphInfo === 'undefined') {
            return;
        }

        try {
            const svgContainer = d3.select('#dag-components');
            svgContainer.selectAll('*').remove();

            const svgContent = await d3.svg(`logs/${window.logName}/vine-logs/subgraph_${dagID}.svg`);
            svgContainer.node().appendChild(svgContent.documentElement);
            const insertedSVG = svgContainer.select('svg');

            insertedSVG
                .attr('preserveAspectRatio', 'xMidYMid meet');

            graphNodeMap = {};
            d3.select('#dag-components svg').selectAll('.node').each(function() {
                var nodeText = d3.select(this).select('text').text();
                graphNodeMap[nodeText] = d3.select(this);
            });

        } catch (error) {
            console.error(error);
        }
    } catch (error) {
        console.error('error when parsing ', error);
    }
}

dagSelector.addEventListener('change', async function() {
    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.toggle('report-button-active');
    }

    if (buttonAnalyzeTask.classList.contains('report-button-active')) {
        removeHighlightedTask();
        analyzeTaskDisplayDetails.style.display = 'none';
        buttonAnalyzeTask.classList.toggle('report-button-active');
    }
    
    const selectedDAGID = dagSelector.value;
    await plotDAGComponentByID(selectedDAGID);
});

function handleDownloadClick() {
    const selectedDAGID = dagSelector.value;
    downloadSVG('dag-components', 'subgraph_' + selectedDAGID + '.svg');
}
window.parent.document.addEventListener('dataLoaded', function() {
    if (typeof window.graphInfo === 'undefined') {
        errorTip.style.visibility = 'visible';
        return;
    }
    errorTip.style.visibility = 'hidden';

    // first remove the previous options
    dagSelector.innerHTML = '';
    // update the options
    const dagIDs = window.graphInfo.map(dag => dag.graph_id);
    dagIDs.forEach(dagID => {
        const option = document.createElement('option');
        option.value = dagID;
        option.text = `${dagID}`;
        dagSelector.appendChild(option);
    });

    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.toggle('report-button-active');
        criticalPathSvgContainer.style.display = 'none';
    }

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);
});

function removeHighlightedTask() {
    if (typeof window.highlitedTask === 'undefined') {
        return;
    }
    d3.select('#dag-components svg').selectAll('g').each(function() {
        var title = d3.select(this).select('title').text();
        if (+title === window.highlitedTask) {
            d3.select(this).select('ellipse').style('fill', window.previousTaskColor);
            window.previousTaskColor = 'white';
            window.highlitedTask = undefined;
        }
    });
}

function highlightTask(taskID) {
    taskID = +taskID;
    if (isNaN(taskID) || taskID === 0) {
        return;
    }
    if (!window.taskDone.some(d => +d.task_id === taskID)) {
        return;
    }
    removeHighlightedTask();
    d3.select('#dag-components svg').selectAll('g').each(function() {
        var title = d3.select(this).select('title').text();
        if (+title === taskID) {
            window.highlitedTask = taskID;
            window.previousTaskColor = d3.select(this).select('ellipse').style('fill');
            window.previousTaskColor = window.previousTaskColor === 'none' ? 'white' : window.previousTaskColor;
            d3.select(this).select('ellipse').style('fill', highlightTaskColor);
        }
    });
}

function getTaskInformation(taskID) {
    var taskData = window.taskDone.filter(function(d) {
        return +d.task_id === taskID;
    });
    taskData = taskData[0];
    let htmlContent = `Task ID: ${taskData.task_id}<br>
        Try Count: ${taskData.try_id}<br>
        Worker ID: ${taskData.worker_id}<br>
        Graph ID: ${taskData.graph_id}<br>
        Input Files: ${taskData.input_files}<br>
        Size of Input Files: ${taskData['size_input_files(MB)']}MB<br>
        Output Files: ${taskData.output_files}<br>
        Size of Output Files: ${taskData['size_output_files(MB)']}MB<br>
        Critical Input File: ${taskData.critical_input_file}<br>
        Wait Time for Critical Input File: ${taskData.critical_input_file_wait_time}<br>
        Category: ${taskData.category.replace(/^<|>$/g, '')}<br>
        When Ready: ${(taskData.when_ready - window.time_manager_start).toFixed(2)}s<br>
        When Running: ${(taskData.when_running - window.time_manager_start).toFixed(2)}s (When Ready + ${(taskData.when_running - taskData.when_ready).toFixed(2)}s)<br>
        When Start on Worker: ${(taskData.time_worker_start - window.time_manager_start).toFixed(2)}s (When Running + ${(taskData.time_worker_start - taskData.when_running).toFixed(2)}s)<br>
        When End on Worker: ${(taskData.time_worker_end - window.time_manager_start).toFixed(2)}s (When Start on Worker + ${(taskData.time_worker_end - taskData.time_worker_start).toFixed(2)}s)<br>
        When Waiting Retrieval: ${(taskData.when_waiting_retrieval - window.time_manager_start).toFixed(2)}s (When End on Worker + ${(taskData.when_waiting_retrieval - taskData.time_worker_end).toFixed(2)}s)<br>
        When Retrieved: ${(taskData.when_retrieved - window.time_manager_start).toFixed(2)}s (When Waiting Retrieval + ${(taskData.when_retrieved - taskData.when_waiting_retrieval).toFixed(2)}s)<br>
        When Done: ${(taskData.when_done - window.time_manager_start).toFixed(2)}s (When Retrieved + ${(taskData.when_done - taskData.when_retrieved).toFixed(2)}s)<br>
    `;
    if ('when_submitted_by_daskvine' in taskData && taskData.when_submitted_by_daskvine > 0) {
        htmlContent += `When DaskVine Submitted: ${taskData.when_submitted_by_daskvine - window.time_manager_start}s<br>
                        When DaskVine Received: ${taskData.when_received_by_daskvine - window.time_manager_start}s<br>`;
    }
    return htmlContent;
}

buttonAnalyzeTask.addEventListener('click', async function() {
    const inputValue = inputAnalyzeTask.value;
    const taskID = +inputValue;
    // invalid input
    if (inputValue === "" || isNaN(taskID) || !window.taskDone.some(d => d.task_id === taskID)) {
        removeHighlightedTask();
        analyzeTaskDisplayDetails.style.display = 'none';
        if (this.classList.contains('report-button-active')) {
            this.classList.toggle('report-button-active');
        }
        return;
    }
    if (taskID === window.highlitedTask) {
        return;
    }

    // a valid task id
    highlightTask(taskID);
    if (!this.classList.contains('report-button-active')) {
        this.classList.toggle('report-button-active');
        analyzeTaskDisplayDetails.style.display = 'block';
    }
    // show the information div
    var taskData = window.taskDone.filter(function(d) {
        return +d.task_id === taskID;
    });
    taskData = taskData[0];

    // update the information div
    taskInformationDiv.innerHTML = getTaskInformation(taskID);
    // update the input files table
    const inputFilesSet = new Set(taskData.input_files);
    const tableData = window.fileInfo.filter(file => inputFilesSet.has(file.filename))
        .map(file => {
            let fileWaitingTime = (taskData.time_worker_start - file['worker_holding'][0][1]).toFixed(2);
            let dependencyTime = file['producers'].length <= 1 ? "0" : (() => {
                for (let i = file['producers'].length - 1; i >= 0; i--) {
                    let producerTaskID = +file['producers'][i];
                    let producerTaskData = window.taskDone.find(d => +d.task_id === producerTaskID);
            
                    if (producerTaskData && producerTaskData.time_worker_end < taskData.time_worker_start) {
                        return (taskData.time_worker_start - producerTaskData.time_worker_end).toFixed(2);
                    }
                }
                return "0"; 
            })();                
            let formattedWorkerHolding = file['worker_holding'].map(tuple => {
                const worker_id = tuple[0];
                const time_stage_in = (tuple[1] - window.time_manager_start).toFixed(2);
                const time_stage_out = (tuple[2] - window.time_manager_start).toFixed(2);
                const lifetime = tuple[3].toFixed(2);
                return `worker${worker_id}: ${time_stage_in}s-${time_stage_out}s (${lifetime}s)`;
            }).join(', ');
            return {
                filename: file.filename,
                size: file['size(MB)'],
                fileWaitingTime: fileWaitingTime,
                dependencyTime: dependencyTime,
                producers: file.producers,
                consumers: file.consumers,
                workerHolding: formattedWorkerHolding
            };
        });

    var table = $('#task-input-files-table');
    if ($.fn.dataTable.isDataTable(table)) {
        table.DataTable().destroy();
    }
    var specificSettings = {
        "processing": false,
        "serverSide": false,
        "data": tableData,
        "columns": [
            { "data": 'filename' },
            { "data": 'size' },
            { "data": 'fileWaitingTime' },
            { "data": 'dependencyTime' },
            { "data": 'producers' },
            { "data": 'consumers' },
            { "data": 'workerHolding' }
        ],
    }
    var table = createTable('#task-input-files-table', specificSettings);
});



function displayCriticalPathInfo(criticalTasks) {
    criticalPathSvgContainer.style.display = 'block';

    const margin = {top: 30, right: 30, bottom: 20, left: 30};
    const svgWidth = criticalPathSvgContainer.clientWidth - margin.left - margin.right;
    const svgHeight = criticalPathSvgContainer.clientHeight - margin.top - margin.bottom;

    var graphCompletionTime;
    var graphStartTime;
    var graphEndTime;
    window.graphInfo.forEach(function(d) {
        if (d.graph_id === +dagSelector.value) {
            graphStartTime = d.time_start;
            graphEndTime = d.time_end;
            graphCompletionTime = d.time_completion;
        }
    });

    criticalPathSvgElement.selectAll('*').remove();
    const svg = criticalPathSvgElement
        .attr('viewBox', `0 0 ${criticalPathSvgContainer.clientWidth} ${criticalPathSvgContainer.clientHeight}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .append('g')
        .attr('transform', `translate(${margin.left}, ${margin.top})`);

    const xScale = d3.scaleLinear()
        .domain([0, graphEndTime - graphStartTime])
        .range([0, svgWidth]);
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

    const yScale = d3.scaleBand()
        .domain([0])
        .range([svgHeight, 0]);

    criticalTasks.forEach(function(taskID) {
        var taskData = window.taskDone.find(d => d.task_id === taskID);

        // time_worker_start ~ time_worker_end
        svg.append('rect')
            .attr('x', xScale(taskData.time_worker_start - graphStartTime))
            .attr('y', yScale(0))
            .attr('width', xScale(taskData.time_worker_end - graphStartTime) - xScale(taskData.time_worker_start - graphStartTime))
            .attr('height', yScale.bandwidth())
            .attr('fill', colorExecution)
            .on('mouseover', function(event, d) {
                d3.select(this).attr('fill', colorHighlight);
                tooltip.innerHTML = getTaskInnerHTML(taskData);
                tooltip.style.visibility = 'visible';
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mousemove', function(event) {
                tooltip.style.top = (event.pageY + 10) + 'px';
                tooltip.style.left = (event.pageX + 10) + 'px';
            })
            .on('mouseout', function() {
                d3.select(this).attr('fill', colorExecution);
                tooltip.style.visibility = 'hidden';
            });
    });
}

function hideCriticalPathInfo(criticalTasks) {
    criticalPathSvgContainer.style.display = 'none';

}

function removeHighlightedCriticalPath(criticalTasks) {
    criticalTasks.forEach(function(taskID) {
        // recover the highlighted task
        graphNodeMap[taskID].select('ellipse').style('fill', 'white');
        // recover the highlighted output file
        var outputFiles = window.taskDone.find(d => +d.task_id === taskID).output_files;
        if (outputFiles.length !== 1) {
            console.log(`Task ${taskID} has ${outputFiles.length} output files`);
            return;
        }
        var outputFile = outputFiles[0];
        var targetNode = graphNodeMap[outputFile];
        targetNode.select('polygon').style('fill', 'white');
    });

}

function highlightCriticalPath(criticalTasks) {
    criticalTasks.forEach(function(taskID) {
        // highlight the task
        graphNodeMap[`${taskID}`].select('ellipse').style('fill', 'orange');
        // find the output file
        var outputFiles = window.taskDone.find(d => +d.task_id === taskID).output_files;
        if (outputFiles.length !== 1) {
            console.log(`Task ${taskID} has ${outputFiles.length} output files`);
            return;
        }
        var outputFile = outputFiles[0];
        var targetNode = graphNodeMap[outputFile];
        targetNode.select('polygon').style('fill', 'orange');
    });
}

buttonHighlightCriticalPath.addEventListener('click', async function() {
    if (typeof window.graphInfo === 'undefined') {
        return;
    }

    // ensure that the analyze button is not active
    if (buttonAnalyzeTask.classList.contains('report-button-active')) {
        removeHighlightedTask();
        analyzeTaskDisplayDetails.style.display = 'none';
        buttonAnalyzeTask.classList.toggle('report-button-active');
    }

    var criticalTasks;
    window.graphInfo.forEach(function(d) {
        if (d.graph_id === +dagSelector.value) {
            criticalTasks = d.critical_tasks;
        }
    });

    this.classList.toggle('report-button-active');
    if (this.classList.contains('report-button-active')) {
        displayCriticalPathInfo(criticalTasks);
        highlightCriticalPath(criticalTasks);
    } else {
        hideCriticalPathInfo(criticalTasks);
        removeHighlightedCriticalPath(criticalTasks);
    }
});


window.parent.document.getElementById('log-selector').addEventListener('change', () => {
    analyzeTaskDisplayDetails.style.display = 'none';
    buttonAnalyzeTask.classList.remove('report-button-active');
    inputAnalyzeTask.value = '';
});