import { downloadSVG } from './tools.js';
import { createTable } from './draw_tables.js';

const errorTip = document.getElementById('dag-components-error-tip');
const dagSelector = document.getElementById('dag-id-selector');

const tableTextFontSize = '3.5rem';
const buttonAnalyzeTaskInDAG = document.getElementById('button-analyze-task-in-dag');
const buttonHighlightCriticalPath = document.getElementById('button-highlight-critical-path');
const buttonDownload = document.getElementById('button-download-dag');
const inputAnalyzeTaskID = document.getElementById('input-task-id-in-dag');
const analyzeTaskDisplayDetails = document.getElementById('analyze-task-display-details');
const criticalPathInfoDiv = document.getElementById('critical-path-info');

const highlightTaskColor = '#f69697';
const highlightcriticalInputFileColor = '#f69697';

export async function plotDAGComponentByID(dagID) {
    try {
        if (typeof window.graphInfo === 'undefined') {
            return;
        }
        const graphInfo = window.graphInfo;
        const dag = graphInfo.find(d => d.graph_id === dagID.toString());

        if (dag) {
            try {
                // let rows = document.querySelectorAll('#dag-table tbody tr');
                // rows.forEach(row => {
                //     row.style.backgroundColor = 'white';
                // });
                const svgElement = d3.select('#dag-components');
                svgElement.selectAll('*').remove();
            
                const svgContent = await d3.svg(`logs/${window.logName}/vine-logs/subgraph_${dagID}.svg`);
                svgElement.node().appendChild(svgContent.documentElement);
                const insertedSVG = svgElement.select('svg');
        
                insertedSVG
                    .attr('preserveAspectRatio', 'xMidYMid meet');

            } catch (error) {
                console.error(error);
            }
        } else {
            console.error(`didn't find dag ${dagID}`);
        }
    } catch (error) {
        console.error('error when parsing ', error);
    }
}

dagSelector.addEventListener('change', async function() {
    const buttonHighlightCriticalPath = document.getElementById('button-highlight-critical-path');
    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.toggle('report-button-active');
    }

    if (buttonAnalyzeTaskInDAG.classList.contains('report-button-active')) {
        buttonAnalyzeTaskInDAG.classList.toggle('report-button-active');
    }
    // hidden the info div
    analyzeTaskDisplayDetails.style.display = 'none';
    criticalPathInfoDiv.style.display = 'none';
    
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
        criticalPathInfoDiv.style.display = 'none';
    }

    buttonDownload.removeEventListener('click', handleDownloadClick); 
    buttonDownload.addEventListener('click', handleDownloadClick);
});

buttonAnalyzeTaskInDAG.addEventListener('click', async function() {
    let taskID = inputAnalyzeTaskID.value;

    // a valid task id
    if (taskID && +taskID !== window.highlitedTask && window.taskDone.some(d => +d.task_id === +taskID)) {
        taskID = +taskID;
        if (!this.classList.contains('report-button-active')) {
            this.classList.toggle('report-button-active');
        }
        // show the information div
        var taskData = window.taskDone.filter(function(d) {
            return +d.task_id === +taskID;
        });
        taskData = taskData[0];
        analyzeTaskDisplayDetails.style.display = 'block';

        // update the left side
        const infoDiv = document.getElementById('analyze-task-display-task-information');
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
        infoDiv.innerHTML = htmlContent;

        // update the right side
        if (typeof taskData.input_files === 'string') {
            taskData.input_files = taskData.input_files.replace(/'/g, '"');
            taskData.input_files = JSON.parse(taskData.input_files);
        } else {
            // already an array
        }

        const inputFilesSet = new Set(taskData.input_files);
        const tableData = window.fileInfo.filter(file => inputFilesSet.has(file.filename))
            .map(file => {
                let workerHolding = JSON.parse(file['worker_holding']);
                let producers = JSON.parse(file['producers']);
                let fileWaitingTime = (taskData.time_worker_start - workerHolding[0][1]).toFixed(2);
                let dependencyTime = producers.length <= 1 ? "0" : (() => {
                    for (let i = producers.length - 1; i >= 0; i--) {
                        let producerTaskID = +producers[i];
                        let producerTaskData = window.taskDone.find(d => +d.task_id === producerTaskID);
                
                        if (producerTaskData && producerTaskData.time_worker_end < taskData.time_worker_start) {
                            return (taskData.time_worker_start - producerTaskData.time_worker_end).toFixed(2);
                        }
                    }
                    return "0"; 
                })();                
                let formattedWorkerHolding = workerHolding.map(tuple => {
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

        // highlight the critical input file
        const criticalInputFile = taskData.critical_input_file;
        const svgElement = d3.select('#dag-components svg');

        // don't highlight the critical input file at the moment
        const highlightCriticalFile = false;

        svgElement.selectAll('g').each(function() {
            var title = d3.select(this).select('title').text();
            // highlight the critical input file
            if (highlightCriticalFile) {
                if (title === criticalInputFile) {
                    window.highlitedInputFile = criticalInputFile;
                    window.previousFileColor = d3.select(this).select('polygon').style('fill');
                    d3.select(this).select('polygon').style('fill', highlightcriticalInputFileColor);
                }
            }
            // highlight the task
            if (+title === taskID) {
                // there is no task highlighted
                if (typeof window.highlitedTask === 'undefined') {
                    window.highlitedTask = taskID;
                    window.previousTaskColor = d3.select(this).select('ellipse').style('fill');
                    window.previousTaskColor = window.previousTaskColor === 'none' ? 'white' : window.previousTaskColor;
                    d3.select(this).select('ellipse').style('fill', highlightTaskColor);
                } else {
                    // there is a task highlighted, first remove the previous one
                    svgElement.selectAll('g').each(function() {
                        var title = d3.select(this).select('title').text();
                        if (highlightCriticalFile) {
                            if (title === window.highlitedInputFile) {
                                d3.select(this).select('polygon').style('fill', window.previousFileColor);
                                window.previousFileColor = 'white';
                                window.highlitedInputFile = undefined;
                            }
                        }
                        if (+title === window.highlitedTask) {
                            d3.select(this).select('ellipse').style('fill', window.previousTaskColor);
                            window.previousTaskColor = 'white';
                            window.highlitedTask = undefined;
                        }
                    });
                    // then highlight the new one
                    window.highlitedTask = taskID;
                    window.previousTaskColor = d3.select(this).select('ellipse').style('fill');
                    window.previousTaskColor = window.previousTaskColor === 'none' ? 'white' : window.previousTaskColor;
                    d3.select(this).select('ellipse').style('fill', highlightTaskColor);
                }

            }
        });
    } else {
        if (this.classList.contains('report-button-active')) {
            this.classList.toggle('report-button-active');
            // hidden all the info divs
            analyzeTaskDisplayDetails.style.display = 'none';

            const svgElement = d3.select('#dag-components svg');
            svgElement.selectAll('g').each(function() {
                var title = d3.select(this).select('title').text();
                if (title === window.highlitedInputFile) {
                    d3.select(this).select('polygon').style('fill', window.previousFileColor);
                    window.previousFileColor = 'white';
                    window.highlitedInputFile = undefined;
                }
                if (+title === window.highlitedTask) {
                    d3.select(this).select('ellipse').style('fill', window.previousTaskColor);
                    window.previousTaskColor = 'white';
                    window.highlitedTask = undefined;
                }
            });
        }
    }
});



document.getElementById('button-highlight-critical-path').addEventListener('click', async function() {

    if (typeof window.graphInfo === 'undefined') {
        return;
    }

    // ensure that the analyze button is not active
    if (buttonAnalyzeTaskInDAG.classList.contains('report-button-active')) {
        buttonAnalyzeTaskInDAG.classList.toggle('report-button-active');
        window.highlitedTask = undefined;
    }

    this.classList.toggle('report-button-active');
    const dagID = +(dagSelector.value);

    const dagInfoArray = window.graphInfo.filter(function(d) {
        return +d.graph_id === dagID;
    });

    if (dagInfoArray.length === 0) {
        return;
    } else if (dagInfoArray.length > 1) {
        console.error('multiple dags with the same ID');
        return;
    }
    const dagInfo = dagInfoArray[0];
    const criticalTasks = JSON.parse(dagInfo.critical_tasks).map(d => +d);
    
    const svgElement = d3.select('#dag-components svg');
    var nodeMap = {};
    svgElement.selectAll('.node').each(function() {
        var nodeIdText = d3.select(this).select('text').text();
        nodeMap[nodeIdText] = d3.select(this);
    });
    var criticalPath = [];

    svgElement.selectAll('g').each(function() {
        var titleText = d3.select(this).select('title').text();
        // find one of the critical tasks
        var ids = titleText.split("->").map(s => s.trim());

        // ensure that we only parse edges with 2 elements
        if (ids.length === 2) {
            var sourceNode = nodeMap[ids[0]];
            var targetNode = nodeMap[ids[1]];
            // if +ids[0] is integer, then it is a task
            if (Number.isInteger(+ids[0]) && !Number.isInteger(+ids[1])) {
                var sourceTaskID = +ids[0];
                var targetFileID = ids[1];
                if (criticalTasks.includes(sourceTaskID)) {
                    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
                        // highlight all the nodes
                        sourceNode.select('ellipse').style('fill', 'orange');
                        targetNode.select('polygon').style('fill', 'orange');
                        criticalPath.push(`${sourceTaskID}--->${targetFileID}`);
                    } else {
                        sourceNode.select('ellipse').style('fill', 'white');
                        targetNode.select('polygon').style('fill', 'white');
                    }
                }
            } else if (Number.isInteger(+ids[0]) && Number.isInteger(+ids[1])) {
                var sourceTaskID = +ids[0];
                var targetTaskID = +ids[1];
                if (criticalTasks.includes(sourceTaskID) && criticalTasks.includes(targetTaskID)) {
                    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
                        sourceNode.select('ellipse').style('fill', 'orange');
                        targetNode.select('ellipse').style('fill', 'orange');
                        criticalPath.push(`${sourceTaskID}--->${targetTaskID}`);
                    } else {
                        sourceNode.select('ellipse').style('fill', 'white');
                        targetNode.select('ellipse').style('fill', 'white');
                    }
                }
            
            }
        }
    });

    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        var sortedCriticalPath = [];
        // sort the critical path
        const format1 = /^\d+--->[a-zA-Z-]+$/;   // task -> file -> task -> file
        const format2 = /^\d+--->\d+$/;          // task -> task -> task
        if (format1.test(criticalPath[0])) {
            criticalTasks.forEach(taskId => {
                var taskRegex = new RegExp(`^${taskId}(--->|$)`);
                criticalPath.forEach(edge => {
                    if (taskRegex.test(edge)) {
                        sortedCriticalPath.push(edge);
                    }
                });
            });
            var criticalPathString = sortedCriticalPath.join('--->');
        } else if (format2.test(criticalPath[0])) {
            var criticalPathString = criticalTasks.join('--->');
        } else {
            console.error('unrecognized format');
        }
        // show the critical path if the button is active
        criticalPathInfoDiv.style.display = 'block';
        criticalPathInfoDiv.innerHTML = `Critical Path: ${criticalPathString}`;
    } else {
        document.getElementById('critical-path-info').style.display = 'none';
    }
});


window.parent.document.getElementById('log-selector').addEventListener('change', () => {
    analyzeTaskDisplayDetails.style.display = 'none';
    buttonAnalyzeTaskInDAG.classList.remove('report-button-active');
    inputAnalyzeTaskID.value = '';
});