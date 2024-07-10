import { sortTable } from './tools.js';

export async function plotDAGComponentByID(dagID) {
    try {
        if (typeof window.generalStatisticsDAG === 'undefined') {
            document.getElementById('dag-components-tip').style.visibility = 'visible';
            return;
        }
        const generalStatisticsDAG = window.generalStatisticsDAG;
        const dag = generalStatisticsDAG.find(d => d.graph_id === dagID.toString());

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

                // highlight the selected row
                // rows = document.querySelectorAll('#dag-table tbody tr');
                // rows.forEach(row => {
                //     if (+row.graph_id === +dagID) {
                //         row.style.backgroundColor = '#f2f2f2';
                //     }
                // });

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

document.getElementById('dag-id-selector').addEventListener('change', async function() {
    const buttonHighlightCriticalPath = document.getElementById('button-highlight-critical-path');
    if (buttonHighlightCriticalPath.classList.contains('report-button-active')) {
        buttonHighlightCriticalPath.classList.toggle('report-button-active');
    }
    const buttonAnalyzeTaskInDAG = document.getElementById('button-analyze-task-in-dag');
    if (buttonAnalyzeTaskInDAG.classList.contains('report-button-active')) {
        buttonAnalyzeTaskInDAG.classList.toggle('report-button-active');
    }
    // hidden the info div
    const taskInfoContainer = document.getElementById('task-info-container');
    taskInfoContainer.style.display = 'none';
    
    const selectedDAGID = document.getElementById('dag-id-selector').value;
    await plotDAGComponentByID(selectedDAGID);
});


window.parent.document.addEventListener('dataLoaded', function() {
    if (typeof window.generalStatisticsDAG === 'undefined') {
        return;
    }
    const selectDAG = document.getElementById('dag-id-selector');
    // first remove the previous options
    selectDAG.innerHTML = '';
    
    // update the options
    const dagIDs = window.generalStatisticsDAG.map(dag => dag.graph_id);
    dagIDs.forEach(dagID => {
        const option = document.createElement('option');
        option.value = dagID;
        option.text = `${dagID}`;
        selectDAG.appendChild(option);
    });
});

document.getElementById('button-analyze-task-in-dag').addEventListener('click', async function() {
    let taskID = document.getElementById('input-task-id-in-dag').value;
    const highlightTaskColor = '#f69697';
    const highlightcriticalInputFileColor = '#ffcc80';

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
        const infoContainer = document.getElementById('task-info-container');
        infoContainer.style.display = 'flex';
        // update the left side
        const infoDiv = document.getElementById('task-info');
        infoDiv.innerHTML = `Task ID: ${taskData.task_id}<br>
            Try Count: ${taskData.try_id}<br>
            Worker ID: ${taskData.worker_id}<br>
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
        // update the right side
        const inputFileInfoDiv = document.getElementById('task-input-file-info');
        taskData.input_files = taskData.input_files.replace(/'/g, '"');
        const inputFilesSet = new Set(JSON.parse(taskData.input_files));
        const tbody = d3.select('#task-input-file-info-table').select('tbody');
        tbody.selectAll('tr').remove();
        const rows = tbody.selectAll('tr')
            .data(window.fileInfo.filter(file => inputFilesSet.has(file.filename)))
            .enter()
            .append('tr');

        rows.append('td').text(d => d.filename);
        rows.append('td').text(d => d['size(MB)']);
        rows.append('td').text(d => {
            let workerHolding = JSON.parse(d['worker_holding']);
            let fileWaitingTime = taskData.time_worker_start - workerHolding[0][1];
            return fileWaitingTime.toFixed(2);
        });
        rows.append('td').text(d => {
            let producers = JSON.parse(d['producers']);
            if (producers.length <= 1) {
                return 0;
            } else {
                for (let i = producers.length - 1; i > 0; i--) { 
                    let producerTaskID = +producers[i];
                    let producerTaskData = window.taskDone.filter(function(d) {
                        return +d.task_id === producerTaskID;
                    });
                    producerTaskData = producerTaskData[0];
                    let producerTaskEndTime = producerTaskData.time_worker_end;
                    if (producerTaskEndTime < taskData.time_worker_start) {
                        return (taskData.time_worker_start - producerTaskEndTime).toFixed(2);
                    }
                }
            }
        });
        rows.append('td').text(d => d['producers']);
        rows.append('td').text(d => d['consumers']);
        rows.append('td').text(d => {
            let workerHolding = JSON.parse(d['worker_holding']);
            workerHolding = workerHolding.map(tuple => {
                const worker_id = tuple[0];
                const time_stage_in = (tuple[1] - window.time_manager_start).toFixed(2);
                const time_stage_out = (tuple[2] - window.time_manager_start).toFixed(2);
                const lifetime = tuple[3].toFixed(2);

                return `worker${worker_id}: ${time_stage_in}s-${time_stage_out}s (${lifetime}s)`;

            });
            const resultString = workerHolding.join(', ');
            return resultString;
          });

        // highlight the critical input file

        const criticalInputFile = taskData.critical_input_file;
        
        const svgElement = d3.select('#dag-components svg');

        svgElement.selectAll('g').each(function() {
            var title = d3.select(this).select('title').text();
            if (title === criticalInputFile) {
                window.highlitedInputFile = criticalInputFile;
                window.previousFileColor = d3.select(this).select('polygon').style('fill');
                d3.select(this).select('polygon').style('fill', highlightcriticalInputFileColor);
            }
            // we want to highlight the task
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
            const taskInfoContainer = document.getElementById('task-info-container');
            taskInfoContainer.style.display = 'none';

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

    if (typeof window.generalStatisticsDAG === 'undefined') {
        return;
    }

    // ensure that the analyze button is not active
    const buttonAnalyzeTaskInDAG = document.getElementById('button-analyze-task-in-dag');
    if (buttonAnalyzeTaskInDAG.classList.contains('report-button-active')) {
        buttonAnalyzeTaskInDAG.classList.toggle('report-button-active');
        window.highlitedTask = undefined;
    }

    this.classList.toggle('report-button-active');
    const thisButton = this;
    
    const dagID = +(document.getElementById('dag-id-selector').value);

    const dagInfoArray = window.generalStatisticsDAG.filter(function(d) {
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
        // ensure we only parse edges with 2 elements
        if (ids.length === 2) {
            var sourceNode = nodeMap[ids[0]];
            var targetNode = nodeMap[ids[1]];
            // if +ids[0] is integer, then it is a task
            if (Number.isInteger(+ids[0])) {
                sourceNode.select('ellipse').style('fill', 'white');
            }
            
            // task -> file
            if (criticalTasks.includes(+ids[0])) {
                if (thisButton.classList.contains('report-button-active')) {
                    // highlight all the nodes
                    sourceNode.select('ellipse').style('fill', 'orange');
                    targetNode.select('polygon').style('fill', 'orange');
                    criticalPath.push(`${ids[0]}--->${ids[1]}`);
                } else {
                    sourceNode.select('ellipse').style('fill', 'white');
                    targetNode.select('polygon').style('fill', 'white');
                }
            }
        }
    });
    
    if (thisButton.classList.contains('report-button-active')) {
        var sortedCriticalPath = [];
        // sort the critical path 
        criticalTasks.forEach(taskId => {
            var taskRegex = new RegExp(`^${taskId}(--->|$)`);
            criticalPath.forEach(path => {
                if (taskRegex.test(path)) {
                    sortedCriticalPath.push(path);
                }
            });
        });
        var criticalPathString = sortedCriticalPath.join('--->');
        // show the critical path if the button is active
        var criticalPathInfoDiv = document.getElementById('critical-path-info');
        criticalPathInfoDiv.style.display = 'block';
        criticalPathInfoDiv.innerHTML = `Critical Path: ${criticalPathString}`;
    } else {
        document.getElementById('critical-path-info').style.display = 'none';
    }
});

const taskInputFileInfoTable = document.getElementById('task-input-file-info-table');
const buttons = document.querySelectorAll('#task-input-file-info-table th button');
buttons.forEach(button => {
    button.addEventListener('click', () => {
        const columnIndex = parseInt(button.getAttribute('data-index'));
        let sortOrder = parseInt(button.getAttribute('data-order'));
        if (sortOrder === 0) {
            sortOrder = 1;
        } else {
            sortOrder = -sortOrder;
        }
        sortTable(taskInputFileInfoTable, columnIndex, sortOrder);
        button.setAttribute('data-order', sortOrder);
    });
});
