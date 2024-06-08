
import { plotExecutionSummary } from './execution_summary.js';
import { plotExecutionDetails } from './execution_details.js';
import { setupZoomAndScroll, pathJoin } from './tools.js';
import { drawViolins } from './violinplot.js';
import { drawCoreLoads } from './cpu_load_plot.js';


/*
document.addEventListener('load', function() {
    let dataDir = '';
    const logSelector = document.getElementById('log-selector');
    
    // 定义一个函数，用于处理选项改变时的逻辑
    async function handleLogChange() {
        const selectedLog = this.value;
        try {
            // wait for the data path to be fetched
            dataDir = await getDataPath(`/input-path/${selectedLog}`);
            // load log description
            loadLogDescription(dataDir);
            // load worker task data
            let workerInfo = await d3.json(pathJoin([dataDir, 'worker_tasks.json']));
            // draw histogram
            plotExecutionSummary(workerInfo);
            setupZoomAndScroll('#histogram', '#histogramContainer');
            // function execution details
            plotExecutionDetails(workerInfo);
            setupZoomAndScroll('#execution-details', '#execution-details-container');
            // draw violin plot
            drawViolins(dataDir, workerInfo);
            // draw core load plot
            drawCoreLoads(dataDir, workerInfo);
        } catch (error) {
            console.error('Error fetching data directory:', error);
        }
    }
    
    // 为logSelector绑定change事件监听器
    logSelector.addEventListener('change', handleLogChange);

    // 如果logSelector有初始值，则手动触发change事件
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});
*/

/*
async function loadLogDescription(dataPath) {
    try {
        let appInfoFilename = dataPath + (dataPath.endsWith("/") ? "" : "/") + 'app_info.json';
        // load JSON data
        const response = await fetch(appInfoFilename);
        const data = await response.json();
        // get div element
        const div = document.querySelector('#description');
        // show data in pre tag
        const formattedData = JSON.stringify(data, null, 4);
        div.innerHTML = `<pre>${formattedData}</pre>`;
    } catch (error) {
        console.error("Could not load JSON data", error);
    }
}
*/

document.getElementById('logSelector').addEventListener('change', function() {
    const selectedLog = this.value;
    fetch(`/input-path/${selectedLog}`)
        .then(response => response.json())
        .then(data => {
            if (data.inputPath) {
                // update image sources
                console.log(data.inputPath);
                document.getElementById('allWorkersSummaryViolinImg').src = `${data.inputPath}/all_workers_summary_violin.svg`;
                document.getElementById('overallAvgCoreLoadImg').src = `${data.inputPath}/overall_core_load.svg`;
                document.getElementById('performanceTransferImg').src = `${data.inputPath}/performance.transfer.svg`;
                document.getElementById('performanceTaskImg').src = `${data.inputPath}/performance.tasks.svg`;
                document.getElementById('performanceWorkersImg').src = `${data.inputPath}/performance.workers.svg`;
                document.getElementById('performanceTasksAccumImg').src = `${data.inputPath}/performance.tasks-accum.svg`;
                document.getElementById('performanceWorkersAccumImg').src = `${data.inputPath}/performance.workers-accum.svg`;
                document.getElementById('performanceTasksCapacitiesImg').src = `${data.inputPath}/performance.tasks-capacities.svg`;
                document.getElementById('performanceTimeManagerImg').src = `${data.inputPath}/performance.time-manager.svg`;
                document.getElementById('performanceTimeWorkersImg').src = `${data.inputPath}/performance.time-workers.svg`;
                document.getElementById('performanceTimesStackedImg').src = `${data.inputPath}/performance.times-stacked.svg`;
                document.getElementById('performanceWorkersDiskImg').src = `${data.inputPath}/performance.workers-disk.svg`;
            }
        })
        .catch(error => console.error('Error:', error));
});

async function getDataPath(path) {
    try {
        const response = await fetch(path);
        const data = await response.json();
        if (data.inputPath) {
            return data.inputPath;
        }
    } catch (error) {
        console.error('Error updating data path:', error);
    }
}
