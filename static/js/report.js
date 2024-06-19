import { plotExecutionSummary } from './execution_summary.js';
import { setupZoomAndScroll, pathJoin } from './tools.js';
import { plotExecutionDetails } from './execution_details.js';
import { plotAccumulatedFiles } from './accumulated_files.js';
import { drawViolins } from './violinplot.js';
import { drawCoreLoads } from './cpu_load_plot.js';


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

async function fetchFile(filePath) {
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${filePath} (${response.statusText})`);
        }
        return await response.text();
    } catch (error) {
        console.error(error);
        throw error;
    }
}


window.addEventListener('load', function() {
    
    async function handleLogChange() {
        const logName = this.value;

        const taskDoneCSV = await fetchFile(`logs/${logName}/vine-logs/task_done.csv`);
        const taskFailedOnManagerCSV = await fetchFile(`logs/${logName}/vine-logs/task_failed_on_manager.csv`);
        const taskFailedOnWorkerCSV = await fetchFile(`logs/${logName}/vine-logs/task_failed_on_worker.csv`);

        const managerInfoJson = await fetchFile(`logs/${logName}/vine-logs/manager_info.json`);
        const libraryInfoCSV = await fetchFile(`logs/${logName}/vine-logs/library_info.csv`);
        const workerSummaryCSV = await fetchFile(`logs/${logName}/vine-logs/worker_summary.csv`);
        const workerDiskUpdateCSV = await fetchFile(`logs/${logName}/vine-logs/worker_disk_update.csv`);

        const manager_time_start = JSON.parse(managerInfoJson).time_start;
        const manager_time_end = JSON.parse(managerInfoJson).time_end;

        try {
            // draw histogram
            // plotExecutionSummary(taskCSV);
            // setupZoomAndScroll('#histogram', '#histogramContainer');
            // function execution details
            plotExecutionDetails(taskDoneCSV, taskFailedOnWorkerCSV, workerSummaryCSV, manager_time_start, manager_time_end);
            setupZoomAndScroll('#execution-details', '#execution-details-container');

            plotAccumulatedFiles(workerDiskUpdateCSV, workerSummaryCSV, manager_time_start, manager_time_end, false);
            setupZoomAndScroll('#worker-accumulated-cached-files', '#worker-accumulated-cached-files-container');
            
            document.getElementById('worker-accumulated-cached-files-display-mode').addEventListener('change', function() {
                const mode = this.value;
                const useDiskUtilization = (mode === 'diskUtilization');
                d3.select('#worker-accumulated-cached-files').selectAll('*').remove();
                plotAccumulatedFiles(workerDiskUpdateCSV, workerSummaryCSV, manager_time_start, manager_time_end, useDiskUtilization);
            });
            
            // draw violin plot
            // drawViolins(dataDir, workerInfo);
            // draw core load plot
            // drawCoreLoads(dataDir, workerInfo);
        } catch (error) {
            console.error('Error fetching data directory:', error);
        }
    }

    // Bind the change event listener to logSelector
    const logSelector = window.parent.document.getElementById('log-selector');
    logSelector.addEventListener('change', handleLogChange);

    // Initialize the report iframe if the logSelector has an initial value
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});
