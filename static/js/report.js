import { plotExecutionSummary } from './execution_summary.js';
import { setupZoomAndScroll, pathJoin } from './tools.js';
import { plotExecutionDetails } from './execution_details.js';
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
        const taskInfoCSV = await fetchFile(`logs/${logName}/vine-logs/task_info.csv`);
        const libraryInfoCSV = await fetchFile(`logs/${logName}/vine-logs/library_info.csv`);
        const workerInfoJson = await fetchFile(`logs/${logName}/vine-logs/worker_info.json`);

        try {
            // draw histogram
            // plotExecutionSummary(taskInfoCSV);
            // setupZoomAndScroll('#histogram', '#histogramContainer');
            // function execution details
            plotExecutionDetails(taskInfoCSV, workerInfoJson);
            setupZoomAndScroll('#execution-details', '#execution-details-container');
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