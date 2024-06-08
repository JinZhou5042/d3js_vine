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


window.addEventListener('load', function() {
    
    async function handleLogChange() {
        const selectedLog = this.value;
        try {
            // wait for the data path to be fetched
            let dataDir = await getDataPath(`/input-path/${selectedLog}`);
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

    // Bind the change event listener to logSelector
    const logSelector = window.parent.document.getElementById('log-selector');
    logSelector.addEventListener('change', handleLogChange);

    // Initialize the report iframe if the logSelector has an initial value
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});