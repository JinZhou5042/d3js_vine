
import { plotExecutionSummary } from './execution_summary.js';
import { plotExecutionDetails } from './execution_details.js';
import { setupZoomAndScroll, pathJoin } from './tools.js';
import { drawViolins } from './violinplot.js';
import { drawCoreLoads } from './cpu_load_plot.js';


document.getElementById('logSelector').addEventListener('change', function() {
    const selectedLog = this.value;
    fetch(`/input-path/${selectedLog}`)
        .then(response => response.json())
        .then(data => {
            if (data.inputPath) {
                // update image sources
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
