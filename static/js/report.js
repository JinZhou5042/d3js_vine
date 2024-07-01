import { fillGeneralStatistics } from './general_statistics.js';
import { setupZoomAndScroll, fetchFile } from './tools.js';
import { plotExecutionDetails } from './execution_details.js';
import { plotWorkerDiskUsage } from './worker_disk_usage.js';
import { plotDAGComponentByID } from './dag.js';



window.addEventListener('load', function() {
    
    async function handleLogChange() {
        const logName = this.value;

        const generalStatisticsManagerCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_manager.csv`);
        const generalStatisticsTaskCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_task.csv`);
        const generalStatisticsWorkerCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_worker.csv`);
        const generalStatisticsFileCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_file.csv`);
        const generalStatisticsDAGCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_dag.csv`);

        const taskDoneCSV = await fetchFile(`logs/${logName}/vine-logs/task_done.csv`);
        const taskFailedOnManagerCSV = await fetchFile(`logs/${logName}/vine-logs/task_failed_on_manager.csv`);
        const taskFailedOnWorkerCSV = await fetchFile(`logs/${logName}/vine-logs/task_failed_on_worker.csv`);

        const workerSummaryCSV = await fetchFile(`logs/${logName}/vine-logs/worker_summary.csv`);
        const workerDiskUpdateCSV = await fetchFile(`logs/${logName}/vine-logs/worker_disk_usage.csv`);

        const manager_time_start = d3.csvParse(generalStatisticsManagerCSV)[0].time_start;
        const manager_time_end = d3.csvParse(generalStatisticsManagerCSV)[0].time_end;

        try {

            fillGeneralStatistics(generalStatisticsManagerCSV, generalStatisticsTaskCSV, generalStatisticsWorkerCSV, generalStatisticsFileCSV, generalStatisticsDAGCSV);

            plotExecutionDetails(taskDoneCSV, taskFailedOnWorkerCSV, workerSummaryCSV, manager_time_start, manager_time_end);
            setupZoomAndScroll('#execution-details', '#execution-details-container');

            plotDAGComponentByID(1, generalStatisticsDAGCSV, logName);
        
            plotWorkerDiskUsage(workerDiskUpdateCSV, workerSummaryCSV, manager_time_start, manager_time_end, false);
            setupZoomAndScroll('#per-worker-disk-usage', '#per-worker-disk-usage-container');
            document.getElementById('display-worker-disk-usage-by-percentage').addEventListener('click', function() {
                this.classList.toggle('report-button-active');
                // first clean the plot
                d3.select('#per-worker-disk-usage').selectAll('*').remove();
                plotWorkerDiskUsage(workerDiskUpdateCSV, workerSummaryCSV, manager_time_start, manager_time_end, this.classList.contains('report-button-active'));
            });

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

