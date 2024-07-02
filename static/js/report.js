import { fillGeneralStatistics } from './general_statistics.js';
import { setupZoomAndScroll, fetchFile } from './tools.js';
import { plotExecutionDetails } from './execution_details.js';
import { plotWorkerDiskUsage } from './worker_disk_usage.js';
import { plotDAGComponentByID } from './dag.js';



window.addEventListener('load', function() {
    
    async function handleLogChange() {
        window.logName = this.value;
        const logName = window.logName;

        window.generalStatisticsManagerCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_manager.csv`);
        window.generalStatisticsTaskCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_task.csv`);
        window.generalStatisticsWorkerCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_worker.csv`);
        window.generalStatisticsFileCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_file.csv`);
        window.generalStatisticsDAGCSV = await fetchFile(`logs/${logName}/vine-logs/general_statistics_dag.csv`);
        window.taskDoneCSV = await fetchFile(`logs/${logName}/vine-logs/task_done.csv`);
        window.taskFailedOnManagerCSV = await fetchFile(`logs/${logName}/vine-logs/task_failed_on_manager.csv`);
        window.taskFailedOnWorkerCSV = await fetchFile(`logs/${logName}/vine-logs/task_failed_on_worker.csv`);
        window.workerSummaryCSV = await fetchFile(`logs/${logName}/vine-logs/worker_summary.csv`);
        window.workerDiskUpdateCSV = await fetchFile(`logs/${logName}/vine-logs/worker_disk_usage.csv`);

        window.generalStatisticsManager = d3.csvParse(window.generalStatisticsManagerCSV);
        window.generalStatisticsTask = d3.csvParse(window.generalStatisticsTaskCSV);
        window.generalStatisticsWorker = d3.csvParse(window.generalStatisticsWorkerCSV);
        window.generalStatisticsFile = d3.csvParse(window.generalStatisticsFileCSV);
        window.generalStatisticsDAG = d3.csvParse(window.generalStatisticsDAGCSV);
        window.taskDone = d3.csvParse(window.taskDoneCSV);
        window.taskFailedOnManager = d3.csvParse(window.taskFailedOnManagerCSV);
        window.taskFailedOnWorker = d3.csvParse(window.taskFailedOnWorkerCSV);
        window.workerSummary = d3.csvParse(window.workerSummaryCSV);
        window.workerDiskUpdate = d3.csvParse(window.workerDiskUpdateCSV);

        window.manager_time_start = d3.csvParse(window.generalStatisticsManagerCSV)[0].time_start;
        window.manager_time_end = d3.csvParse(window.generalStatisticsManagerCSV)[0].time_end;

        window.parent.document.dispatchEvent(new Event('dataLoaded'));

        try {

            fillGeneralStatistics();

            plotExecutionDetails();
            setupZoomAndScroll('#execution-details', '#execution-details-container');

            plotDAGComponentByID(1);
        
            plotWorkerDiskUsage(false);
            setupZoomAndScroll('#worker-disk-usage', '#worker-disk-usage-container');


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

