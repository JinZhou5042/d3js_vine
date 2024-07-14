import { setupZoomAndScroll, fetchFile } from './tools.js';
import { plotExecutionDetails } from './execution_details.js';
import { plotWorkerDiskUsage } from './worker_disk_usage.js';
import { plotDAGComponentByID } from './dag.js';


window.addEventListener('load', function() {
    
    async function handleLogChange() {
        window.logName = this.value;
        // remove all the svgs
        var svgs = d3.selectAll('svg');
        svgs.each(function() {
            d3.select(this).selectAll('*').remove();
        });

        // hidden some divs
        const headerTips = window.parent.document.getElementsByClassName('error-tip');
        for (let i = 0; i < headerTips.length; i++) {
            headerTips[i].style.display = 'none';
        }

        const files = [
            { name: 'generalStatisticsManager', url: `logs/${window.logName}/vine-logs/general_statistics_manager.csv` },
            { name: 'generalStatisticsTask', url: `logs/${window.logName}/vine-logs/general_statistics_task.csv` },
            { name: 'generalStatisticsWorker', url: `logs/${window.logName}/vine-logs/general_statistics_worker.csv` },
            { name: 'generalStatisticsFile', url: `logs/${window.logName}/vine-logs/general_statistics_file.csv` },
            { name: 'generalStatisticsDAG', url: `logs/${window.logName}/vine-logs/general_statistics_dag.csv` },
            { name: 'taskDone', url: `logs/${window.logName}/vine-logs/task_done.csv` },
            { name: 'taskFailedOnManager', url: `logs/${window.logName}/vine-logs/task_failed_on_manager.csv` },
            { name: 'taskFailedOnWorker', url: `logs/${window.logName}/vine-logs/task_failed_on_worker.csv` },
            { name: 'workerSummary', url: `logs/${window.logName}/vine-logs/worker_summary.csv` },
            { name: 'workerDiskUpdate', url: `logs/${window.logName}/vine-logs/worker_disk_usage.csv` },
            { name: 'fileInfo', url: `logs/${window.logName}/vine-logs/file_info.csv` },
            { name: 'workerConnections', url: `logs/${window.logName}/vine-logs/worker_connections.csv` },
        ];
        for (const file of files) {
            try {
                const response = await fetchFile(file.url);
                window[`${file.name}CSV`] = response;
                window[file.name] = d3.csvParse(response);
            } catch (error) {
                console.error(`Error fetching or parsing ${file.name} file:`, error);
            }
        }
        window.generalStatisticsManager = window.window.generalStatisticsManager[0];
        window.time_manager_start = window.generalStatisticsManager.time_start;
        window.time_manager_end = window.generalStatisticsManager.time_end;

        window.minTime = window.time_manager_start;
        window.maxTime = window.time_manager_end;

        window.parent.document.dispatchEvent(new Event('dataLoaded'));

        try {
            plotExecutionDetails();
            setupZoomAndScroll('#execution-details', '#execution-details-container');

            plotDAGComponentByID(1);
        
            plotWorkerDiskUsage({displayDiskUsageByPercentage: false});


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

