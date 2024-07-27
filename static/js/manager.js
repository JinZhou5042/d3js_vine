import { fetchFile } from './tools.js';

const factoryDescriptionContainer = document.getElementById('factory-description-container');

function fillMgrDescription() {
    document.getElementById('start-time').textContent = window.generalStatisticsManager.time_start_human;
    document.getElementById('end-time').textContent = window.generalStatisticsManager.time_end_human;
    document.getElementById('lift-time').textContent = window.generalStatisticsManager['lifetime(s)'] + 's';
    document.getElementById('tasks-submitted').textContent = window.generalStatisticsManager.tasks_submitted;
    document.getElementById('tasks-done').textContent = window.generalStatisticsManager.tasks_done;
    document.getElementById('tasks-waiting').textContent = window.generalStatisticsManager.tasks_failed_on_manager;
    document.getElementById('tasks-failed').textContent = window.generalStatisticsManager.tasks_failed_on_worker;
    document.getElementById('workers-connected').textContent = window.generalStatisticsManager.total_workers;
    document.getElementById('workers-active').textContent = window.generalStatisticsManager.active_workers;
    document.getElementById('max-concurrent-workers').textContent = window.generalStatisticsManager.max_concurrent_workers;
}
async function fillFactoryDescription() {
    return;
    try {
        var factory = await fetchFile(`logs/${window.logName}/vine-logs/factory.json`);
        factory = JSON.parse(factory);
        factory = JSON.stringify(factory, null, 2);
        factoryDescriptionContainer.innerHTML = `<pre class="formatted-json"><b>factory.json</b> ${factory}</pre>`;
    } catch (error) {
        // pass
    }
}


window.parent.document.addEventListener('dataLoaded', function() {
    fillMgrDescription();
    fillFactoryDescription();
});