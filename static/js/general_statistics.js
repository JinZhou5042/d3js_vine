
export function fillGeneralStatistics() {
    fillGeneralStatisticsManager();
    fillGeneralStatisticsTask();
    fillGeneralStatisticsWorker();
    fillGeneralStatisticsFile();
    fillGeneralStatisticsDAG();
}

function fillGeneralStatisticsManager() {
    const tbody = d3.select('#general-statistics-manager-table').select('tbody');

    const rows = tbody.selectAll('tr')
        .data(window.generalStatisticsManager)
        .enter()
        .append('tr');

    rows.append('td').text(d => d.time_start_human);
    rows.append('td').text(d => d.time_end_human);
    rows.append('td').text(d => d['lifetime(s)']);
    rows.append('td').text(d => d.tasks_submitted);
    rows.append('td').text(d => d.tasks_done);
    rows.append('td').text(d => d.tasks_failed_on_manager);
    rows.append('td').text(d => d.tasks_failed_on_worker);
    rows.append('td').text(d => d.max_task_try_count);
    rows.append('td').text(d => d.total_workers);
    rows.append('td').text(d => d.active_workers);
    rows.append('td').text(d => d.max_concurrent_workers);
}

function fillGeneralStatisticsTask() {
    const tbody = d3.select('#general-statistics-task-table').select('tbody');

    const rows = tbody.selectAll('tr')
        .data(window.generalStatisticsTask)
        .enter()
        .append('tr');

    rows.append('td').text(d => d.category);
    rows.append('td').text(d => d.submitted);
    rows.append('td').text(d => d.ready);
    rows.append('td').text(d => d.running);
    rows.append('td').text(d => d.waiting_retrieval);
    rows.append('td').text(d => d.retrieved);
    rows.append('td').text(d => d.done);
    rows.append('td').text(d => d.workers);
}

function fillGeneralStatisticsWorker() {
    const tbody = d3.select('#general-statistics-worker-table').select('tbody');

    const rows = tbody.selectAll('tr')
        .data(window.generalStatisticsWorker)
        .enter()
        .append('tr');

    rows.append('td').text(d => d.worker_id);
    rows.append('td').text(d => d.worker_hash);
    rows.append('td').text(d => d.worker_machine_name);
    rows.append('td').text(d => d.worker_ip);
    rows.append('td').text(d => d.worker_port);
    rows.append('td').text(d => d.time_connected);
    rows.append('td').text(d => d.time_disconnected);
    rows.append('td').text(d => d['lifetime(s)']);
    rows.append('td').text(d => d.cores);
    rows.append('td').text(d => d['memory(MB)']);
    rows.append('td').text(d => d['disk(MB)']);
    rows.append('td').text(d => d.tasks_done);
    rows.append('td').text(d => d['avg_task_runtime(s)']);
    rows.append('td').text(d => d['peak_disk_usage(MB)']);
    rows.append('td').text(d => d['peak_disk_usage(%)']);
}

function fillGeneralStatisticsFile() {
    const tbody = d3.select('#general-statistics-file-table').select('tbody');

    const rows = tbody.selectAll('tr')
        .data(window.generalStatisticsFile)
        .enter()
        .append('tr');

    rows.append('td').text(d => d.filename);
    rows.append('td').text(d => d['size(MB)']);
    rows.append('td').text(d => d.num_worker_holding);
    rows.append('td').text(d => d.producers);
    rows.append('td').text(d => d.consumers);
}

function fillGeneralStatisticsDAG() {
    const tbody = d3.select('#general-statistics-dag-table').select('tbody');

    const rows = tbody.selectAll('tr')
        .data(window.generalStatisticsDAG)
        .enter()
        .append('tr');

    rows.append('td').text(d => d.graph_id);
    rows.append('td').text(d => d.num_tasks);
    rows.append('td').text(d => d.num_critical_tasks);
    rows.append('td').text(d => d.critical_tasks);
    rows.append('td').text(d => d.time_critical_nodes);
    rows.append('td').text(d => d.time_critical_edges);
    rows.append('td').text(d => d.time_critical_path);
    rows.append('td').text(d => d.tasks);
}