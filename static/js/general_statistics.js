
export function fillGeneralStatistics() {
    fillGeneralStatisticsManager();
}

function fillGeneralStatisticsManager() {
    if (typeof window.generalStatisticsManager === 'undefined') {
        return;
    }

    $(window).on('load', function() {
        $('#general-statistics-manager-table').DataTable({
            "paging": true,
            "searching": false
        });
    });
    
    
    const tbody = d3.select('#general-statistics-manager-table').select('tbody');
    tbody.selectAll('tr').remove();

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

function drawTaskDoneTable(url) {
    var searchType = 'task-id';
    var searchValue = '';
    var timestampType = 'original';

    var table = $('#general-statistics-task-table').DataTable({
        "processing": true,
        "serverSide": true,
        "paging": true,
        "pageLength": 50,
        "destroy": true,
        "searching": false,
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
                d.search.type = searchType;
                d.search.value = searchValue;
                d.timestamp_type = timestampType;
            },
            "dataSrc": function(json) {
                json.data.forEach(function(task) {
                    task.task_id = parseInt(task.task_id, 10);
                    task.try_id = parseInt(task.try_id, 10);
                    task.worker_id = parseInt(task.worker_id, 10);
                });
                return json.data;
            }
        },
        "columns": [
            { "data": "task_id" },
            { "data": "try_id" },
            { "data": "worker_id" },
            { "data": "when_ready" },
            { "data": "when_running" },
            { "data": "time_worker_start" },
            { "data": "time_worker_end" },
            { "data": "when_waiting_retrieval" },
            { "data": "when_retrieved" },
            { "data": "when_done" },
            { "data": "category" },
            { "data": "size_input_files(MB)" },
            { "data": "size_output_files(MB)" },
            { "data": "input_files" },
            { "data": "output_files" },
        ],
        "fixedHeader": false,
        "fixedColumns": {
            leftColumns: 1
        },
        "scrollX": true,
        "scrollY": "500px",
        "initComplete": function(settings, json) {
            $('#general-statistics-task-table_wrapper *').css({
                'font-size': '4rem',
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
    $('#button-reset-task-table').off('click').on('click', function() {
        if (document.getElementById('button-convert-task-timestamp').classList.contains('report-button-active')) {
            document.getElementById('button-convert-task-timestamp').classList.toggle('report-button-active');
        }
        searchValue = '';
        timestampType = 'original';
        table.ajax.reload();
    });
    $('#button-search-task-by-id').off('click').on('click', function() {
        searchType = 'task-id';
        searchValue = $('#input-search-task-by-id').val();
        table.ajax.reload();
    });
    $('#button-search-task-by-category').off('click').on('click', function() {
        searchType = 'category';
        searchValue = $('#input-search-task-by-category').val();
        table.ajax.reload();
    });
    $('#button-search-task-by-filename').off('click').on('click', function() {
        searchType = 'filename';
        searchValue = $('#input-search-task-by-filename').val();
        table.ajax.reload();
    });
    $('#button-convert-task-timestamp').off('click').on('click', function() {
        this.classList.toggle('report-button-active');
        if (this.classList.contains('report-button-active')) {
            timestampType = 'startFromManager';
        } else {
            timestampType = 'original';
        }
        table.ajax.reload();
    });
}

function drawWorkerTable(url) {
    var table = $('#general-statistics-worker-table').DataTable({
        "processing": true,
        "serverSide": true,
        "paging": true,
        "pageLength": 50,
        "destroy": true,
        "searching": false,
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
            },
        },
        "columns": [
            { "data": "worker_id" },
            { "data": "worker_hash" },
            { "data": "worker_machine_name" },
            { "data": "worker_ip" },
            { "data": "worker_port" },
            { "data": "time_connected" },
            { "data": "time_disconnected" },
            { "data": "lifetime(s)" },
            { "data": "cores" },
            { "data": "memory(MB)" },
            { "data": "disk(MB)" },
            { "data": "tasks_done" },
            { "data": "avg_task_runtime(s)" },
            { "data": "peak_disk_usage(MB)" },
            { "data": "peak_disk_usage(%)" },
        ],
        "fixedHeader": false,
        "fixedColumns": {
            leftColumns: 1
        },
        "scrollX": true,
        "scrollY": "500px",
        "initComplete": function(settings, json) {
            $('#general-statistics-worker-table_wrapper *').css({
                'font-size': '4rem',
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
}

function drawDAGTable(url) {
    var table = $('#general-statistics-dag-table').DataTable({
        "processing": true,
        "serverSide": true,
        "paging": true,
        "pageLength": 50,
        "destroy": true,
        "searching": false,
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
            },
        },
        "columns": [
            { "data": "graph_id" },
            { "data": "num_tasks" },
            { "data": "time_critical_path" },
            { "data": "num_critical_tasks" },
            { "data": "critical_tasks" },
        ],
        "fixedHeader": false,
        "fixedColumns": {
            leftColumns: 1
        },
        "scrollX": true,
        "scrollY": "500px",
        "initComplete": function(settings, json) {
            $('#general-statistics-dag-table_wrapper *').css({
                'font-size': '4rem',
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
}

function drawFileTable(url) {
    var table = $('#general-statistics-file-table').DataTable({
        "processing": true,
        "serverSide": true,
        "paging": true,
        "pageLength": 50,
        "destroy": true,
        "searching": false,
        "ajax": {
            "url": url,
            "type": "GET",
            "data": function(d) {
                d.log_name = window.logName;
            },
        },
        "columns": [
            { "data": "filename" },
            { "data": "size(MB)" },
            { "data": "producers" },
            { "data": "consumers" },
            { "data": "num_workers_holding" },
            { "data": "worker_holding" },
        ],
        "fixedHeader": false,
        "fixedColumns": {
            leftColumns: 1
        },
        "scrollX": true,
        "scrollY": "500px",
        "initComplete": function(settings, json) {
            $('#general-statistics-file-table_wrapper *').css({
                'font-size': '4rem',
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
}

function loadPage(dataName, page, perPage) {
    var url = `http://127.0.0.1:9122/` + dataName;

    $.ajax({
        url: url,
        type: 'GET',
        data: {
            log_name: window.logName,
            page: page,
            per_page: perPage
        },
        success: function() {
            if (dataName === 'taskDone') {
                drawTaskDoneTable(url); 
            } else if (dataName === 'worker') {
                drawWorkerTable(url);
            } else if (dataName === 'dag') {
                drawDAGTable(url);
            } else if (dataName == 'file') {
                drawFileTable(url);
            }
        },
        error: function(xhr, status, error) {
            console.error('Error status:', status);
            console.error('Error details:', error);
            console.error('Server response:', xhr.responseText);
        }
    });
}

window.parent.document.addEventListener('dataLoaded', function() {
    loadPage('taskDone', 1, 50);
    document.getElementById('button-reset-task-table').click();
    loadPage('worker', 1, 50);
    loadPage('dag', 1, 50);
    loadPage('file', 1, 50);
});



function fillGeneralStatisticsWorker() {
    if (typeof window.generalStatisticsWorker === 'undefined') {
        return;
    }

    const tbody = d3.select('#general-statistics-worker-table').select('tbody');
    tbody.selectAll('tr').remove();

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

