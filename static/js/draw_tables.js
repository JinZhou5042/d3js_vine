const tableTextFontSize = '3.5rem';

function drawTaskCompletedTable(url) {
    var searchType = 'task-id';
    var searchValue = '';
    var timestampType = 'original';

    var table = $('#tasks-completed-table').DataTable({
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
            $('#tasks-completed-table_wrapper *').css({
                'font-size': tableTextFontSize,
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
    $('#button-tasks-completed-reset-table').off('click').on('click', function() {
        if (document.getElementById('button-tasks-completed-convert-timestamp').classList.contains('report-button-active')) {
            document.getElementById('button-tasks-completed-convert-timestamp').classList.toggle('report-button-active');
        }
        searchValue = '';
        timestampType = 'original';
        table.ajax.reload();
    });
    $('#button-tasks-completed-convert-timestamp').off('click').on('click', function() {
        this.classList.toggle('report-button-active');
        if (this.classList.contains('report-button-active')) {
            timestampType = 'startFromManager';
        } else {
            timestampType = 'original';
        }
        table.ajax.reload();
    });
    $('#button-tasks-completed-search-by-id').off('click').on('click', function() {
        searchType = 'task-id';
        searchValue = $('#input-tasks-completed-task-id').val();
        table.ajax.reload();
    });
    $('#button-tasks-completed-search-by-category').off('click').on('click', function() {
        searchType = 'category';
        searchValue = $('#input-tasks-completed-category').val();
        table.ajax.reload();
    });
    $('#button-tasks-completed-search-by-filename').off('click').on('click', function() {
        searchType = 'filename';
        searchValue = $('#input-tasks-completed-filename').val();
        table.ajax.reload();
    });
}

function drawTaskFailedTable(url) {
    var searchType = 'task-id';
    var searchValue = '';
    var timestampType = 'original';

    var table = $('#tasks-failed-table').DataTable({
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
            { "data": "when_next_ready" },
            { "data": "category" },
        ],
        "fixedHeader": false,
        "fixedColumns": {
            leftColumns: 1
        },
        "scrollX": true,
        "scrollY": "500px",
        "initComplete": function(settings, json) {
            $('#tasks-failed-table_wrapper *').css({
                'font-size': tableTextFontSize,
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
    $('#button-tasks-failed-reset-table').off('click').on('click', function() {
        if (document.getElementById('button-tasks-failed-convert-timestamp').classList.contains('report-button-active')) {
            document.getElementById('button-tasks-failed-convert-timestamp').classList.toggle('report-button-active');
        }
        searchValue = '';
        timestampType = 'original';
        table.ajax.reload();
    });
    $('#button-tasks-failed-convert-timestamp').off('click').on('click', function() {
        this.classList.toggle('report-button-active');
        if (this.classList.contains('report-button-active')) {
            timestampType = 'startFromManager';
        } else {
            timestampType = 'original';
        }
        table.ajax.reload();
    });
    $('#button-tasks-failed-search-by-id').off('click').on('click', function() {
        searchType = 'task-id';
        searchValue = $('#input-tasks-failed-task-id').val();
        table.ajax.reload();
    });
    $('#button-tasks-failed-search-by-category').off('click').on('click', function() {
        searchType = 'category';
        searchValue = $('#input-tasks-failed-category').val();
        table.ajax.reload();
    });
    $('#button-tasks-failed-search-by-worker-id').off('click').on('click', function() {
        searchType = 'worker-id';
        searchValue = $('#input-tasks-failed-worker-id').val();
        table.ajax.reload();
    });
}

function drawWorkerTable(url) {
    var table = $('#worker-table').DataTable({
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
            $('#worker-table_wrapper *').css({
                'font-size': tableTextFontSize,
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
}

function drawDAGTable(url) {
    var table = $('#dag-table').DataTable({
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
            $('#dag-table_wrapper *').css({
                'font-size': tableTextFontSize,
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
}

function drawFileTable(url) {
    var table = $('#file-table').DataTable({
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
            $('#file-table_wrapper *').css({
                'font-size': tableTextFontSize,
                'height': 'auto',
                'white-space': 'nowrap',
            });
        }
    });
}

function loadPage(dataName, page, perPage) {
    var url = dataName;

    $.ajax({
        url: url,
        type: 'GET',
        data: {
            log_name: window.logName,
            page: page,
            per_page: perPage
        },
        success: function() {
            if (dataName === 'tasksCompleted') {
                drawTaskCompletedTable(url); 
            } else if (dataName == 'tasksFailed') {
                drawTaskFailedTable(url);
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
    loadPage('tasksCompleted', 1, 50);
    document.getElementById('button-tasks-completed-reset-table').click();
    loadPage('tasksFailed', 1, 50);
    loadPage('worker', 1, 50);
    loadPage('dag', 1, 50);
    loadPage('file', 1, 50);
});

