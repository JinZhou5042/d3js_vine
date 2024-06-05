const titles = {
    taskExecutionSummaryTitle: "Function Execution Summary",
    workerConfigurationsTitle: "Worker Information",
    workerSlotFunctionExecutionTitle: "Function Execution Details",
    averageCpuUsagePerTaskTitle: "Function CPU Usage Distribution",
    taskExecutionTimeDistributionTitle: "Function Runtime Distribution",
    vineGraphsTitle: "Vine Graphs",
};

function setTitles() {

    document.getElementById('task-execution-summary-link').textContent = titles.taskExecutionSummaryTitle;
    document.getElementById('task-execution-summary-title').textContent = titles.taskExecutionSummaryTitle;
    
    document.getElementById('worker-info-link').textContent = titles.workerConfigurationsTitle;
    document.getElementById('worker-info-title').textContent = titles.workerConfigurationsTitle;

    document.getElementById('worker-slot-function-execution-link').textContent = titles.workerSlotFunctionExecutionTitle;
    document.getElementById('worker-slot-function-execution-title').textContent = titles.workerSlotFunctionExecutionTitle;
    
    document.getElementById('average-cpu-usage-per-task-link').textContent = titles.averageCpuUsagePerTaskTitle;
    document.getElementById('average-cpu-usage-per-task-title').textContent = titles.averageCpuUsagePerTaskTitle;

    document.getElementById('task-execution-time-distribution-link').textContent = titles.taskExecutionTimeDistributionTitle;
    document.getElementById('task-execution-time-distribution-title').textContent = titles.taskExecutionTimeDistributionTitle;

    document.getElementById('vine-graphs-link').textContent = titles.vineGraphsTitle;
    document.getElementById('vine-graphs-title').textContent = titles.vineGraphsTitle;
}

window.addEventListener('load', setTitles);