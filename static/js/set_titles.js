const titles = {
    workflowDescriptionTitle: "Workflow Description",
    taskExecutionSummaryTitle: "Per Worker Task Execution Summary",
    workerConfigurationsTitle: "Per Worker Configurations",
    workerSlotFunctionExecutionTitle: "Worker-Slot Function Execution",
    averageCpuUsagePerTaskTitle: "Average CPU Usage Per Task",
    taskExecutionTimeDistributionTitle: "Task Execution Time Distribution",
    vineGraphsTitle: "Vine Graphs",
};

function setTitles() {
    document.getElementById('workflow-description-link').textContent = titles.workflowDescriptionTitle;
    document.getElementById('workflow-description-title').textContent = titles.workflowDescriptionTitle;

    document.getElementById('task-execution-summary-link').textContent = titles.taskExecutionSummaryTitle;
    document.getElementById('task-execution-summary-title').textContent = titles.taskExecutionSummaryTitle;
    
    document.getElementById('worker-configurations-link').textContent = titles.taskExecutionSummaryTitle;
    document.getElementById('worker-configurations-title').textContent = titles.taskExecutionSummaryTitle;

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