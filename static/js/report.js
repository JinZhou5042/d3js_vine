function setReportIFrame() {
    var iframe = document.getElementById('contentFrame');
    if (!iframe.src.endsWith('/report')) {
        iframe.src = '/report';
    }
}

const titles = {
    taskExecutionSummaryTitle: "Function Execution Summary",
    workerConfigurationsTitle: "Worker Information",
    workerSlotFunctionExecutionTitle: "Function Execution Details",
    averageCpuUsagePerTaskTitle: "Function CPU Usage Distribution",
    taskExecutionTimeDistributionTitle: "Function Runtime Distribution",
    vineGraphsTitle: "Vine Graphs",
    logDebugTitle: "debug"
};

function setReportSidebarAndContentTitles() {
    setReportIFrame();

    var iframeDocument = document.getElementById('contentFrame').contentDocument;

    document.getElementById('worker-info-link').textContent = titles.workerConfigurationsTitle;
    iframeDocument.getElementById('worker-info-title').textContent = titles.workerConfigurationsTitle;
    
    document.getElementById('task-execution-summary-link').textContent = titles.taskExecutionSummaryTitle;
    iframeDocument.getElementById('task-execution-summary-title').textContent = titles.taskExecutionSummaryTitle;

    document.getElementById('worker-slot-function-execution-link').textContent = titles.workerSlotFunctionExecutionTitle;
    iframeDocument.getElementById('worker-slot-function-execution-title').textContent = titles.workerSlotFunctionExecutionTitle;
    
    document.getElementById('average-cpu-usage-per-task-link').textContent = titles.averageCpuUsagePerTaskTitle;
    iframeDocument.getElementById('average-cpu-usage-per-task-title').textContent = titles.averageCpuUsagePerTaskTitle;

    document.getElementById('task-execution-time-distribution-link').textContent = titles.taskExecutionTimeDistributionTitle;
    iframeDocument.getElementById('task-execution-time-distribution-title').textContent = titles.taskExecutionTimeDistributionTitle;

    document.getElementById('vine-graphs-link').textContent = titles.vineGraphsTitle;
    iframeDocument.getElementById('vine-graphs-title').textContent = titles.vineGraphsTitle;
    
    document.getElementById('log-debug-link').textContent = titles.logDebugTitle;
    iframeDocument.getElementById('log-debug-title').textContent = titles.logDebugTitle;

}

window.addEventListener('load', setReportSidebarAndContentTitles);


document.addEventListener('DOMContentLoaded', function() {
    var buttons = document.querySelectorAll('.report-scroll-btn');
    buttons.forEach(function(button) {
        button.addEventListener('click', function() {
            setReportIFrame();
            var targetId = this.getAttribute('data-target');
            navigateWithinIframe(targetId);
        });
    });
});

function navigateWithinIframe(targetId) {
    var iframe = document.getElementById('contentFrame');
    if (iframe && iframe.contentWindow) {
        if (iframe.contentWindow.document.readyState === 'complete') {
            scrollIframeTo(iframe, targetId);
        } else {
            iframe.onload = function() {
                scrollIframeTo(iframe, targetId);
            };
        }
    }
}

function scrollIframeTo(iframe, targetId) {
    var target = iframe.contentWindow.document.querySelector(targetId);
    if (target) {
        target.scrollIntoView({ behavior: 'smooth' });
    }
}

