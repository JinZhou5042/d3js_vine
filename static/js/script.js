
import { drawWorkerTaskHistogram } from './histogram.js';
import { drawBarChart } from './barplot.js';
import { setupZoomAndScroll, pathJoin } from './tools.js';
import { initializeWorkerCheckboxes } from './violinplot.js';
import { displayCSV } from './displayCSV.js';


document.addEventListener('DOMContentLoaded', function() {
    let dataDir = '';
    const logSelector = document.getElementById('logSelector');
    
    // 定义一个函数，用于处理选项改变时的逻辑
    async function handleLogChange() {
        const selectedLog = this.value;
        try {
            // wait for the data path to be fetched
            dataDir = await getDataPath(`/input-path/${selectedLog}`);
            // load log description
            loadLogDescription(dataDir);
            // load worker task data
            let workerInfo = await d3.json(pathJoin([dataDir, 'worker_tasks.json']));
            // draw histogram
            drawWorkerTaskHistogram(workerInfo);
            setupZoomAndScroll('#histogram', '#histogramContainer');
            // load worker configs
            let workerConfigs = pathJoin([dataDir, 'workerConfigs.csv']);
            displayCSV(workerConfigs, '#workerConfigsContainer');
            // load performance data
            drawBarChart(workerInfo);
            setupZoomAndScroll('#barchart', '#barchartContainer');
            // draw violin plot
            initializeWorkerCheckboxes(dataDir, workerInfo);
        } catch (error) {
            console.error('Error fetching data directory:', error);
        }
    }
    
    // 为logSelector绑定change事件监听器
    logSelector.addEventListener('change', handleLogChange);

    // 如果logSelector有初始值，则手动触发change事件
    if (logSelector.value) {
        logSelector.dispatchEvent(new Event('change'));
    }
});


document.querySelectorAll('.sidebar a').forEach(link => {
    link.addEventListener('click', function(e) {
        e.preventDefault(); // 阻止默认的锚点跳转行为

        const targetId = this.getAttribute('href').substring(1); // 获取锚点目标id
        const targetElement = document.getElementById(targetId);
        if (targetElement) {
            // 滚动到指定元素位置
            window.scrollTo({
                top: targetElement.offsetTop,
                behavior: 'smooth'
            });
            // 如果目标元素内有h1内的span，则为其添加高亮类以触发动画
            const span = targetElement.querySelector('h1 > span');
            if (span) {
                // 先移除类以确保动画可以再次触发
                span.classList.remove('text-highlight');
                // 触发重排让浏览器认为是一个新的动画
                void span.offsetWidth;
                // 重新添加类来触发动画
                span.classList.add('text-highlight');
            }
        }
    });
});

async function loadLogDescription(dataPath) {
    try {
        let appInfoFilename = dataPath + (dataPath.endsWith("/") ? "" : "/") + 'app_info.json';
        // load JSON data
        const response = await fetch(appInfoFilename);
        const data = await response.json();
        // get div element
        const div = document.querySelector('#description');
        // show data in pre tag
        const formattedData = JSON.stringify(data, null, 4);
        div.innerHTML = `<pre>${formattedData}</pre>`;
    } catch (error) {
        console.error("Could not load JSON data", error);
    }
}

document.getElementById('logSelector').addEventListener('change', function() {
    const selectedLog = this.value;
    fetch(`/input-path/${selectedLog}`)
        .then(response => response.json())
        .then(data => {
            if (data.inputPath) {
                // update image sources
                document.getElementById('allWorkersSummaryViolinImg').src = `${data.inputPath}/all_workers_summary_violin.svg`;
                document.getElementById('performanceTransferImg').src = `${data.inputPath}/performance.transfer.svg`;
                document.getElementById('performanceTaskImg').src = `${data.inputPath}/performance.tasks.svg`;
                document.getElementById('performanceWorkersImg').src = `${data.inputPath}/performance.workers.svg`;
                document.getElementById('performanceTasksAccumImg').src = `${data.inputPath}/performance.tasks-accum.svg`;
                document.getElementById('performanceWorkersAccumImg').src = `${data.inputPath}/performance.workers-accum.svg`;
                document.getElementById('performanceTasksCapacitiesImg').src = `${data.inputPath}/performance.tasks-capacities.svg`;
                document.getElementById('performanceTimeManagerImg').src = `${data.inputPath}/performance.time-manager.svg`;
                document.getElementById('performanceTimeWorkersImg').src = `${data.inputPath}/performance.time-workers.svg`;
                document.getElementById('performanceTimesStackedImg').src = `${data.inputPath}/performance.times-stacked.svg`;
                document.getElementById('performanceWorkersDiskImg').src = `${data.inputPath}/performance.workers-disk.svg`;
            }
        })
        .catch(error => console.error('Error:', error));
});

async function getDataPath(path) {
    try {
        const response = await fetch(path);
        const data = await response.json();
        if (data.inputPath) {
            return data.inputPath;
        }
    } catch (error) {
        console.error('Error updating data path:', error);
    }
}
