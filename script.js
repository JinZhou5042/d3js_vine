
import { drawWorkerTaskHistogram } from './histogram.js';
import { drawBarChart } from './barplot.js';
import { setupZoomAndScroll } from './tools.js';
import { initializeWorkerCheckboxes } from './violinplot.js';

document.addEventListener('DOMContentLoaded', function() {
    // 加载JSON数据
    d3.json('data.json').then(function(workerInfo) {
        // 绘制直方图
        drawWorkerTaskHistogram(workerInfo);
        // 绘制条形图
        drawBarChart(workerInfo);
        // 设置缩放和滚动监听
        setupZoomAndScroll('#barchart', '#barchartContainer');
        // Show Violin Plot
        initializeWorkerCheckboxes(workerInfo);

    }).catch(function(error) {
        console.error('Error loading or processing data:', error);
    });
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
