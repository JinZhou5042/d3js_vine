export function initializeWorkerCheckboxes(workerInfo) {
    const workerCount = Object.keys(workerInfo).filter(key => key.startsWith('worker')).length;
    const checkboxesContainer = document.getElementById('violinRightButton');
    checkboxesContainer.innerHTML = '';

    // 全选/全不选复选框
    const selectAllCheckbox = document.createElement('input');
    selectAllCheckbox.type = 'checkbox';
    selectAllCheckbox.id = 'selectAll';
    selectAllCheckbox.classList.add('workerCheckbox'); // 使用驼峰命名
    selectAllCheckbox.addEventListener('change', (event) => {
        document.querySelectorAll('.workerCheckbox').forEach(checkbox => {
            checkbox.checked = event.target.checked;
            loadWorkerCharts(checkbox.value, checkbox.checked);
        });
    });

    const selectAllLabel = document.createElement('label');
    selectAllLabel.htmlFor = 'selectAll';
    selectAllLabel.textContent = 'select all';
    checkboxesContainer.appendChild(selectAllCheckbox);
    checkboxesContainer.appendChild(selectAllLabel);

    // 生成并初始化worker复选框
    for (let i = 1; i <= workerCount; i++) {
        const workerID = `worker${i}`;
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = workerID;
        checkbox.value = workerID;
        checkbox.classList.add('workerCheckbox');
        checkbox.addEventListener('change', () => loadWorkerCharts(workerID, checkbox.checked));

        const label = document.createElement('label');
        label.htmlFor = workerID;
        label.textContent = workerID;

        checkboxesContainer.appendChild(checkbox);
        checkboxesContainer.appendChild(label);
    }
    selectAllCheckbox.checked = true;
    onToggleSelectAll();
}


function loadWorkerCharts(workerID, isChecked) {
    const violinChartContainer = document.getElementById('violinRightDetailsByWorkerContainer');
    // 确保能够正确引用到 violinContainer
    const violinContainer = document.getElementById('violinContainer'); // 确保这是正确的ID

    if (isChecked) {
        // 创建图像元素
        const img = document.createElement('img');
        img.src = `Input/${workerID}_violin.svg`;
        img.alt = `Violin plot for ${workerID}`;
        img.classList.add('workerViolin', workerID); // 同时添加 'workerViolin' 和 workerID 作为类
        
        if (violinContainer) {
            // 基于 violinContainer 的高度设置图片高度为该容器高度的80%
            const containerHeight = violinContainer.clientHeight; // 获取 violinContainer 的当前高度
            img.style.height = `${containerHeight * 1}px`; // 设置图片高度为容器高度的80%
        }
        
        // 找到正确的插入位置
        const existingImgs = Array.from(violinChartContainer.querySelectorAll('.workerViolin'));
        const workerNumbers = existingImgs.map(img => parseInt(img.classList[1].replace('worker', ''), 10));
        const currentWorkerNumber = parseInt(workerID.replace('worker', ''), 10);
        
        let insertBeforeImg = null;
        for (let i = 0; i < workerNumbers.length; i++) {
            if (currentWorkerNumber < workerNumbers[i]) {
                insertBeforeImg = existingImgs[i];
                break;
            }
        }

        if (insertBeforeImg) {
            violinChartContainer.insertBefore(img, insertBeforeImg);
        } else {
            violinChartContainer.appendChild(img);
        }
    } else {
        // 移除对应的图像
        document.querySelectorAll(`.${workerID}`).forEach(img => img.remove());
    }
}


function onToggleSelectAll() {
    const isChecked = document.getElementById('selectAll').checked; // 获取全选复选框的选中状态
    const workerCheckboxes = document.querySelectorAll('.workerCheckbox'); // 获取所有worker复选框

    workerCheckboxes.forEach(checkbox => {
        checkbox.checked = isChecked; // 设置每个worker复选框的状态与全选复选框相同
        
        if (checkbox.id !== 'selectAll') {
            loadWorkerCharts(checkbox.value, isChecked); // 加载或移除对应的violin plots
        }
    });
}
