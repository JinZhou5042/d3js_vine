import { pathJoin } from './tools.js';

export function drawCoreLoads(dataDir, workerInfo) {
    // clear the existing images
    const container = document.getElementById('avgCoreLoadRightDetailsByWorkerContainer');
    const images = container.querySelectorAll('img');
    images.forEach(img => {
        container.removeChild(img);
    });

    const workerCount = Object.keys(workerInfo).filter(key => key.startsWith('worker')).length;

    const checkboxesContainer = document.getElementById('avgCoreLoadRightButton');
    checkboxesContainer.innerHTML = '';

    // initialize the selectAllCoreLoad button
    let selectAllCheckbox = initializeSetAllButon(checkboxesContainer, dataDir);
    
    // initialize the worker checkboxes
    initializeWorkerCheckboxes(checkboxesContainer, workerCount, dataDir);

    // flip the selectAllCoreLoad button to checked
    flipUpSelectAll(selectAllCheckbox, dataDir);
}

function initializeWorkerCheckboxes(checkboxesContainer, workerCount, dataDir) {
    for (let i = 1; i <= workerCount; i++) {
        const workerID = `worker${i}`;
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = workerID;
        checkbox.value = workerID;
        checkbox.classList.add('workerCheckboxCoreLoad');
        checkbox.addEventListener('change', () => loadWorkerCharts(dataDir, workerID, checkbox.checked));
        
        const label = document.createElement('label');
        label.htmlFor = workerID;
        label.textContent = workerID;

        checkboxesContainer.appendChild(checkbox);
        checkboxesContainer.appendChild(label);
    }
}

function initializeSetAllButon(checkboxesContainer, dataDir) {
    // generate and initialize selectAllCoreLoad checkbox
    const selectAllCheckbox = document.createElement('input');
    selectAllCheckbox.type = 'checkbox';
    selectAllCheckbox.id = 'selectAllCoreLoad';
    selectAllCheckbox.classList.add('workerCheckbox');
    selectAllCheckbox.addEventListener('change', (event) => {
        document.querySelectorAll('.workerCheckboxCoreLoad').forEach(checkbox => {
            checkbox.checked = event.target.checked;
            loadWorkerCharts(dataDir, checkbox.value, checkbox.checked);
        });
    });
    checkboxesContainer.appendChild(selectAllCheckbox);
    // set up the label
    const selectAllLabel = document.createElement('label');
    selectAllLabel.htmlFor = 'selectAllCoreLoad';
    selectAllLabel.textContent = 'select all';
    checkboxesContainer.appendChild(selectAllLabel);

    return selectAllCheckbox;
}


function loadWorkerCharts(dataDir, workerID, isChecked) {
    // get the upper container and 
    const avgCoreLoadChartContainer = document.getElementById('avgCoreLoadRightDetailsByWorkerContainer');
    const avgCoreLoadContainer = document.getElementById('avgCoreLoadContainer'); 

    if (isChecked) {
        // create an image element for the violin plot
        const img = document.createElement('img');
        img.src = pathJoin([dataDir, `${workerID}_core_load.svg`]);
        img.alt = `Avg Core Load plot for ${workerID}`;
        img.classList.add('workerAvgCoreLoad', `${workerID}_core_load`); // each image has a class of workerID
        
        if (avgCoreLoadContainer) {
            // set the height of the image to 80% of the container height
            const containerHeight = avgCoreLoadContainer.clientHeight;
            img.style.height = `${containerHeight * 1}px`; 
        }
        
        // find the correct position to insert the image
        const existingImgs = Array.from(avgCoreLoadChartContainer.querySelectorAll('.workerAvgCoreLoad'));
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
            avgCoreLoadChartContainer.insertBefore(img, insertBeforeImg);
        } else {
            avgCoreLoadChartContainer.appendChild(img);
        }
    } else {
        // remove all images with the workerID class
        document.querySelectorAll(`.${workerID}_core_load`).forEach(img => img.remove());
    }
}

function flipUpSelectAll(selectAllCheckbox, dataDir) {
    selectAllCheckbox.checked = true;
    // get the status of the selectAllCoreLoad checkbox
    const isChecked = document.getElementById('selectAllCoreLoad').checked;
    // get all worker checkboxes
    const workerCheckboxes = document.querySelectorAll('.workerCheckboxCoreLoad');

    workerCheckboxes.forEach(checkbox => {
        checkbox.checked = isChecked;
        if (checkbox.id !== 'selectAllCoreLoad') {
            loadWorkerCharts(dataDir, checkbox.value, isChecked);
        }
    });
}