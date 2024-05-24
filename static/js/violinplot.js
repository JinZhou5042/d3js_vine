import { pathJoin } from "./tools.js";  

export function drawViolins(dataDir, workerInfo) {
    // clear the existing images
    const container = document.getElementById('violinRightDetailsByWorkerContainer');
    const images = container.querySelectorAll('img');
    images.forEach(img => {
        container.removeChild(img);
    });

    const workerCount = Object.keys(workerInfo).filter(key => key.startsWith('worker')).length;

    const checkboxesContainer = document.getElementById('violinRightButton');
    checkboxesContainer.innerHTML = '';

    // initialize the selectAllViolin button
    let selectAllCheckbox = initializeSetAllButon(checkboxesContainer, dataDir);
    
    // initialize the worker checkboxes
    initializeWorkerCheckboxes(checkboxesContainer, workerCount, dataDir);

    // flip the selectAllViolin button to checked
    flipUpSelectAll(selectAllCheckbox, dataDir);
}

function initializeWorkerCheckboxes(checkboxesContainer, workerCount, dataDir) {
    for (let i = 1; i <= workerCount; i++) {
        const workerID = `worker${i}`;
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = workerID;
        checkbox.value = workerID;
        checkbox.classList.add('workerViolinCheckbox');
        checkbox.addEventListener('change', () => loadWorkerCharts(dataDir, workerID, checkbox.checked));

        const label = document.createElement('label');
        label.htmlFor = workerID;
        label.textContent = workerID;

        checkboxesContainer.appendChild(checkbox);
        checkboxesContainer.appendChild(label);
    }
}

function initializeSetAllButon(checkboxesContainer, dataDir) {
    // generate and initialize selectAllViolin checkbox
    const selectAllCheckbox = document.createElement('input');
    selectAllCheckbox.type = 'checkbox';
    selectAllCheckbox.id = 'selectAllViolin';
    selectAllCheckbox.classList.add('workerCheckbox');
    selectAllCheckbox.addEventListener('change', (event) => {
        document.querySelectorAll('.workerViolinCheckbox').forEach(checkbox => {
            checkbox.checked = event.target.checked;
            loadWorkerCharts(dataDir, checkbox.value, checkbox.checked);
        });
    });
    checkboxesContainer.appendChild(selectAllCheckbox);
    // set up the label
    const selectAllLabel = document.createElement('label');
    selectAllLabel.htmlFor = 'selectAllViolin';
    selectAllLabel.textContent = 'select all';
    checkboxesContainer.appendChild(selectAllLabel);

    return selectAllCheckbox;
}

function loadWorkerCharts(dataDir, workerID, isChecked) {
    // get the upper container and 
    const violinChartContainer = document.getElementById('violinRightDetailsByWorkerContainer');
    const violinContainer = document.getElementById('violinContainer'); 

    if (isChecked) {
        // create an image element for the violin plot
        const img = document.createElement('img');
        img.src = pathJoin([dataDir, `${workerID}_violin.svg`]);
        img.alt = `Violin plot for ${workerID}`;
        img.classList.add('workerViolin', `${workerID}_violin`); // each image has a class of workerID
        
        if (violinContainer) {
            // set the height of the image to 80% of the container height
            const containerHeight = violinContainer.clientHeight;
            img.style.height = `${containerHeight * 1}px`; 
        }
        
        // find the correct position to insert the image
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
        // remove all images with the workerID class
        document.querySelectorAll(`.workerViolin`).forEach(img => img.remove());
    }
}

function flipUpSelectAll(selectAllCheckbox, dataDir) {
    selectAllCheckbox.checked = true;
    // get the status of the selectAllViolin checkbox
    const isChecked = document.getElementById('selectAllViolin').checked;
    // get all worker checkboxes
    const workerCheckboxes = document.querySelectorAll('.workerViolinCheckbox');

    workerCheckboxes.forEach(checkbox => {
        checkbox.checked = isChecked;
        if (checkbox.id !== 'selectAllViolin') {
            loadWorkerCharts(dataDir, checkbox.value, isChecked);
        }
    });
}
