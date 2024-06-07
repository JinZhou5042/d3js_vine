
/******************************************************************/
/*                              Logs                              */
/******************************************************************/



document.addEventListener('DOMContentLoaded', function() {
    const logSelector = document.querySelector('#log-selector');
    logSelector.addEventListener('change', () => {
        const logName = logSelector.value;
        const debugFilePath = `logs/${logName}/vine-logs/debug`;
        const debugDivID = 'log-text-debug';
        loadLogFile(debugFilePath, debugDivID);
    });
});

async function loadLogFile(filePath, elementId) {
    console.log('filePath:', filePath);
    console.log('elementId:', elementId);
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${filePath} (${response.statusText})`);
        }
        const text = await response.text();
        const element = document.getElementById(elementId);
        if (element) {
            element.textContent = text;
        } else {
            console.error(`Element with id "${elementId}" not found`);
        }
    } catch (error) {
        console.error('Error fetching file:', error.message);
        const element_1 = document.getElementById(elementId);
        if (element_1) {
            element_1.textContent = `Error loading file: ${filePath}`;
        } else {
            console.error(`Element with id "${elementId}" not found`);
        }
    }
    console.log('Data loaded successfully');
}

function hideOthersShowOne(showId) {
    document.querySelectorAll('#content div, #content h1').forEach(div => {
        div.style.display = 'none';
    });
    const elementToShow = document.getElementById(showId);
    if (elementToShow) {
        elementToShow.style.display = 'block';
    } else {
        console.error(`Element with id "${showId}" not found`);
    }
}



/* Bind events */
document.querySelector('#log-debug-link').addEventListener('click', () => {
    hideOthersShowOne('log-text-debug');
});

document.addEventListener('DOMContentLoaded', function() {
    var reportLinks = document.querySelectorAll('.report-link');
    reportParser = (event) => {
        event.preventDefault();
        var allDivs = document.querySelectorAll('#content > div');

        allDivs.forEach(div => {
            if (div.classList.contains('report')) {
                div.style.display = 'block';
            } else {
                div.style.display = 'none';
            }
        });
    };

    reportLinks.forEach(link => {
        link.addEventListener('click', reportParser);
    });
});
