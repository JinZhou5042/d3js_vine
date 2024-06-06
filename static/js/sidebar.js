document.querySelectorAll('#sidebar .report-link').forEach(link => {
    link.addEventListener('click', function(event) {
        event.preventDefault();
        toggleReports();
    });
});

function toggleReports() {
    // 获取所有在 #content 中的 div
    document.querySelectorAll('#content div').forEach(div => {
        // 如果是 report 类的 div，显示它
        if (div.classList.contains('report')) {
            div.style.display = 'block';
        } else {
            // 否则，隐藏这个 div
            div.style.display = 'none';
        }
    });
}




/******************************************************************/
/*                              Logs                              */
/******************************************************************/

function initializeDB() {
    return new Promise((resolve, reject) => {
        const dbName = 'vineDB';
        const storeName = 'logs';
        const dbVersion = 7;

        const dbRequest = indexedDB.open(dbName, dbVersion);

        dbRequest.onupgradeneeded = function(event) {
            const db = event.target.result;
            
            /* delete the previous database */
            if (db.objectStoreNames.contains(storeName)) {
                db.deleteObjectStore(storeName);
                console.log(`Previous object store "${storeName}" deleted`);
            }

            /* create a new object store */
            db.createObjectStore(storeName, { keyPath: 'id' });
            console.log(`Object store "${storeName}" created`);
        };

        dbRequest.onsuccess = function(event) {
            const db = event.target.result;
            console.log('Existing object stores:', db.objectStoreNames);
            resolve(db);
        };

        dbRequest.onerror = function(event) {
            reject("Database error: " + event.target.errorCode);
        };
    });
}

document.addEventListener('DOMContentLoaded', function() {
    initializeDB();
    
    const logName = document.querySelector('#log-selector').value;
    const debugFilePath = 'logs/' + logName + '/vine-logs/debug';
    const debugDivID = 'log-text-debug';

    loadLogFile(debugFilePath, debugDivID);
});



function saveToIndexedDB(dbName, storeName, dataEntry) { 
    return new Promise((resolve, reject) => {
        const dbRequest = indexedDB.open(dbName);

        dbRequest.onerror = function(event) {
            reject("Database error: " + event.target.errorCode);
        };

        dbRequest.onsuccess = function(event) {
            const db = event.target.result;
            const transaction = db.transaction(storeName, "readwrite");
            const store = transaction.objectStore(storeName);
            const request = store.put(dataEntry);

            request.onsuccess = function() {
                resolve('Data saved successfully');
            };
            request.onerror = function(event) {
                reject("Error writing to database: " + event.target.errorCode);
            };
        };
    });
}



function loadFromIndexedDB(dbName, storeName, key) {
    return new Promise((resolve, reject) => {
        const dbRequest = indexedDB.open(dbName);

        dbRequest.onerror = function(event) {
            reject("Database error: " + event.target.errorCode);
        };

        dbRequest.onsuccess = function(event) {
            const db = event.target.result;
            console.log('Opened database:', db);
            console.log('Existing object stores on load:', db.objectStoreNames);

            // 检查对象存储是否存在
            if (!db.objectStoreNames.contains(storeName)) {
                console.log(`Object store "${storeName}" not found in database "${dbName}"`);
                reject(`Object store "${storeName}" not found in database "${dbName}"`);
                return;
            }
            const transaction = db.transaction(storeName, "readonly");
            const store = transaction.objectStore(storeName);
            const request = store.get(key);

            request.onsuccess = function() {
                if (request.result) {
                    resolve(request.result.content);
                } else {
                    reject('No data found for key: ' + key);
                }
            };

            request.onerror = function(event) {
                reject("Error reading from database: " + event.target.errorCode);
            };
        };
    });
}



async function loadDataToElement(elementId, filePath) {
    console.log('filePath:', filePath)
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${filePath} (${response.statusText})`);
        }
        const text = await response.text();
        let logEntry = {
            id: filePath,
            content: text
        };
        await saveToIndexedDB('vineDB', 'logs', logEntry);
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


async function loadLogFile(filePath, debugDivID) {
    try {
        const data = await loadFromIndexedDB('vineDB', 'logs', filePath);
        const element = document.getElementById(debugDivID);
        if (element) {
            element.textContent = data;
        } else {
            console.error(`Element with id "${debugDivID}" not found`);
        }
    } catch (error) {
        try {
            await loadDataToElement(filePath, debugDivID);
            console.log('Data loaded successfully');
        } catch (fetchError) {
            console.error('Fetch error:', fetchError);
        }
    }
}


document.querySelector('#log-debug-link').addEventListener('click', () => {
    hideOthersShowOne('log-text-debug');
});