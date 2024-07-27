
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

export function formatUnixTimestamp(unixTimestamp, format = 'YYYY-MM-DD HH:mm:ss.SSS') {
    var date = new Date(unixTimestamp * 1000);
    var year = date.getFullYear();
    var month = ('0' + (date.getMonth() + 1)).slice(-2);
    var day = ('0' + date.getDate()).slice(-2);
    var hours = ('0' + date.getHours()).slice(-2);
    var minutes = ('0' + date.getMinutes()).slice(-2);
    var seconds = ('0' + date.getSeconds()).slice(-2);
    var milliseconds = ('00' + date.getMilliseconds()).slice(-3); 

    switch (format) {
        case 'YYYY-MM-DD':
            return `${year}-${month}-${day}`;
        case 'YYYY-MM-DD HH:mm:ss':
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        case 'MM/DD/YYYY':
            return `${month}/${day}/${year}`;
        case 'YYYY-MM-DD HH:mm:ss.SSS':
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
        default:
            return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
    }
}

export async function fetchCSVData(csvFilename) {
    try {
        const response = await axios.get(`get_csv_data`, {
            params: {
                log_name: logName,
                csv_filename: csvFilename
            }
        });
        return response.data;

    } catch (error) {
        console.error('Error:', error);
    }
}

export async function fetchFile(filePath) {
    try {
        const response = await fetch(filePath);
        if (!response.ok) {
            throw new Error(`Failed to fetch file: ${filePath} (${response.statusText})`);
        }
        return await response.text();
    } catch (error) {
        console.error(error);
        throw error;
    }
}

export function pathJoin(parts, sep) {
    var separator = sep || '/';
    var replace = new RegExp(separator + '{1,}', 'g');
    return parts.join(separator).replace(replace, separator);
}

export function setupZoomAndScroll(svgElementName, svgContainerName) {
    const svgElement = document.querySelector(svgElementName); // Select the SVG element.
    const svgContainer = document.querySelector(svgContainerName); // Select the container of the SVG.

    // Store the initial width and height of the SVG.
    let initialWidth = svgElement.getBoundingClientRect().width;
    let initialHeight = svgElement.getBoundingClientRect().height;

    // Define the maximum and minimum zoom scales.
    const maxWidth = initialWidth * 64;
    const maxHeight = initialHeight * 64; 
    const minWidth = initialWidth * 0.95;
    const minHeight = initialHeight * 0.95;

    svgContainer.addEventListener('wheel', function(event) {
        if (event.ctrlKey) { // Check if the Ctrl key is pressed during scroll.
            event.preventDefault(); // Prevent the default scroll behavior.

            const zoomFactor = event.deltaY < 0 ? 1.1 : 0.9; // Determine the zoom direction.
            let newWidth = initialWidth * zoomFactor; // Calculate the new width based on the zoom factor.
            let newHeight = initialHeight * zoomFactor; // Calculate the new height based on the zoom factor.

            // Check if the new dimensions exceed the zoom limits.
            if ((newWidth >= maxWidth && zoomFactor > 1) || (newWidth <= minWidth && zoomFactor < 1) ||
                (newHeight >= maxHeight && zoomFactor > 1) || (newHeight <= minHeight && zoomFactor < 1)) {
                return; // If the new dimensions are outside the limits, exit the function.
            }

            // Calculate the mouse position relative to the SVG content before scaling.
            const rect = svgElement.getBoundingClientRect(); // Get the current size and position of the SVG.
            const mouseX = event.clientX - rect.left; // Mouse X position within the SVG.
            const mouseY = event.clientY - rect.top; // Mouse Y position within the SVG.

            // Determine the mouse position as a fraction of the SVG's width and height.
            const scaleX = mouseX / rect.width; 
            const scaleY = mouseY / rect.height; 

            // Apply the new dimensions to the SVG element.
            svgElement.style.width = `${newWidth}px`;
            svgElement.style.height = `${newHeight}px`;

            // After scaling, calculate where the mouse position would be relative to the new size.
            const newRect = svgElement.getBoundingClientRect(); // Get the new size and position of the SVG.
            const targetX = scaleX * newRect.width; 
            const targetY = scaleY * newRect.height; 

            // Calculate the scroll offsets needed to keep the mouse-over point visually static.
            const offsetX = targetX - mouseX; 
            const offsetY = targetY - mouseY; 

            // Adjust the scroll position of the container to compensate for the scaling.
            svgContainer.scrollLeft += offsetX;
            svgContainer.scrollTop += offsetY;

            // Update the initial dimensions for the next scaling operation.
            initialWidth = newWidth;
            initialHeight = newHeight;
        }
    });
}

export function downloadSVG(svgElementId, filename = null) {
    const svgElement = document.getElementById(svgElementId);
    if (!svgElement) {
        console.error('SVG element not found');
        return;
    }
    if (!filename) {
        filename = svgElementId.replace(/-/g, '_');
        if (filename.endsWith('svg')) {
            filename = filename.substring(0, filename.length - 4);
        }
        filename = filename + '.svg';
    }

    const serializer = new XMLSerializer();
    const svgString = serializer.serializeToString(svgElement);

    const blob = new Blob([svgString], {type: "image/svg+xml"});

    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}


export function getTaskInnerHTML(taskData) {
    return `
        task id: ${taskData.task_id}<br>
        worker: ${taskData.worker_id} (core ${taskData.core_id})<br>
        category: ${taskData.category.replace(/^<|>$/g, '')}<br>
        execution time: ${(taskData.time_worker_end - taskData.time_worker_start).toFixed(2)}s<br>
        input size: ${(taskData.size_input_mgr - 0).toFixed(4)}MB<br>
        output size: ${(taskData.size_output_mgr - 0).toFixed(4)}MB<br>
        when ready: ${(taskData.when_ready - window.minTime).toFixed(2)}s<br>
        when running: ${(taskData.when_running - window.minTime).toFixed(2)}s<br>
        when actually running: ${(taskData.time_worker_start - window.minTime).toFixed(2)}s<br>
        when actually done: ${(taskData.time_worker_end - window.minTime).toFixed(2)}s<br>
        when waiting retrieval: ${(taskData.when_waiting_retrieval - window.minTime).toFixed(2)}s<br>
        when retrieved: ${(taskData.when_retrieved - window.minTime).toFixed(2)}s<br>
        when done: ${(taskData.when_done - window.minTime).toFixed(2)}s<br>
    `;
}