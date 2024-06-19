
export function setupZoomAndScroll(svgSelector, containerSelector) {
    const svgElement = document.querySelector(svgSelector); // Select the SVG element.
    const container = document.querySelector(containerSelector); // Select the container of the SVG.

    // Store the initial width and height of the SVG.
    let initialWidth = svgElement.getBoundingClientRect().width;
    let initialHeight = svgElement.getBoundingClientRect().height;

    // Define the maximum and minimum zoom scales.
    const maxWidth = initialWidth * 20; // Max width is 20 times the initial width.
    const maxHeight = initialHeight * 20; // Max height is 20 times the initial height.
    const minWidth = initialWidth * 0.95; // Min width is 80% of the initial width.
    const minHeight = initialHeight * 0.95; // Min height is 80% of the initial height.

    container.addEventListener('wheel', function(event) {
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
            container.scrollLeft += offsetX;
            container.scrollTop += offsetY;

            // Update the initial dimensions for the next scaling operation.
            initialWidth = newWidth;
            initialHeight = newHeight;
        }
    });
}

export function pathJoin(parts, sep) {
    var separator = sep || '/';
    var replace = new RegExp(separator + '{1,}', 'g');
    return parts.join(separator).replace(replace, separator);
}