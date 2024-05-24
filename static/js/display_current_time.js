function displayCurrentTime() {
    var now = new Date();
    var dateString = now.toLocaleString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZoneName: 'short'
    });
    document.getElementById("current-time").innerHTML = dateString;
}
window.addEventListener('load', displayCurrentTime);
