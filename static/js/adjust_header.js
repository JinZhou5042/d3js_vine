document.getElementById('header-button').addEventListener('click', function() {
    var header = document.getElementById('header');
    var button = this;
    var isVisible = header.style.top === '0px' || header.style.top === '';

    var content = document.getElementById('content');
    if (isVisible) {
        header.style.top = '-140px'; // hide header
        button.style.clipPath = 'polygon(25% 40%, 50% 80%, 75% 40%)'; // change to triangle pointing down
        content.style.marginTop = '0px';
    } else {
        header.style.top = '0px';    // show header
        button.style.clipPath = 'polygon(25% 60%, 50% 20%, 75% 60%)'; // change to triangle pointing up
        content.style.marginTop = '140px';
    }
    content.offsetHeight;
});

function centerButtonWrapper() {
    var wrapper = document.getElementById('header-button-wrapper');
    var windowWidth = window.innerWidth;
    var wrapperWidth = wrapper.offsetWidth;

    var leftOffset = (windowWidth - wrapperWidth) / 2;
    wrapper.style.position = 'absolute';
    wrapper.style.left = leftOffset + 'px';
}

window.onload = centerButtonWrapper;
window.onresize = centerButtonWrapper;
