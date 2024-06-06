document.getElementById('header-button').addEventListener('click', function() {
    var header = document.getElementById('header');
    var button = this;
    var isVisible = header.style.top === '0px' || header.style.top === '';

    var content = document.getElementById('content');
    if (isVisible) {
        header.style.top = '-130px'; // hide header
        header.style.borderBottom = 'none';
        button.style.clipPath = 'polygon(25% 40%, 50% 80%, 75% 40%)'; // change to triangle pointing down
        content.style.top = '0px';
        content.style.height = "100%"
    } else {
        header.style.top = '0px';    // show header
        header.style.borderBottom = '1px solid #ccc';
        button.style.clipPath = 'polygon(25% 60%, 50% 20%, 75% 60%)'; // change to triangle pointing up
        content.style.top = '140px';
        content.style.height = "calc(100% - 140px)"
    }
    content.offsetHeight;
});
