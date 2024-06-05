document.getElementById('log-debug-link').addEventListener('click', function(event) {
    event.preventDefault();

    var xhr = new XMLHttpRequest();

    xhr.open('GET', 'path/to/your/file.txt', true);

    // 设置请求完成的处理函数
    xhr.onload = function() {
        if (xhr.status === 200) {
            document.getElementById('content').innerHTML = xhr.responseText;
        } else {
            document.getElementById('content').innerHTML = '内容加载失败，请稍后再试。';
        }
    };
    xhr.onerror = function() {
        document.getElementById('contentDiv').innerHTML = '网络错误，请检查您的连接后再试。';
    };
    // 发送请求
    xhr.send();
});
