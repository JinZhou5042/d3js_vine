document.querySelectorAll('#sidebar a').forEach(link => {
    link.addEventListener('click', function(e) {
        e.preventDefault(); // prevent default anchor behavior

        const targetId = this.getAttribute('href').substring(1);
        const targetElement = document.getElementById(targetId);
        if (targetElement) {
            // temporarily add smooth scroll behavior
            document.documentElement.classList.add('smooth-scroll');

            // scroll to the target element
            targetElement.scrollIntoView();

            // remove smooth scroll behavior after 0.3 seconds
            setTimeout(() => {
                document.documentElement.classList.remove('smooth-scroll');
            }, 300);

            // if the target element has a span in the h1, trigger the highlight animation
            const span = targetElement.querySelector('h1 > span');
            if (span) {
                span.classList.remove('text-highlight');
                void span.offsetWidth;
                span.classList.add('text-highlight');
            }
        }
    });
});
