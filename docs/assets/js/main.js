// 导航栏滚动效果
window.addEventListener('scroll', () => {
    const navbar = document.getElementById('navbar');
    if (navbar) {
        if (window.scrollY > 50) {
            navbar.classList.add('scrolled');
        } else {
            navbar.classList.remove('scrolled');
        }
    }
});

// 移动端菜单
const menuBtn = document.getElementById('menuBtn');
const navLinks = document.getElementById('navLinks');

if (menuBtn) {
    menuBtn.addEventListener('click', () => {
        navLinks.classList.toggle('active');
    });
}

// 平滑滚动
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            target.scrollIntoView({ behavior: 'smooth' });
            if (navLinks) navLinks.classList.remove('active');
        }
    });
});

// 代码标签页切换
const tabBtns = document.querySelectorAll('.code-tab-btn');
const tabContents = document.querySelectorAll('.code-tab-content');

tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        const tabId = btn.getAttribute('data-tab');
        
        tabBtns.forEach(b => b.classList.remove('active'));
        tabContents.forEach(c => c.classList.remove('active'));
        
        btn.classList.add('active');
        document.getElementById(`${tabId}-tab`).classList.add('active');
    });
});

// 滚动动画观察器
const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.classList.add('visible');
        }
    });
}, observerOptions);

document.querySelectorAll('.feature-card, .arch-component, .testimonial-card, .version-item, .timeline-item').forEach(el => {
    observer.observe(el);
});

// 文档页面 - 自动生成目录
function generateTOC() {
    const content = document.querySelector('.docs-content');
    const sidebar = document.querySelector('.docs-sidebar .toc');
    
    if (!content || !sidebar) return;
    
    const headings = content.querySelectorAll('h2, h3');
    if (headings.length === 0) return;
    
    let tocHTML = '<ul>';
    headings.forEach(heading => {
        const id = heading.textContent.toLowerCase().replace(/[^a-z0-9]+/g, '-');
        heading.id = id;
        const level = heading.tagName === 'H2' ? 'toc-h2' : 'toc-h3';
        tocHTML += `<li class="${level}"><a href="#${id}">${heading.textContent}</a></li>`;
    });
    tocHTML += '</ul>';
    
    sidebar.innerHTML = '<h4>本页目录</h4>' + tocHTML;
}

// 文档页面 - 激活当前目录项
function highlightCurrentTOCItem() {
    const tocLinks = document.querySelectorAll('.toc a');
    if (tocLinks.length === 0) return;
    
    const scrollPosition = window.scrollY + 100;
    
    let currentId = '';
    for (const heading of document.querySelectorAll('.docs-content h2, .docs-content h3')) {
        if (heading.offsetTop <= scrollPosition) {
            currentId = heading.id;
        }
    }
    
    tocLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === `#${currentId}`) {
            link.classList.add('active');
        }
    });
}

// 初始化文档功能
if (document.querySelector('.docs-content')) {
    generateTOC();
    window.addEventListener('scroll', highlightCurrentTOCItem);
    highlightCurrentTOCItem();
}

// 搜索功能
const searchInput = document.getElementById('searchInput');
if (searchInput) {
    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.toLowerCase();
        // 实现搜索逻辑
        console.log('Search:', query);
    });
}