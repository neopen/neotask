(function () {
  'use strict';

  var header = document.querySelector('.site-header');
  var navToggle = document.getElementById('navToggle');
  var siteNav = document.getElementById('siteNav');

  function closeNav() {
    if (!header || !navToggle) return;
    header.classList.remove('is-open');
    if (siteNav) siteNav.classList.remove('is-open');
    navToggle.setAttribute('aria-expanded', 'false');
    navToggle.setAttribute('aria-label', '打开菜单');
    document.body.classList.remove('nav-open');
  }

  if (navToggle && header) {
    navToggle.addEventListener('click', function () {
      var open = siteNav.classList.toggle('is-open');
      header.classList.toggle('is-open', open);
      navToggle.setAttribute('aria-expanded', String(open));
      navToggle.setAttribute('aria-label', open ? '关闭菜单' : '打开菜单');
      document.body.classList.toggle('nav-open', open);
    });
  }

  if (siteNav) {
    siteNav.addEventListener('click', function (e) {
      if (e.target.closest('a')) closeNav();
    });
  }

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') closeNav();
  });

  // Copy buttons
  function legacyCopy(text) {
    return new Promise(function (resolve, reject) {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand('copy');
        resolve();
      } catch (err) {
        reject(err);
      } finally {
        document.body.removeChild(ta);
      }
    });
  }

  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text).catch(function () {
        return legacyCopy(text);
      });
    }
    return legacyCopy(text);
  }

  document.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-copy], [data-copy-target]');
    if (!btn) return;
    var text = btn.getAttribute('data-copy');
    var targetId = btn.getAttribute('data-copy-target');
    if (!text && targetId) {
      var target = document.getElementById(targetId);
      text = target ? target.innerText : '';
    }
    if (!text) return;
    copyText(text).then(function () {
      btn.classList.add('is-copied');
      var label = btn.getAttribute('aria-label');
      if (label && !btn.dataset.originalLabel) btn.dataset.originalLabel = label;
      var textEl = btn.querySelector('[data-copy-label]');
      if (textEl) textEl.textContent = '已复制';
      window.setTimeout(function () {
        btn.classList.remove('is-copied');
        if (textEl) textEl.textContent = '复制';
      }, 1600);
    }).catch(function () {});
  });

  // Docs article code blocks: add copy buttons
  document.querySelectorAll('.article-body pre').forEach(function (pre, i) {
    if (pre.classList.contains('mermaid')) return;
    var wrap = document.createElement('div');
    wrap.className = 'code-shell';
    pre.parentNode.insertBefore(wrap, pre);
    wrap.appendChild(pre);
    pre.id = pre.id || 'doc-code-' + i;
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'doc-copy';
    btn.textContent = '复制';
    btn.setAttribute('data-copy-target', pre.id);
    btn.setAttribute('aria-label', '复制代码');
    wrap.appendChild(btn);
  });

  // Landing code tabs
  var tabButtons = document.querySelectorAll('[data-code-tab]');
  if (tabButtons.length) {
    tabButtons.forEach(function (btn) {
      btn.addEventListener('click', function () {
        var key = btn.getAttribute('data-code-tab');
        tabButtons.forEach(function (b) {
          var active = b === btn;
          b.classList.toggle('is-active', active);
          b.setAttribute('aria-selected', String(active));
        });
        document.querySelectorAll('.code-tab-panel').forEach(function (panel) {
          var active = panel.id === 'code-' + key;
          panel.classList.toggle('is-active', active);
          panel.hidden = !active;
        });
        document.querySelectorAll('.code-block').forEach(function (block) {
          var active = block.id === 'code-' + key + '-src';
          block.classList.toggle('is-active', active);
          block.hidden = !active;
        });
        var copyBtn = document.querySelector('.code-window .code-copy');
        if (copyBtn) copyBtn.setAttribute('data-copy-target', 'code-' + key + '-src');
      });
    });
  }

  // Docs version switcher
  document.querySelectorAll('[data-docs-version]').forEach(function (select) {
    select.addEventListener('change', function () {
      var id = select.value;
      if (!/^[a-z0-9.-]+$/.test(id)) return;
      window.location.href = '/docs/' + id + '/';
    });
  });

  // Docs sidebar search
  var docsSearch = document.getElementById('docsSearch');
  if (docsSearch) {
    var navLinks = Array.prototype.slice.call(document.querySelectorAll('#docsNav .docs-nav-link'));
    var navGroups = Array.prototype.slice.call(document.querySelectorAll('#docsNav .docs-nav-group'));
    var navEmpty = document.getElementById('docsNavEmpty');

    var filterNav = function () {
      var q = docsSearch.value.trim().toLowerCase();
      var visible = 0;
      navLinks.forEach(function (link) {
        var haystack = (link.getAttribute('data-search') || link.textContent).toLowerCase();
        var match = !q || haystack.indexOf(q) !== -1;
        link.hidden = !match;
        if (match) visible++;
      });
      navGroups.forEach(function (group) {
        var any = group.querySelector('.docs-nav-link:not([hidden])');
        group.hidden = !any;
      });
      if (navEmpty) navEmpty.hidden = visible > 0;
    };

    docsSearch.addEventListener('input', filterNav);
    docsSearch.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        docsSearch.value = '';
        filterNav();
        docsSearch.blur();
      }
    });
  }

  // Docs hub search
  var hubSearch = document.getElementById('docsHubSearch');
  if (hubSearch) {
    var cards = Array.prototype.slice.call(document.querySelectorAll('[data-doc-card]'));
    var groups = Array.prototype.slice.call(document.querySelectorAll('[data-doc-group]'));
    var hubEmpty = document.getElementById('docsHubEmpty');

    var filterHub = function () {
      var q = hubSearch.value.trim().toLowerCase();
      var visible = 0;
      cards.forEach(function (card) {
        var haystack = (card.getAttribute('data-doc-search') || card.textContent).toLowerCase();
        var match = !q || haystack.indexOf(q) !== -1;
        card.hidden = !match;
        if (match) visible++;
      });
      groups.forEach(function (group) {
        var any = group.querySelector('[data-doc-card]:not([hidden])');
        group.hidden = !any;
      });
      if (hubEmpty) hubEmpty.hidden = visible > 0;
    };

    hubSearch.addEventListener('input', filterHub);
    hubSearch.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        hubSearch.value = '';
        filterHub();
        hubSearch.blur();
      }
    });

    // "/" focuses docs search
    document.addEventListener('keydown', function (e) {
      if (e.key === '/' && !/^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName)) {
        e.preventDefault();
        hubSearch.focus();
      }
    });
  }

  // Right-hand table of contents active state
  var tocLinks = Array.prototype.slice.call(document.querySelectorAll('.docs-toc a'));
  if (tocLinks.length && 'IntersectionObserver' in window) {
    var byId = {};
    var headings = [];
    tocLinks.forEach(function (link) {
      var id = decodeURIComponent(link.getAttribute('href').slice(1));
      var heading = document.getElementById(id);
      if (heading) {
        byId[id] = link;
        headings.push(heading);
      }
    });

    var setActive = function (id) {
      tocLinks.forEach(function (link) { link.classList.remove('is-active'); });
      if (byId[id]) byId[id].classList.add('is-active');
    };

    var observer = new IntersectionObserver(function (entries) {
      var visible = entries.filter(function (entry) { return entry.isIntersecting; });
      if (visible.length) {
        visible.sort(function (a, b) { return a.boundingClientRect.top - b.boundingClientRect.top; });
        setActive(visible[0].target.id);
      }
    }, { rootMargin: '-90px 0px -70% 0px', threshold: [0, 1] });

    headings.forEach(function (h) { observer.observe(h); });
  }

  // Smooth in-page anchors with sticky header offset
  document.addEventListener('click', function (e) {
    var link = e.target.closest('a[href^="#"]');
    if (!link) return;
    var hash = link.getAttribute('href');
    if (!hash || hash === '#') return;
    var target = document.getElementById(decodeURIComponent(hash.slice(1)));
    if (!target) return;
    e.preventDefault();
    var top = target.getBoundingClientRect().top + window.pageYOffset - 84;
    window.scrollTo({ top: top, behavior: 'smooth' });
    history.replaceState(null, '', hash);
  });
})();
