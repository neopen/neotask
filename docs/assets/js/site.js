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
    if (pre.classList.contains('mermaid') || pre.classList.contains('diagram')) return;
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

  // Docs diagrams: click to view fullscreen with zoom and pan
  (function () {
    var sources = Array.prototype.slice.call(document.querySelectorAll('.article-body pre.diagram, .article-body pre.mermaid'));
    if (!sources.length) return;

    var MIN_SCALE = 0.25;
    var MAX_SCALE = 8;
    var box = null;
    var stage = null;
    var canvas = null;
    var content = null;
    var titleEl = null;
    var levelEl = null;
    var lastFocus = null;
    var fitScale = 1;
    var scale = 1;
    var tx = 0;
    var ty = 0;
    var pointers = new Map();
    var drag = null;
    var pinch = null;
    var suppressClick = false;

    function clamp(value, lo, hi) {
      return Math.min(hi, Math.max(lo, value));
    }

    function headingFor(pre) {
      var node = pre;
      while (node && node !== document.body) {
        var sib = node.previousElementSibling;
        while (sib) {
          if (/^H[1-4]$/.test(sib.tagName)) return sib.textContent.trim();
          sib = sib.previousElementSibling;
        }
        node = node.parentElement;
      }
      var h1 = document.querySelector('.article-header h1');
      return h1 ? h1.textContent.trim() : '图形预览';
    }

    function apply() {
      canvas.style.transform = 'translate3d(' + tx + 'px,' + ty + 'px,0) scale(' + scale + ')';
      if (levelEl) levelEl.textContent = Math.round((scale / fitScale) * 100) + '%';
    }

    function fit() {
      if (!stage) return;
      canvas.style.transform = 'none';
      var pad = 48;
      var availW = Math.max(120, stage.clientWidth - pad * 2);
      var availH = Math.max(120, stage.clientHeight - pad * 2);
      var w = content.offsetWidth || 1;
      var h = content.offsetHeight || 1;
      fitScale = clamp(Math.min(availW / w, availH / h), 0.8, 2.6);
      scale = fitScale;
      tx = 0;
      ty = 0;
      apply();
    }

    function zoomBy(factor, clientX, clientY) {
      var next = clamp(scale * factor, MIN_SCALE, MAX_SCALE);
      if (Math.abs(next - scale) < 0.0001) return;
      if (typeof clientX === 'number' && typeof clientY === 'number') {
        var rect = stage.getBoundingClientRect();
        var ox = clientX - rect.left - rect.width / 2;
        var oy = clientY - rect.top - rect.height / 2;
        var k = next / scale;
        tx = ox - (ox - tx) * k;
        ty = oy - (oy - ty) * k;
      }
      scale = next;
      apply();
    }

    function distance(a, b) {
      return Math.hypot(a.x - b.x, a.y - b.y);
    }

    function build() {
      box = document.createElement('div');
      box.className = 'diagram-lightbox';
      box.setAttribute('role', 'dialog');
      box.setAttribute('aria-modal', 'true');
      box.setAttribute('aria-hidden', 'true');
      box.setAttribute('aria-label', '图形全屏预览');
      box.innerHTML = '<div class="diagram-lightbox-bar">' +
        '<p class="diagram-lightbox-title" data-diagram-title></p>' +
        '<div class="diagram-lightbox-tools">' +
        '<button type="button" class="diagram-tool" data-diagram-zoom="out" aria-label="缩小">−</button>' +
        '<button type="button" class="diagram-tool diagram-tool-level" data-diagram-reset aria-label="重置缩放">100%</button>' +
        '<button type="button" class="diagram-tool" data-diagram-zoom="in" aria-label="放大">+</button>' +
        '<button type="button" class="diagram-tool diagram-tool-close" data-diagram-close aria-label="关闭预览">✕</button>' +
        '</div></div>' +
        '<div class="diagram-lightbox-stage" data-diagram-stage>' +
        '<div class="diagram-lightbox-canvas" data-diagram-canvas>' +
        '<pre class="diagram-lightbox-content" data-diagram-content></pre>' +
        '</div>' +
        '<p class="diagram-lightbox-hint"><span class="hint-desktop">滚轮 / 双击缩放 · 拖拽移动 · Esc 关闭</span><span class="hint-mobile">双指缩放 · 拖拽移动</span></p></div>';
      document.body.appendChild(box);

      titleEl = box.querySelector('[data-diagram-title]');
      levelEl = box.querySelector('[data-diagram-reset]');
      stage = box.querySelector('[data-diagram-stage]');
      canvas = box.querySelector('[data-diagram-canvas]');
      content = box.querySelector('[data-diagram-content]');

      box.addEventListener('click', function (e) {
        if (e.target.closest('[data-diagram-close]')) {
          close();
          return;
        }
        var zoomBtn = e.target.closest('[data-diagram-zoom]');
        if (zoomBtn) {
          zoomBy(zoomBtn.getAttribute('data-diagram-zoom') === 'in' ? 1.25 : 0.8);
          return;
        }
        if (e.target.closest('[data-diagram-reset]')) fit();
      });

      stage.addEventListener('wheel', function (e) {
        e.preventDefault();
        zoomBy(Math.exp(-e.deltaY * 0.0016), e.clientX, e.clientY);
      }, { passive: false });

      stage.addEventListener('dblclick', function (e) {
        if (scale > fitScale * 1.5) fit();
        else zoomBy((fitScale * 2.6) / scale, e.clientX, e.clientY);
      });

      stage.addEventListener('pointerdown', function (e) {
        if (e.pointerType === 'mouse' && e.button !== 0) return;
        pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
        try {
          stage.setPointerCapture(e.pointerId);
        } catch (err) {}
        suppressClick = false;
        if (pointers.size === 1) {
          drag = { x: e.clientX, y: e.clientY, tx: tx, ty: ty, moved: false };
          pinch = null;
          stage.classList.add('is-dragging');
        } else if (pointers.size === 2) {
          var pts = Array.from(pointers.values());
          pinch = {
            dist: distance(pts[0], pts[1]),
            midX: (pts[0].x + pts[1].x) / 2,
            midY: (pts[0].y + pts[1].y) / 2
          };
          drag = null;
        }
      });

      stage.addEventListener('pointermove', function (e) {
        if (!pointers.has(e.pointerId)) return;
        pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });

        if (pointers.size >= 2 && pinch) {
          var pts = Array.from(pointers.values()).slice(0, 2);
          var dist = distance(pts[0], pts[1]);
          var midX = (pts[0].x + pts[1].x) / 2;
          var midY = (pts[0].y + pts[1].y) / 2;
          if (pinch.dist > 0) zoomBy(dist / pinch.dist, midX, midY);
          tx += midX - pinch.midX;
          ty += midY - pinch.midY;
          pinch.dist = dist;
          pinch.midX = midX;
          pinch.midY = midY;
          suppressClick = true;
          apply();
          return;
        }

        if (drag && pointers.size === 1) {
          var dx = e.clientX - drag.x;
          var dy = e.clientY - drag.y;
          if (!drag.moved && Math.hypot(dx, dy) > 3) {
            drag.moved = true;
            suppressClick = true;
          }
          if (drag.moved) {
            tx = drag.tx + dx;
            ty = drag.ty + dy;
            apply();
          }
        }
      });

      function release(e) {
        pointers.delete(e.pointerId);
        if (pointers.size === 1) {
          var rest = Array.from(pointers.values())[0];
          drag = { x: rest.x, y: rest.y, tx: tx, ty: ty, moved: true };
          pinch = null;
        } else if (pointers.size === 0) {
          drag = null;
          pinch = null;
          stage.classList.remove('is-dragging');
        }
      }

      stage.addEventListener('pointerup', release);
      stage.addEventListener('pointercancel', release);

      stage.addEventListener('click', function (e) {
        var suppressed = suppressClick;
        suppressClick = false;
        if (suppressed || e.target.closest('.diagram-lightbox-content')) return;
        close();
      });

      document.addEventListener('keydown', function (e) {
        if (!box.classList.contains('is-open')) return;
        if (e.key === 'Escape') {
          close();
          return;
        }
        if (e.key === '+' || e.key === '=') {
          e.preventDefault();
          zoomBy(1.25);
        } else if (e.key === '-' || e.key === '_') {
          e.preventDefault();
          zoomBy(0.8);
        } else if (e.key === '0') {
          e.preventDefault();
          fit();
        } else if (e.key === 'Tab') {
          var focusable = box.querySelectorAll('button');
          var first = focusable[0];
          var last = focusable[focusable.length - 1];
          if (e.shiftKey && document.activeElement === first) {
            e.preventDefault();
            last.focus();
          } else if (!e.shiftKey && document.activeElement === last) {
            e.preventDefault();
            first.focus();
          }
        }
      });

      window.addEventListener('resize', function () {
        if (box.classList.contains('is-open')) fit();
      });
    }

    function open(pre) {
      if (!box) build();
      lastFocus = document.activeElement;
      titleEl.textContent = headingFor(pre);

      content.className = 'diagram-lightbox-content';
      content.textContent = '';
      var svg = pre.querySelector('svg');
      if (svg) {
        var clone = svg.cloneNode(true);
        var vb = svg.viewBox && svg.viewBox.baseVal;
        clone.style.maxWidth = 'none';
        if (vb && vb.width) {
          clone.setAttribute('width', vb.width);
          clone.setAttribute('height', vb.height);
        }
        content.classList.add('is-mermaid');
        content.appendChild(clone);
      } else {
        var code = pre.querySelector('code') || pre;
        content.textContent = code.textContent.replace(/\s+$/, '');
      }

      document.body.classList.add('diagram-open');
      box.classList.add('is-open');
      box.setAttribute('aria-hidden', 'false');
      fit();
      box.querySelector('[data-diagram-close]').focus();
    }

    function close() {
      if (!box || !box.classList.contains('is-open')) return;
      box.classList.remove('is-open');
      box.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('diagram-open');
      if (lastFocus && typeof lastFocus.focus === 'function') lastFocus.focus();
    }

    sources.forEach(function (pre) {
      var wrap = document.createElement('div');
      wrap.className = 'code-shell diagram-shell';
      pre.parentNode.insertBefore(wrap, pre);
      wrap.appendChild(pre);

      var btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'doc-copy doc-expand';
      btn.textContent = '放大';
      btn.setAttribute('aria-label', '全屏查看图形');
      btn.addEventListener('click', function () {
        open(pre);
      });
      wrap.appendChild(btn);

      pre.setAttribute('role', 'button');
      pre.setAttribute('tabindex', '0');
      pre.setAttribute('aria-label', '全屏查看：' + headingFor(pre));
      pre.setAttribute('title', '点击全屏查看');
      pre.addEventListener('click', function () {
        open(pre);
      });
      pre.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar') {
          e.preventDefault();
          open(pre);
        }
      });
    });
  })();

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

  // Scroll progress bar and reveal-on-scroll
  (function () {
    var reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    var bar = document.createElement('div');
    bar.className = 'scroll-progress';
    bar.setAttribute('aria-hidden', 'true');
    document.body.appendChild(bar);

    var docEl = document.scrollingElement || document.documentElement;
    var pending = [];
    var observer = null;
    var ticking = false;

    function updateProgress() {
      var max = docEl.scrollHeight - window.innerHeight;
      var ratio = max > 0 ? docEl.scrollTop / max : 0;
      bar.style.transform = 'scaleX(' + Math.min(1, Math.max(0, ratio)) + ')';
    }

    function reveal(el) {
      if (!el.classList.contains('reveal') || el.classList.contains('is-visible')) return;
      el.classList.add('is-visible');
      if (observer) observer.unobserve(el);
      var index = pending.indexOf(el);
      if (index !== -1) pending.splice(index, 1);
      var delay = parseFloat(el.style.getPropertyValue('--reveal-delay')) || 0;
      window.setTimeout(function () {
        el.classList.remove('reveal', 'is-visible');
        el.style.removeProperty('--reveal-delay');
      }, 800 + delay);
    }

    function revealAll(immediate) {
      pending.slice().forEach(function (el) {
        if (!immediate) return reveal(el);
        if (observer) observer.unobserve(el);
        el.classList.remove('reveal', 'is-visible');
        el.style.removeProperty('--reveal-delay');
        var index = pending.indexOf(el);
        if (index !== -1) pending.splice(index, 1);
      });
    }

    if (!reduceMotion) {
      var selector = [
        '.section-head',
        '.proof-item',
        '.value-card',
        '.bento-card',
        '.code-layout',
        '.compare-wrap',
        '.compare-note',
        '.use-card',
        '.arch-layer',
        '.deploy-panel',
        '.bench-card',
        '.bench-note',
        '.home-doc',
        '.faq-item',
        '.cta-copy',
        '.cta-actions',
        '.feature-row',
        '.contrib-card',
        '.dev-setup',
        '.contrib-steps',
        '.gh-card',
        '.aside-card',
        '.release',
        '.version-card',
        '.doc-feature',
        '.doc-card',
        '.doc-group-head',
        '.article-body > h2',
        '.article-body > ul',
        '.article-body > ol',
        '.article-body > table',
        '.article-body > .doc-note',
        '.article-body > .doc-warning',
        '.article-body .code-shell',
        '.article-body .diagram-shell'
      ].join(',');
      var nodes = Array.prototype.slice.call(document.querySelectorAll(selector));
      var groups = new Map();
      var threshold = window.innerHeight * 0.92;

      nodes.forEach(function (el) {
        if (el.getBoundingClientRect().top < threshold) return;
        el.classList.add('reveal');
        var siblings = groups.get(el.parentElement) || [];
        var delay = Math.min(siblings.length * 60, 240);
        if (delay) el.style.setProperty('--reveal-delay', delay + 'ms');
        siblings.push(el);
        groups.set(el.parentElement, siblings);
        pending.push(el);
      });

      if ('IntersectionObserver' in window) {
        observer = new IntersectionObserver(function (entries) {
          entries.forEach(function (entry) {
            if (entry.isIntersecting) reveal(entry.target);
          });
        }, { rootMargin: '0px 0px -8% 0px', threshold: 0.01 });
        pending.forEach(function (el) { observer.observe(el); });
      }

      var hubSearch = document.getElementById('docsHubSearch');
      if (hubSearch) {
        hubSearch.addEventListener('input', function () { revealAll(true); });
      }
    }

    function onScroll() {
      if (ticking) return;
      ticking = true;
      window.requestAnimationFrame(function () {
        ticking = false;
        updateProgress();
        if (pending.length) {
          var limit = window.innerHeight * 0.95;
          pending.slice().forEach(function (el) {
            if (el.getBoundingClientRect().top < limit) reveal(el);
          });
        }
      });
    }

    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', updateProgress);
    updateProgress();
  })();

  // Smooth in-page anchors with sticky header offset
  document.addEventListener('click', function (e) {
    var link = e.target.closest('a[href^="#"]');
    if (!link) return;
    var hash = link.getAttribute('href');
    if (!hash || hash === '#') return;
    var target = document.getElementById(decodeURIComponent(hash.slice(1)));
    if (!target) return;
    e.preventDefault();
    var top = target.id === 'top' ? 0 : target.getBoundingClientRect().top + window.pageYOffset - 84;
    window.scrollTo({ top: top, behavior: 'smooth' });
    history.replaceState(null, '', hash);
  });
})();
