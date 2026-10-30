#!/usr/bin/env node
/**
 * NeoTask website build.
 * Source: site/{data,pages,partials,content} -> static HTML at repo root and /docs.
 * Docs are versioned: /docs/ lists versions, /docs/<version>/ holds each doc set.
 * No dependencies; run with `node site/build.mjs`.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SITE = path.join(ROOT, 'site');

const readJson = (p) => JSON.parse(fs.readFileSync(p, 'utf8'));
const data = readJson(path.join(SITE, 'data', 'site.json'));
const manifest = readJson(path.join(SITE, 'data', 'docs.json'));
const versions = manifest.versions.map((v) => ({
  ...v,
  doc: readJson(path.join(SITE, 'data', v.data))
}));
const latest = versions.find((v) => v.id === manifest.default) || versions[0];
const latestPages = latest.doc.pages;

const esc = (s) => String(s)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;');

const docsBase = (v) => `/docs/${v.id}`;
const docsHref = (v, slug) => `${docsBase(v)}/${slug}.html`;

const baseVars = {
  NAME: data.name,
  VERSION: data.version,
  RELEASE_DATE: data.releaseDate,
  DESCRIPTION: data.description,
  HOMEPAGE: data.homepage,
  REPO: data.repo,
  PYPI: data.pypi,
  ISSUES: data.issues,
  DISCUSSIONS: data.discussions,
  CHANGELOG_URL: data.changelog,
  EXAMPLES: data.examples,
  BLOG: data.blog,
  LICENSE: data.license,
  EMAIL: data.email,
  PYTHON: data.python,
  STARS: String(data.stats.stars),
  FORKS: String(data.stats.forks),
  DOCS_COUNT: String(latestPages.length),
  DOCS_URL: docsBase(latest),
  DOCS_VERSION: latest.label,
  DOCS_LIBRARY: latest.library,
  DOCS_BASELINE: latest.baseline,
  DOCS_UPDATED: latest.updated,
  YEAR: '2026',
  ACTIVE_HOME: '',
  ACTIVE_FEATURES: '',
  ACTIVE_DOCS: '',
  ACTIVE_CHANGELOG: '',
  ACTIVE_COMMUNITY: ''
};

function applyVars(html, extra = {}) {
  const vars = { ...baseVars, ...extra };
  return html.replace(/\{\{([A-Z0-9_]+)\}\}/g, (m, key) =>
    Object.prototype.hasOwnProperty.call(vars, key) ? vars[key] : m
  );
}

function includePartials(html) {
  return html.replace(/<!--\s*@include\s+([\w-]+)\s*-->/g, (m, name) => {
    const file = path.join(SITE, 'partials', `${name}.html`);
    if (!fs.existsSync(file)) throw new Error(`Missing partial: ${name}`);
    return fs.readFileSync(file, 'utf8');
  });
}

// Rewrite unversioned docs links to the current (or latest known) doc set.
function rewriteDocsLinks(html, version) {
  return html.replace(/\/docs\/([a-z0-9-]+)\.html/g, (match, slug) => {
    if (version.doc.pages.some((p) => p.slug === slug)) return docsHref(version, slug);
    if (version.id !== latest.id && latestPages.some((p) => p.slug === slug)) {
      return docsHref(latest, slug);
    }
    return match;
  });
}

const iconPaths = {
  rocket: '<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>',
  sliders: '<line x1="4" y1="21" x2="4" y2="14"/><line x1="4" y1="10" x2="4" y2="3"/><line x1="12" y1="21" x2="12" y2="12"/><line x1="12" y1="8" x2="12" y2="3"/><line x1="20" y1="21" x2="20" y2="16"/><line x1="20" y1="12" x2="20" y2="3"/><line x1="1" y1="14" x2="7" y2="14"/><line x1="9" y1="8" x2="15" y2="8"/><line x1="17" y1="16" x2="23" y2="16"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  layers: '<path d="m12 2 9 5-9 5-9-5 9-5z"/><path d="m3 12 9 5 9-5"/><path d="m3 17 9 5 9-5"/>',
  blocks: '<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
  nodes: '<circle cx="5" cy="6" r="2.5"/><circle cx="19" cy="6" r="2.5"/><circle cx="12" cy="18" r="2.5"/><path d="M7.5 6h9M6.5 8.2l4 7.6M17.5 8.2l-4 7.6"/>',
  database: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.66 3.58 3 8 3s8-1.34 8-3V5"/><path d="M4 12c0 1.66 3.58 3 8 3s8-1.34 8-3"/>',
  sort: '<path d="m3 8 4-4 4 4"/><path d="M7 4v16"/><path d="m21 16-4 4-4-4"/><path d="M17 20V4"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="m9 12 2 2 4-4"/>',
  lock: '<rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>',
  gauge: '<path d="M12 14 17 9"/><path d="M3.34 19a10 10 0 1 1 17.32 0"/><circle cx="12" cy="14" r="1"/>',
  activity: '<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
  code: '<path d="m16 18 6-6-6-6"/><path d="m8 6-6 6 6 6"/>',
  chart: '<path d="M3 3v18h18"/><rect x="7" y="12" width="3" height="6" rx="1"/><rect x="12" y="8" width="3" height="10" rx="1"/><rect x="17" y="5" width="3" height="13" rx="1"/>',
  history: '<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l4 2"/>',
  workflow: '<rect x="3" y="3" width="7" height="6" rx="1.5"/><rect x="14" y="15" width="7" height="6" rx="1.5"/><path d="M6.5 9v5a4 4 0 0 0 4 4H14"/>'
};

const icon = (name, cls = 'icon') =>
  `<svg class="${cls}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${iconPaths[name] || iconPaths.layers}</svg>`;

const categoryLabel = (v, id) => v.doc.categories.find((c) => c.id === id)?.label || id;

function versionOptions(current) {
  return versions.map((v) => `<option value="${v.id}"${v.id === current.id ? ' selected' : ''}>${v.label}${v.status === 'latest' ? '（最新）' : ''}</option>`).join('');
}

function renderVersionPicker(v) {
  return `<div class="docs-version-picker">
        <span class="docs-version-caption">文档版本</span>
        <div class="docs-version-row">
          <select class="docs-version-select" data-docs-version aria-label="切换文档版本">
            ${versionOptions(v)}
          </select>
          <a class="docs-version-all" href="/docs/">所有版本</a>
        </div>
        <p class="docs-version-note">适用于 ${esc(v.library)}</p>
      </div>`;
}

function renderSidebar(v, activeSlug, includeSearch = true) {
  const groups = v.doc.categories.map((cat) => {
    const links = v.doc.pages
      .filter((p) => p.category === cat.id)
      .map((p) => `<a class="docs-nav-link${p.slug === activeSlug ? ' is-active' : ''}" href="${docsHref(v, p.slug)}" data-search="${esc(`${p.title} ${p.description}`)}">${esc(p.title)}${p.category === 'roadmap' ? '<span class="nav-soon">规划</span>' : ''}</a>`)
      .join('\n        ');
    return `<div class="docs-nav-group">
        <p class="docs-nav-label">${esc(cat.label)}</p>
        ${links}
      </div>`;
  });
  const search = includeSearch ? `<div class="docs-search-wrap">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
        <input id="docsSearch" type="search" placeholder="搜索文档" autocomplete="off" aria-label="搜索文档">
      </div>` : '';
  const navId = includeSearch ? 'docsNav' : 'docsNavMobile';
  return `${renderVersionPicker(v)}
      ${search}
      <nav class="docs-nav" id="${navId}" aria-label="文档导航">
        ${groups.join('\n        ')}
      </nav>
      ${includeSearch ? '<p class="docs-nav-empty" id="docsNavEmpty" hidden>没有匹配的文档</p>' : ''}`;
}

function renderToc(content) {
  const headings = [];
  const seen = new Map();
  const withIds = content.replace(/<h2(\s[^>]*)?>([\s\S]*?)<\/h2>/g, (m, attrs = '', inner) => {
    const text = inner.replace(/<[^>]+>/g, '').trim();
    if (!text) return m;
    const count = seen.get(text) || 0;
    seen.set(text, count + 1);
    const id = count ? `${text}-${count}` : text;
    headings.push({ id, text });
    if (/\sid=/.test(attrs)) return m;
    return `<h2${attrs} id="${esc(id)}">${inner}</h2>`;
  });
  const items = headings
    .slice(0, 14)
    .map((h) => `<a href="#${encodeURIComponent(h.id)}">${esc(h.text)}</a>`)
    .join('');
  return {
    content: withIds,
    toc: items ? `<nav class="docs-toc" aria-label="本页目录"><p class="docs-toc-label">本页目录</p>${items}</nav>` : ''
  };
}

function docsPageHead(v, p) {
  const title = `${p.title} · NeoTask ${v.label} 文档`;
  const url = `${data.homepage}${docsHref(v, p.slug)}`;
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${esc(title)}</title>
  <meta name="description" content="${esc(p.description)}">
  <link rel="canonical" href="${url}">
  <meta property="og:site_name" content="NeoTask 文档">
  <meta property="og:type" content="article">
  <meta property="og:title" content="${esc(title)}">
  <meta property="og:description" content="${esc(p.description)}">
  <meta property="og:url" content="${url}">
  <meta property="og:image" content="${data.homepage}/assets/img/og.png">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:image" content="${data.homepage}/assets/img/og.png">
  <link rel="icon" href="/assets/img/favicon.svg" type="image/svg+xml">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap">
  <link rel="stylesheet" href="/assets/css/site.css">
  <link rel="stylesheet" href="/assets/css/docs.css">
</head>`;
}

function buildDocsPage(v, p) {
  const contentFile = path.join(SITE, 'content', 'docs', v.id, `${p.slug}.html`);
  if (!fs.existsSync(contentFile)) {
    throw new Error(`Missing docs content: ${path.relative(ROOT, contentFile)}`);
  }
  const outFile = path.join(ROOT, 'docs', v.id, `${p.slug}.html`);

  const raw = fs.readFileSync(contentFile, 'utf8');
  const { content, toc } = renderToc(raw);
  const idx = v.doc.pages.findIndex((x) => x.slug === p.slug);
  const prev = v.doc.pages[idx - 1];
  const next = v.doc.pages[idx + 1];
  const pager = `<nav class="docs-pager" aria-label="上一篇与下一篇">
      ${prev ? `<a class="pager-prev" href="${docsHref(v, prev.slug)}"><span>上一篇</span><strong>${esc(prev.title)}</strong></a>` : '<span></span>'}
      ${next ? `<a class="pager-next" href="${docsHref(v, next.slug)}"><span>下一篇</span><strong>${esc(next.title)}</strong></a>` : '<span></span>'}
    </nav>`;
  const mermaid = /class="mermaid"/.test(content)
    ? `\n  <script type="module">import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs'; mermaid.initialize({ startOnLoad: true, theme: 'neutral' });</script>`
    : '';

  const page = `${docsPageHead(v, p)}
<body class="docs-page">
  <!-- @include header -->
  <div class="docs-shell">
    <aside class="docs-sidebar" id="docsSidebar">
      <a class="docs-sidebar-home" href="/docs/"><span class="docs-sidebar-badge">DOCS</span> 文档中心</a>
      ${renderSidebar(v, p.slug)}
    </aside>
    <main class="docs-main" id="main">
      <nav class="breadcrumb" aria-label="面包屑">
        <a href="/">首页</a><span>/</span><a href="/docs/">文档</a><span>/</span><a href="${docsBase(v)}/">${esc(v.label)}</a><span>/</span><span>${esc(categoryLabel(v, p.category))}</span>
      </nav>
      <details class="docs-mobile-nav">
        <summary>文档导航 · ${esc(v.label)}</summary>
        ${renderSidebar(v, p.slug, false)}
      </details>
      <article class="article">
        <header class="article-header">
          <p class="article-eyebrow">${esc(categoryLabel(v, p.category))}</p>
          <h1>${esc(p.title)}</h1>
          <p class="article-lede">${esc(p.description)}</p>
          <div class="article-meta">
            <span class="article-version">${esc(v.label)} · ${esc(v.library)}</span>
            <span class="dot"></span>
            <span>更新于 ${esc(p.updated)}</span>
            <span class="dot"></span>
            <span>约 ${esc(p.time)} 读完</span>
            <span class="dot"></span>
            <a href="${data.repo}" target="_blank" rel="noopener">GitHub ${icon('code', 'icon-xs')}</a>
          </div>
        </header>
        <div class="article-body">
${content}
        </div>
      </article>
      ${pager}
      <p class="docs-feedback">发现文档问题？欢迎在 <a href="${data.issues}" target="_blank" rel="noopener">GitHub Issues</a> 反馈。</p>
    </main>
    ${toc}
  </div>
  <!-- @include footer -->
  <script src="/assets/js/site.js" defer></script>${mermaid}
</body>
</html>
`;

  let rendered = includePartials(page);
  rendered = applyVars(rendered, {
    ACTIVE_DOCS: 'is-active',
    DOCS_VERSION: v.label,
    DOCS_LIBRARY: v.library,
    DOCS_BASELINE: v.baseline
  });
  rendered = rewriteDocsLinks(rendered, v);
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, rendered, 'utf8');
}

function renderDocsHub(v) {
  const featured = v.doc.pages.filter((p) => p.featured).map((p) => `
        <a class="doc-feature" href="${docsHref(v, p.slug)}" data-doc-card data-doc-search="${esc(`${p.title} ${p.description} ${categoryLabel(v, p.category)}`)}">
          <span class="doc-feature-icon">${icon(p.icon)}</span>
          <span class="doc-feature-body">
            <span class="doc-feature-cat">${esc(categoryLabel(v, p.category))}</span>
            <strong>${esc(p.title)}</strong>
            <span>${esc(p.description)}</span>
          </span>
          <span class="doc-feature-arrow" aria-hidden="true">→</span>
        </a>`).join('');

  const groups = v.doc.categories.map((cat) => {
    const cards = v.doc.pages.filter((p) => p.category === cat.id).map((p) => `
          <a class="doc-card" href="${docsHref(v, p.slug)}" data-doc-card data-doc-search="${esc(`${p.title} ${p.description} ${cat.label}`)}">
            <span class="doc-card-icon">${icon(p.icon)}</span>
            <span class="doc-card-title">${esc(p.title)}${p.category === 'roadmap' ? '<em class="tag-soon">规划</em>' : ''}</span>
            <span class="doc-card-desc">${esc(p.description)}</span>
            <span class="doc-card-meta">${esc(p.time)}</span>
          </a>`).join('');
    return `
      <section class="doc-group" data-doc-group id="${cat.id}">
        <div class="doc-group-head">
          <h2>${esc(cat.label)}</h2>
          <span>${String(v.doc.pages.filter((p) => p.category === cat.id).length).padStart(2, '0')}</span>
        </div>
        <div class="doc-card-grid">${cards}
        </div>
      </section>`;
  }).join('');

  return { featured, groups };
}

function buildPage(name, outRel, version, extraVars = {}) {
  let html = fs.readFileSync(path.join(SITE, 'pages', name), 'utf8');
  html = includePartials(html);
  html = applyVars(html, extraVars);
  html = rewriteDocsLinks(html, version);
  const outFile = path.join(ROOT, outRel);
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, html, 'utf8');
}

function writeRedirect(outRel, target, title) {
  const url = target.startsWith('http') ? target : `${data.homepage}${target}`;
  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="robots" content="noindex">
  <title>${esc(title)} · NeoTask 文档</title>
  <link rel="canonical" href="${url}">
  <meta http-equiv="refresh" content="0; url=${target}">
  <script>location.replace(${JSON.stringify(target)});</script>
</head>
<body>
  <p>此页面已迁移至 <a href="${target}">${esc(target)}</a>。</p>
</body>
</html>
`;
  const outFile = path.join(ROOT, outRel);
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, html, 'utf8');
}

// --- Clean generated output -------------------------------------------------
fs.rmSync(path.join(ROOT, 'docs'), { recursive: true, force: true });
fs.mkdirSync(path.join(ROOT, 'docs'), { recursive: true });

// --- Generate docs articles -------------------------------------------------
for (const v of versions) {
  for (const p of v.doc.pages) buildDocsPage(v, p);
}

// --- Generate version hubs --------------------------------------------------
for (const v of versions) {
  const hub = renderDocsHub(v);
  buildPage('docs.html', `docs/${v.id}/index.html`, v, {
    ACTIVE_DOCS: 'is-active',
    DOCS_VERSION: v.label,
    DOCS_LIBRARY: v.library,
    DOCS_BASELINE: v.baseline,
    DOCS_UPDATED: v.updated,
    DOCS_VERSION_OPTIONS: versionOptions(v),
    DOCS_FEATURED: hub.featured,
    DOCS_GROUPS: hub.groups
  });
}

// --- Version center ---------------------------------------------------------
const versionCards = versions.map((v) => `
      <article class="version-card${v.status === 'latest' ? ' is-latest' : ''}">
        <div class="version-card-head">
          <div>
            <p class="version-card-label">${esc(v.label)}</p>
            <h2>${esc(v.library)}</h2>
          </div>
          ${v.status === 'latest' ? '<span class="version-badge">最新</span>' : '<span class="version-badge is-muted">维护中</span>'}
        </div>
        <p class="version-card-desc">${esc(v.description)}</p>
        <dl class="version-card-meta">
          <div><dt>文档基线</dt><dd>v${esc(v.baseline)}</dd></div>
          <div><dt>适用版本</dt><dd>${esc(v.library)}</dd></div>
          <div><dt>更新于</dt><dd>${esc(v.updated)}</dd></div>
          <div><dt>文档数量</dt><dd>${v.doc.pages.length} 篇</dd></div>
        </dl>
        <a class="btn btn-primary btn-block" href="${docsBase(v)}/">进入 ${esc(v.label)} 文档 <span aria-hidden="true">→</span></a>
      </article>`).join('');

const latestScope = latest.doc.categories.map((cat) => {
  const count = latest.doc.pages.filter((p) => p.category === cat.id).length;
  return `
          <div>
            <h3><a href="${docsBase(latest)}/#${cat.id}">${esc(cat.label)} · ${count} 篇</a></h3>
            <p>${esc(cat.description || '')}</p>
          </div>`;
}).join('');

buildPage('docs-versions.html', 'docs/index.html', latest, {
  ACTIVE_DOCS: 'is-active',
  VERSION_CARDS: versionCards,
  LATEST_SCOPE: latestScope,
  VERSION_COUNT: String(versions.length),
  LATEST_VERSION: latest.label,
  LATEST_VERSION_ID: latest.id,
  LATEST_LIBRARY: latest.library,
  LATEST_BASELINE: latest.baseline,
  LATEST_UPDATED: latest.updated
});

// --- Legacy unversioned docs routes ---------------------------------------
const retired = {
  'core-features': 'task-lifecycle',
  decentralization: 'distributed',
  'dag-workflow': 'roadmap'
};
for (const v of versions.filter((x) => x.legacy)) {
  for (const slug of v.legacySlugs || []) {
    const p = v.doc.pages.find((x) => x.slug === slug);
    if (p) writeRedirect(`docs/${p.slug}.html`, docsHref(v, p.slug), p.title);
  }
  for (const [from, to] of Object.entries(retired)) {
    if (v.doc.pages.some((p) => p.slug === to)) {
      writeRedirect(`docs/${from}.html`, docsHref(v, to), to);
    }
  }
}
if (latestPages.some((p) => p.slug === 'roadmap')) {
  writeRedirect('docs/evolution.html', '/changelog.html', '版本路线图');
}

// --- Generate changelog -----------------------------------------------------
const typeLabel = { major: '主要版本', minor: '功能版本', patch: '修复版本', alpha: '内测版本' };
const releaseList = data.releases.map((r, i) => `
      <article class="release${i === 0 ? ' is-latest' : ''}" id="v${r.version}">
        <div class="release-side">
          <div class="release-version">v${r.version}${i === 0 ? '<span class="release-latest">最新</span>' : ''}</div>
          <time datetime="${r.date}">${r.date}</time>
          <span class="release-type ${r.type}">${typeLabel[r.type]}</span>
        </div>
        <div class="release-body">
          <h2>${esc(r.summary)}</h2>
          <ul>
            ${r.items.map((item) => `<li>${esc(item)}</li>`).join('\n            ')}
          </ul>
          <a class="text-link" href="${r.url}" target="_blank" rel="noopener">查看完整发布说明 <span aria-hidden="true">→</span></a>
        </div>
      </article>`).join('');
buildPage('changelog.html', 'changelog.html', latest, {
  ACTIVE_CHANGELOG: 'is-active',
  RELEASES: releaseList
});

// --- Other top-level pages --------------------------------------------------
buildPage('index.html', 'index.html', latest, { ACTIVE_HOME: 'is-active' });
buildPage('features.html', 'features.html', latest, { ACTIVE_FEATURES: 'is-active' });
buildPage('community.html', 'community.html', latest, { ACTIVE_COMMUNITY: 'is-active' });
buildPage('404.html', '404.html', latest, {});

// --- Sitemap ---------------------------------------------------------------
const urls = [
  '/',
  '/features.html',
  '/changelog.html',
  '/community.html',
  '/docs/',
  `${docsBase(latest)}/`,
  ...latestPages.map((p) => docsHref(latest, p.slug))
];
const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map((u) => `  <url><loc>${data.homepage}${u}</loc></url>`).join('\n')}
</urlset>
`;
fs.writeFileSync(path.join(ROOT, 'sitemap.xml'), sitemap, 'utf8');
fs.writeFileSync(path.join(ROOT, 'robots.txt'), `User-agent: *\nAllow: /\n\nSitemap: ${data.homepage}/sitemap.xml\n`, 'utf8');

console.log(`Built ${versions.length} docs version(s), ${latestPages.length} latest pages, 4 site pages.`);
