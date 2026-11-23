#!/usr/bin/env node
/**
 * NeoTask website build.
 * Source: site/{data,pages,partials,content} -> static HTML at repo root (zh) and /en.
 * Docs are versioned: /docs/ lists versions, /docs/<version>/ holds each doc set.
 * No dependencies; run with `node site/build.mjs`.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SITE = path.join(ROOT, 'site');

const readJson = (p) => JSON.parse(fs.readFileSync(p, 'utf8'));

const esc = (s) => String(s)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;');

const LOCALES = [
  {
    id: 'zh',
    outDir: '',
    lang: 'zh-CN',
    ogLocale: 'zh_CN',
    hreflang: 'zh-CN',
    switchHreflang: 'en',
    switchLabel: 'EN',
    switchAria: 'Switch to English',
    dataFile: 'site.json',
    uiFile: 'ui.json',
    manifestFile: 'docs.json',
    contentDir: 'docs',
    pagesDir: '',
    partialsDir: '',
    labels: {
      docsSuffix: '文档',
      prev: '上一篇',
      next: '下一篇',
      toc: '本页目录',
      tocAria: '本页目录',
      back: '回到顶部',
      updated: '更新于',
      readTime: (t) => `约 ${t} 读完`,
      versionCaption: '文档版本',
      versionAria: '切换文档版本',
      allVersions: '所有版本',
      versionNote: (v) => `适用于 ${v.library}`,
      search: '搜索文档',
      searchAria: '搜索文档',
      navAria: '文档导航',
      navEmpty: '没有匹配的文档',
      navSoon: '规划',
      docsCenter: '文档中心',
      crumbHome: '首页',
      crumbDocs: '文档',
      breadcrumbAria: '面包屑',
      mobileNav: (v) => `文档导航 · ${v.label}`,
      pagerAria: '上一篇与下一篇',
      feedback: '发现文档问题？欢迎在 <a href="{ISSUES}" target="_blank" rel="noopener">GitHub Issues</a> 反馈。',
      benchEnvHeaders: ['项目', '配置'],
      benchOutcomeOk: '成功',
      benchOutcomeFail: '受限'
    }
  },
  {
    id: 'en',
    outDir: 'en',
    lang: 'en',
    ogLocale: 'en_US',
    hreflang: 'en',
    switchHreflang: 'zh-CN',
    switchLabel: '中文',
    switchAria: '切换到中文',
    dataFile: 'site.en.json',
    uiFile: 'ui.en.json',
    manifestFile: 'docs.en.json',
    contentDir: 'en/docs',
    pagesDir: 'en',
    partialsDir: 'en',
    labels: {
      docsSuffix: 'docs',
      prev: 'Previous',
      next: 'Next',
      toc: 'On this page',
      tocAria: 'On this page',
      back: 'Back to top',
      updated: 'Updated',
      readTime: (t) => `${t} read`,
      versionCaption: 'Docs version',
      versionAria: 'Switch docs version',
      allVersions: 'All versions',
      versionNote: (v) => `For ${v.library}`,
      search: 'Search docs',
      searchAria: 'Search docs',
      navAria: 'Docs navigation',
      navEmpty: 'No matching docs',
      navSoon: 'planned',
      docsCenter: 'Docs center',
      crumbHome: 'Home',
      crumbDocs: 'Docs',
      breadcrumbAria: 'Breadcrumb',
      mobileNav: (v) => `Docs navigation · ${v.label}`,
      pagerAria: 'Previous and next',
      feedback: 'Found a docs issue? Report it in <a href="{ISSUES}" target="_blank" rel="noopener">GitHub Issues</a>.',
      benchEnvHeaders: ['Item', 'Configuration'],
      benchOutcomeOk: 'Pass',
      benchOutcomeFail: 'Limited'
    }
  }
];

function loadLocale(cfg) {
  const data = readJson(path.join(SITE, 'data', cfg.dataFile));
  const ui = readJson(path.join(SITE, 'data', cfg.uiFile));
  const manifest = readJson(path.join(SITE, 'data', cfg.manifestFile));
  const versions = manifest.versions.map((v) => {
    const doc = readJson(path.join(SITE, 'data', v.data));
    let bench = null;
    if (v.benchmarks) {
      const file = path.join(SITE, 'data', v.benchmarks);
      if (!fs.existsSync(file)) throw new Error(`Missing benchmark data: ${path.relative(ROOT, file)}`);
      bench = readJson(file);
    }
    return { ...v, doc, bench };
  });
  const latest = versions.find((v) => v.id === manifest.default) || versions[0];
  return {
    ...cfg,
    prefix: cfg.id === 'zh' ? '' : `/${cfg.id}`,
    data,
    ui,
    manifest,
    versions,
    latest,
    latestPages: latest.doc.pages
  };
}

const locales = LOCALES.map(loadLocale);
const zhLocale = locales[0];
const shared = zhLocale.data;

const absUrl = (p = '/') => `${shared.homepage}${p}`;

const docsBase = (ctx, v) => `${ctx.prefix}/docs/${v.id}`;
const docsHref = (ctx, v, slug) => `${docsBase(ctx, v)}/${slug}.html`;

const siteRef = { '@type': 'WebSite', name: shared.name, url: `${shared.homepage}/` };
const orgRef = {
  '@type': 'Organization',
  name: 'NeoPen',
  url: shared.homepage,
  logo: { '@type': 'ImageObject', url: absUrl('/assets/img/favicon.svg') }
};

const jsonLd = (obj) => {
  const body = JSON.stringify(obj, null, 2)
    .replace(/</g, '\\u003c')
    .split('\n')
    .map((line) => `  ${line}`)
    .join('\n');
  return `  <script type="application/ld+json">\n${body}\n  </script>`;
};

function flattenUi(ctx) {
  const out = {};
  const token = (s) => String(s)
    .replace(/\{V\}/g, ctx.latest.label)
    .replace(/\{LIB\}/g, ctx.latest.library)
    .replace(/\{BASE\}/g, ctx.latest.baseline);
  const walk = (obj, prefix) => {
    for (const [k, v] of Object.entries(obj)) {
      const key = prefix + k.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toUpperCase();
      if (v && typeof v === 'object') walk(v, `${key}_`);
      else out[`UI_${key}`] = token(v);
    }
  };
  walk(ctx.ui, '');
  return out;
}

function baseVars(ctx) {
  const d = ctx.data;
  const latest = ctx.latest;
  return {
    NAME: d.name,
    VERSION: d.version,
    RELEASE_DATE: d.releaseDate,
    DESCRIPTION: d.description,
    HOMEPAGE: shared.homepage,
    REPO: shared.repo,
    PYPI: shared.pypi,
    ISSUES: shared.issues,
    DISCUSSIONS: shared.discussions,
    CHANGELOG_URL: shared.changelog,
    EXAMPLES: shared.examples,
    BLOG: shared.blog,
    LICENSE: shared.license,
    EMAIL: shared.email,
    PYTHON: shared.python,
    STARS: String(d.stats.stars),
    FORKS: String(d.stats.forks),
    YEAR: '2026',
    HTML_LANG: ctx.lang,
    OG_LOCALE: ctx.ogLocale,
    DOCS_COUNT: String(ctx.latestPages.length),
    DOCS_URL: docsBase(ctx, latest),
    DOCS_HUB_PATH: docsBase(ctx, latest),
    DOCS_ALL_PATH: `${ctx.prefix}/docs/`,
    QUICKSTART_URL: docsHref(ctx, latest, 'quickstart'),
    API_URL: docsHref(ctx, latest, 'api-reference'),
    DOCS_VERSION: latest.label,
    DOCS_VERSION_ID: latest.id,
    DOCS_LIBRARY: latest.library,
    DOCS_BASELINE: latest.baseline,
    DOCS_UPDATED: latest.updated,
    DOCS_BENCHMARK_URL: ctx.latestPages.some((p) => p.slug === 'benchmarks')
      ? docsHref(ctx, latest, 'benchmarks')
      : `${ctx.prefix}/docs/`,
    DOCS_HUB_CANONICAL: absUrl(`${docsBase(ctx, latest)}/`),
    DOCS_ALL_CANONICAL: absUrl(`${ctx.prefix}/docs/`),
    CHANGELOG_PAGE_URL: `${ctx.prefix}/changelog.html`,
    LANG_SWITCH_LABEL: ctx.switchLabel,
    LANG_SWITCH_HREFLANG: ctx.switchHreflang,
    LANG_SWITCH_ARIA: ctx.switchAria,
    ACTIVE_HOME: '',
    ACTIVE_FEATURES: '',
    ACTIVE_BENCHMARKS: '',
    ACTIVE_DOCS: '',
    ACTIVE_CHANGELOG: '',
    ACTIVE_COMMUNITY: '',
    ...flattenUi(ctx)
  };
}

function applyVars(html, vars) {
  return html.replace(/\{\{([A-Z0-9_]+)\}\}/g, (m, key) =>
    Object.prototype.hasOwnProperty.call(vars, key) ? vars[key] : m
  );
}

function includePartials(ctx, html) {
  return html.replace(/<!--\s*@include\s+([\w-]+)\s*-->/g, (m, name) => {
    const candidates = [
      ctx.partialsDir ? path.join(SITE, 'partials', ctx.partialsDir, `${name}.html`) : null,
      path.join(SITE, 'partials', `${name}.html`)
    ].filter(Boolean);
    const file = candidates.find((c) => fs.existsSync(c));
    if (!file) throw new Error(`Missing partial: ${name} (${ctx.id})`);
    return fs.readFileSync(file, 'utf8');
  });
}

// Rewrite unversioned docs links to the current (or latest known) doc set,
// keeping each locale inside its own prefix.
function rewriteDocsLinks(ctx, html, version) {
  const known = (v, slug) => v.doc.pages.some((p) => p.slug === slug);
  const target = (slug) => {
    if (known(version, slug)) return docsHref(ctx, version, slug);
    if (version.id !== ctx.latest.id && known(ctx.latest, slug)) return docsHref(ctx, ctx.latest, slug);
    return null;
  };
  let out = html.replace(/(?<!\/en)\/docs\/([a-z0-9-]+)\.html/g, (m, slug) => target(slug) || m);
  if (ctx.id === 'en') {
    out = out.replace(/\/en\/docs\/([a-z0-9-]+)\.html/g, (m, slug) => {
      const href = target(slug);
      return href ? href : m;
    });
  }
  return out;
}

function publicUrl(outRel) {
  let u = `/${outRel.split(path.sep).join('/')}`;
  if (u.endsWith('/index.html')) u = u.slice(0, -'index.html'.length);
  return u || '/';
}

function pairForUrl(url) {
  if (url === '/' || url === '/en/') return { zh: '/', en: '/en/' };
  if (url.startsWith('/en/')) return { zh: url.replace(/^\/en/, '') || '/', en: url };
  return { zh: url, en: `/en${url}` };
}

function switchUrl(outRel, ctx) {
  const pair = pairForUrl(publicUrl(outRel));
  return ctx.id === 'zh' ? pair.en : pair.zh;
}

function alternateTags(outRel) {
  const pair = pairForUrl(publicUrl(outRel));
  return [
    `<link rel="alternate" hreflang="zh-CN" href="${esc(absUrl(pair.zh))}">`,
    `<link rel="alternate" hreflang="en" href="${esc(absUrl(pair.en))}">`,
    `<link rel="alternate" hreflang="x-default" href="${esc(absUrl(pair.zh))}">`
  ].join('\n  ');
}

function injectAlternates(html, outRel) {
  return html.replace('</head>', `  ${alternateTags(outRel)}\n</head>`);
}

const builtUrls = [];
function writePage(outRel, html) {
  const outFile = path.join(ROOT, outRel);
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, html, 'utf8');
  builtUrls.push(publicUrl(outRel));
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
  bookmark: '<path d="M6 3h12a1 1 0 0 1 1 1v17l-7-4-7 4V4a1 1 0 0 1 1-1z"/>',
  link: '<path d="M10 13a5 5 0 0 0 7.07 0l2.83-2.83a5 5 0 0 0-7.07-7.07l-1.41 1.41"/><path d="M14 11a5 5 0 0 0-7.07 0L4.1 13.83a5 5 0 0 0 7.07 7.07l1.41-1.41"/>',
  coins: '<circle cx="8" cy="8" r="6"/><path d="M18.09 10.37A6 6 0 1 1 10.34 18"/><path d="M7 6h1v4"/><path d="m16.71 13.88.7.71-2.82 2.82"/>',
  workflow: '<rect x="3" y="3" width="7" height="6" rx="1.5"/><rect x="14" y="15" width="7" height="6" rx="1.5"/><path d="M6.5 9v5a4 4 0 0 0 4 4H14"/>',
  box: '<path d="M21 8 12 3 3 8v8l9 5 9-5V8z"/><path d="m3 8 9 5 9-5"/><path d="M12 13v8"/>',
  refresh: '<path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/><path d="M21 12a9 9 0 0 1-15 6.7L3 16"/><path d="M3 21v-5h5"/>',
  terminal: '<path d="m4 17 6-6-6-6"/><path d="M12 19h8"/>',
  'arrow-up': '<path d="M12 19V5"/><path d="m5 12 7-7 7 7"/>'
};

const icon = (name, cls = 'icon') =>
  `<svg class="${cls}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${iconPaths[name] || iconPaths.layers}</svg>`;

const categoryLabel = (v, id) => v.doc.categories.find((c) => c.id === id)?.label || id;

function versionOptions(ctx, current) {
  const suffix = ctx.ui.docs.versionOptionLatest || '';
  return ctx.versions.map((v) => `<option value="${v.id}"${v.id === current.id ? ' selected' : ''}>${esc(v.label)}${v.status === 'latest' ? esc(suffix) : ''}</option>`).join('');
}

function renderVersionPicker(ctx, v) {
  const t = ctx.labels;
  return `<div class="docs-version-picker">
        <span class="docs-version-caption">${esc(t.versionCaption)}</span>
        <div class="docs-version-row">
          <select class="docs-version-select" data-docs-version aria-label="${esc(t.versionAria)}">
            ${versionOptions(ctx, v)}
          </select>
          <a class="docs-version-all" href="${ctx.prefix}/docs/">${esc(t.allVersions)}</a>
        </div>
        <p class="docs-version-note">${esc(t.versionNote(v))}</p>
      </div>`;
}

function renderSidebar(ctx, v, activeSlug, includeSearch = true) {
  const t = ctx.labels;
  const groups = v.doc.categories.map((cat) => {
    const links = v.doc.pages
      .filter((p) => p.category === cat.id)
      .map((p) => `<a class="docs-nav-link${p.slug === activeSlug ? ' is-active' : ''}" href="${docsHref(ctx, v, p.slug)}" data-search="${esc(`${p.title} ${p.description}`)}">${esc(p.title)}${p.category === 'roadmap' ? `<span class="nav-soon">${esc(t.navSoon)}</span>` : ''}</a>`)
      .join('\n        ');
    return `<div class="docs-nav-group">
        <p class="docs-nav-label">${esc(cat.label)}</p>
        ${links}
      </div>`;
  });
  const search = includeSearch ? `<div class="docs-search-wrap">
        <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>
        <input id="docsSearch" type="search" placeholder="${esc(t.search)}" autocomplete="off" aria-label="${esc(t.searchAria)}">
      </div>` : '';
  const navId = includeSearch ? 'docsNav' : 'docsNavMobile';
  return `${renderVersionPicker(ctx, v)}
      ${search}
      <nav class="docs-nav" id="${navId}" aria-label="${esc(t.navAria)}">
        ${groups.join('\n        ')}
      </nav>
      ${includeSearch ? `<p class="docs-nav-empty" id="docsNavEmpty" hidden>${esc(t.navEmpty)}</p>` : ''}`;
}

function renderToc(ctx, content) {
  const headings = [];
  const seen = new Map();
  const withIds = content.replace(/<h2(\s[^>]*)?>([\s\S]*?)<\/h2>/g, (m, attrs = '', inner) => {
    const text = inner.replace(/<[^>]+>/g, '').trim();
    if (!text) return m;
    const explicit = attrs.match(/\sid="([^"]+)"/);
    if (explicit) {
      headings.push({ id: explicit[1], text });
      return m;
    }
    const count = seen.get(text) || 0;
    seen.set(text, count + 1);
    const id = count ? `${text}-${count}` : text;
    headings.push({ id, text });
    return `<h2${attrs} id="${esc(id)}">${inner}</h2>`;
  });
  const items = headings
    .slice(0, 14)
    .map((h) => `<a href="#${encodeURIComponent(h.id)}">${esc(h.text)}</a>`)
    .join('');
  return {
    content: withIds,
    toc: items ? `<nav class="docs-toc" aria-label="${esc(ctx.labels.tocAria)}"><p class="docs-toc-label">${esc(ctx.labels.toc)}</p>${items}</nav>` : ''
  };
}

// --- Benchmark rendering ---------------------------------------------------
const benchPct = (value, max) => `${Math.max(2, Math.round((value / max) * 100))}%`;

function benchTable(headers, rows, cls = '') {
  return `<table${cls ? ` class="${cls}"` : ''}>
<thead><tr>${headers.map((h) => `<th>${esc(h)}</th>`).join('')}</tr></thead>
<tbody>
${rows.map((r) => `<tr>${r.map((c) => `<td>${esc(c)}</td>`).join('')}</tr>`).join('\n')}
</tbody>
</table>`;
}

function benchKpis(b) {
  return `<div class="bench-kpis">
${b.metrics.map((m) => `  <div class="bench-kpi${m.highlight ? ' is-highlight' : ''}">
    <span class="bench-kpi-label">${esc(m.label)}</span>
    <span class="bench-kpi-value">${esc(m.value)}<span>${esc(m.unit)}</span></span>
    <span class="bench-kpi-detail">${esc(m.detail)}</span>
  </div>`).join('\n')}
</div>`;
}

function benchHomeCards(b) {
  return b.metrics.map((m, i) => `          <div class="bench-card${i === 0 ? ' bench-main' : ''}">
            <span class="bench-label">${esc(m.label)}</span>
            <strong class="bench-num">${esc(m.value)}<span> ${esc(m.unit)}</span></strong>
            <p>${esc(m.homeDetail || m.detail)}</p>
          </div>`).join('\n');
}

function benchBars(caption, bars, max, note) {
  const top = max || Math.max(...bars.map((b) => b.value));
  return `<figure class="bench-chart">
  <figcaption>${esc(caption)}</figcaption>
  <div class="bench-bars">
${bars.map((b) => `    <div class="bench-bar-row">
      <span class="bench-bar-label">${esc(b.label)}</span>
      <span class="bench-bar-track"><span class="bench-bar-fill" style="width: ${benchPct(b.value, top)}"></span></span>
      <span class="bench-bar-value">${esc(b.display)}</span>
    </div>`).join('\n')}
  </div>
  ${note ? `<p class="bench-chart-note">${esc(note)}</p>` : ''}
</figure>`;
}

function benchBoundaries(ctx, rows) {
  const t = ctx.labels;
  return `<div class="bench-boundaries">
${rows.map((r) => `  <div class="bench-boundary ${r.ok ? 'is-ok' : 'is-fail'}">
    <div class="bench-boundary-head">
      <span class="bench-boundary-badge">${r.ok ? t.benchOutcomeOk : t.benchOutcomeFail}</span>
      <strong>${esc(r.mode)}</strong>
      <span class="bench-boundary-workers">${r.workers} worker${r.workers > 1 ? 's' : ''}</span>
    </div>
    <p class="bench-boundary-outcome">${esc(r.outcome)}</p>
${r.meta.length ? `    <dl class="bench-boundary-meta">${r.meta.map((m) => `<div><dt>${esc(m.label)}</dt><dd>${esc(m.value)}</dd></div>`).join('')}</dl>` : ''}
  </div>`).join('\n')}
</div>`;
}

function renderBenchmarkContent(ctx, raw, b) {
  const vars = {
    BENCH_RELEASE: esc(b.version),
    BENCH_METRICS: benchKpis(b),
    BENCH_ENV_TABLE: benchTable(ctx.labels.benchEnvHeaders, b.environment.map((r) => [r.label, r.value]), 'bench-env'),
    BENCH_SCHEDULING_TABLE: benchTable(b.scheduling.headers, b.scheduling.rows),
    BENCH_IO_CHART: benchBars(b.io.caption, b.io.bars, b.io.max, b.io.note),
    BENCH_IO_TABLE: benchTable(b.io.headers, b.io.rows),
    BENCH_SQLITE_BOUNDARIES: benchBoundaries(ctx, b.sqlite.rows),
    BENCH_PREFETCH_CHART: benchBars(b.prefetch.caption, b.prefetch.bars, 0, b.prefetch.note),
    BENCH_STRESS_TABLE: benchTable(b.stress.headers, b.stress.rows),
    BENCH_STRESS_NOTE: esc(b.stress.note),
    BENCH_EVENT_TABLE: benchTable(b.eventbus.headers, b.eventbus.rows)
  };
  return raw.replace(/\{\{([A-Z0-9_]+)\}\}/g, (m, key) =>
    Object.prototype.hasOwnProperty.call(vars, key) ? vars[key] : m
  );
}

function docsPageHead(ctx, v, p) {
  const t = ctx.labels;
  const title = `${p.title} · NeoTask ${v.label} ${t.docsSuffix}`;
  const url = absUrl(docsHref(ctx, v, p.slug));
  const image = absUrl('/assets/img/og.png');
  const section = categoryLabel(v, p.category);
  const keywords = [
    p.title,
    ctx.id === 'zh' ? `${p.title} 教程` : `${p.title} guide`,
    ctx.id === 'zh' ? `${p.title} 文档` : `${p.title} docs`,
    section,
    `NeoTask ${v.label} ${t.docsSuffix}`,
    v.library,
    ctx.id === 'zh' ? 'Python 任务队列' : 'Python task queue',
    ...(p.slug === 'roadmap'
      ? (ctx.id === 'zh'
        ? ['NeoFlow', 'DAG 工作流编排', '上层编排服务']
        : ['NeoFlow', 'DAG workflow orchestration', 'orchestration layer'])
      : [])
  ].filter((k, i, arr) => arr.indexOf(k) === i).join(', ');
  const ld = {
    '@context': 'https://schema.org',
    '@graph': [
      {
        '@type': 'TechArticle',
        '@id': `${url}#article`,
        headline: p.title,
        name: title,
        description: p.description,
        image,
        url,
        mainEntityOfPage: url,
        inLanguage: ctx.lang,
        datePublished: v.released,
        dateModified: p.updated,
        articleSection: section,
        keywords,
        author: orgRef,
        publisher: orgRef,
        isPartOf: siteRef,
        about: {
          '@type': 'SoftwareApplication',
          name: shared.name,
          applicationCategory: 'DeveloperApplication',
          operatingSystem: 'OS Independent',
          url: `${shared.homepage}/`
        },
        ...(p.slug === 'roadmap' ? {
          mentions: {
            '@type': 'SoftwareApplication',
            name: 'NeoFlow',
            applicationCategory: 'DeveloperApplication',
            description: ctx.id === 'zh'
              ? 'DAG 工作流编排引擎，负责任务拓扑、条件分支与状态推进；NeoTask 作为其执行层。'
              : 'DAG workflow orchestration engine that owns topology, conditional branches and state progression; NeoTask is its execution layer.'
          }
        } : {})
      },
      {
        '@type': 'BreadcrumbList',
        '@id': `${url}#breadcrumb`,
        itemListElement: [
          { '@type': 'ListItem', position: 1, name: t.crumbHome, item: `${shared.homepage}${ctx.prefix}/` },
          { '@type': 'ListItem', position: 2, name: t.crumbDocs, item: absUrl(`${ctx.prefix}/docs/`) },
          { '@type': 'ListItem', position: 3, name: v.label, item: absUrl(`${docsBase(ctx, v)}/`) },
          { '@type': 'ListItem', position: 4, name: p.title, item: url }
        ]
      }
    ]
  };
  const siteName = ctx.ui.docs.siteName;
  const alt = `${p.title} · NeoTask ${t.docsSuffix}`;
  return `<!DOCTYPE html>
<html lang="${ctx.lang}">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${esc(title)}</title>
  <meta name="description" content="${esc(p.description)}">
  <meta name="keywords" content="${esc(keywords)}">
  <meta name="author" content="NeoPen">
  <meta name="robots" content="index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1">
  <link rel="canonical" href="${url}">
  <meta property="og:site_name" content="${esc(siteName)}">
  <meta property="og:locale" content="${ctx.ogLocale}">
  <meta property="og:type" content="article">
  <meta property="og:title" content="${esc(title)}">
  <meta property="og:description" content="${esc(p.description)}">
  <meta property="og:url" content="${url}">
  <meta property="og:image" content="${image}">
  <meta property="og:image:type" content="image/png">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:image:alt" content="${esc(alt)}">
  <meta property="article:published_time" content="${v.released}">
  <meta property="article:modified_time" content="${p.updated}">
  <meta property="article:section" content="${esc(section)}">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${esc(title)}">
  <meta name="twitter:description" content="${esc(p.description)}">
  <meta name="twitter:image" content="${image}">
  <meta name="twitter:image:alt" content="${esc(alt)}">
  <meta name="theme-color" content="#0b1713">
  <link rel="icon" href="/assets/img/favicon.svg" type="image/svg+xml">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap">
  <link rel="stylesheet" href="/assets/css/site.css">
  <link rel="stylesheet" href="/assets/css/docs.css">
${jsonLd(ld)}
</head>`;
}

function buildDocsPage(ctx, v, p) {
  const contentFile = path.join(SITE, 'content', ctx.contentDir, v.id, `${p.slug}.html`);
  if (!fs.existsSync(contentFile)) {
    throw new Error(`Missing docs content: ${path.relative(ROOT, contentFile)}`);
  }
  const outRel = path.join(ctx.outDir, 'docs', v.id, `${p.slug}.html`);
  const t = ctx.labels;

  let raw = fs.readFileSync(contentFile, 'utf8');
  if (p.slug === 'benchmarks') {
    if (!v.bench) throw new Error(`Missing benchmark data for docs version ${v.id} (${ctx.id})`);
    raw = renderBenchmarkContent(ctx, raw, v.bench);
  }
  const { content, toc } = renderToc(ctx, raw);
  const idx = v.doc.pages.findIndex((x) => x.slug === p.slug);
  const prev = v.doc.pages[idx - 1];
  const next = v.doc.pages[idx + 1];
  const pager = `<nav class="docs-pager" aria-label="${esc(t.pagerAria)}">
      ${prev ? `<a class="pager-prev" href="${docsHref(ctx, v, prev.slug)}"><span>${esc(t.prev)}</span><strong>${esc(prev.title)}</strong></a>` : '<span></span>'}
      ${next ? `<a class="pager-next" href="${docsHref(ctx, v, next.slug)}"><span>${esc(t.next)}</span><strong>${esc(next.title)}</strong></a>` : '<span></span>'}
    </nav>`;
  const mermaid = /class="mermaid"/.test(content)
    ? `\n  <script type="module">import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs'; mermaid.initialize({ startOnLoad: true, theme: 'neutral' });</script>`
    : '';

  const page = `${docsPageHead(ctx, v, p)}
<body class="docs-page" id="top">
  <!-- @include header -->
  <div class="docs-shell">
    <aside class="docs-sidebar" id="docsSidebar">
      <a class="docs-sidebar-home" href="${ctx.prefix}/docs/"><span class="docs-sidebar-badge">DOCS</span> ${esc(t.docsCenter)}</a>
      ${renderSidebar(ctx, v, p.slug)}
    </aside>
    <main class="docs-main" id="main">
      <nav class="breadcrumb" aria-label="${esc(t.breadcrumbAria)}">
        <a href="${ctx.prefix}/">${esc(t.crumbHome)}</a><span>/</span><a href="${ctx.prefix}/docs/">${esc(t.crumbDocs)}</a><span>/</span><a href="${docsBase(ctx, v)}/">${esc(v.label)}</a><span>/</span><span>${esc(categoryLabel(v, p.category))}</span>
      </nav>
      <details class="docs-mobile-nav">
        <summary>${esc(t.mobileNav(v))}</summary>
        ${renderSidebar(ctx, v, p.slug, false)}
      </details>
      <article class="article">
        <header class="article-header">
          <p class="article-eyebrow">${esc(categoryLabel(v, p.category))}</p>
          <h1>${esc(p.title)}</h1>
          <p class="article-lede">${esc(p.description)}</p>
          <div class="article-meta">
            <span class="article-version">${esc(v.label)} · ${esc(v.library)}</span>
            <span class="dot"></span>
            <span>${esc(t.updated)} ${esc(p.updated)}</span>
            <span class="dot"></span>
            <span>${esc(t.readTime(p.time))}</span>
            <span class="dot"></span>
            <a href="${shared.repo}" target="_blank" rel="noopener">GitHub ${icon('code', 'icon-xs')}</a>
          </div>
        </header>
        <div class="article-body">
${content}
        </div>
        <footer class="article-foot">
          <a class="article-top" href="#top">${esc(t.back)} ${icon('arrow-up', 'icon-xs')}</a>
        </footer>
      </article>
      ${pager}
      <p class="docs-feedback">${t.feedback.replace('{ISSUES}', shared.issues)}</p>
    </main>
    ${toc}
  </div>
  <!-- @include footer -->
  <script src="/assets/js/site.js" defer></script>${mermaid}
</body>
</html>
`;

  let rendered = includePartials(ctx, page);
  rendered = applyVars(rendered, {
    ...baseVars(ctx),
    LANG_SWITCH_URL: switchUrl(outRel, ctx),
    ACTIVE_DOCS: p.slug === 'benchmarks' ? '' : 'is-active',
    ACTIVE_BENCHMARKS: p.slug === 'benchmarks' ? 'is-active' : '',
    DOCS_VERSION: v.label,
    DOCS_LIBRARY: v.library,
    DOCS_BASELINE: v.baseline
  });
  rendered = rewriteDocsLinks(ctx, rendered, v);
  rendered = injectAlternates(rendered, outRel);
  writePage(outRel, rendered);
}

function docsHubLd(ctx, v) {
  const t = ctx.labels;
  const url = absUrl(`${docsBase(ctx, v)}/`);
  return {
    '@context': 'https://schema.org',
    '@graph': [
      {
        '@type': 'CollectionPage',
        '@id': `${url}#collection`,
        name: `NeoTask ${v.label} ${t.docsSuffix}`,
        description: v.description,
        url,
        inLanguage: ctx.lang,
        isPartOf: siteRef,
        about: {
          '@type': 'SoftwareApplication',
          name: shared.name,
          applicationCategory: 'DeveloperApplication',
          operatingSystem: 'OS Independent',
          url: `${shared.homepage}/`
        },
        hasPart: v.doc.pages.map((p) => ({
          '@type': 'TechArticle',
          headline: p.title,
          description: p.description,
          url: absUrl(docsHref(ctx, v, p.slug))
        }))
      },
      {
        '@type': 'BreadcrumbList',
        '@id': `${url}#breadcrumb`,
        itemListElement: [
          { '@type': 'ListItem', position: 1, name: t.crumbHome, item: `${shared.homepage}${ctx.prefix}/` },
          { '@type': 'ListItem', position: 2, name: t.crumbDocs, item: absUrl(`${ctx.prefix}/docs/`) },
          { '@type': 'ListItem', position: 3, name: v.label, item: url }
        ]
      }
    ]
  };
}

function docsVersionsLd(ctx) {
  const t = ctx.labels;
  const url = absUrl(`${ctx.prefix}/docs/`);
  return {
    '@context': 'https://schema.org',
    '@graph': [
      {
        '@type': 'CollectionPage',
        '@id': `${url}#collection`,
        name: ctx.ui.docs.versionsTitle,
        description: ctx.ui.docs.versionsLede,
        url,
        inLanguage: ctx.lang,
        isPartOf: siteRef,
        hasPart: ctx.versions.map((v) => ({
          '@type': 'WebPage',
          name: `NeoTask ${v.label} ${t.docsSuffix}`,
          description: v.description,
          url: absUrl(`${docsBase(ctx, v)}/`)
        }))
      },
      {
        '@type': 'BreadcrumbList',
        '@id': `${url}#breadcrumb`,
        itemListElement: [
          { '@type': 'ListItem', position: 1, name: t.crumbHome, item: `${shared.homepage}${ctx.prefix}/` },
          { '@type': 'ListItem', position: 2, name: t.crumbDocs, item: url }
        ]
      }
    ]
  };
}

function renderDocsHub(ctx, v) {
  const t = ctx.labels;
  const featured = v.doc.pages.filter((p) => p.featured).map((p) => `
        <a class="doc-feature" href="${docsHref(ctx, v, p.slug)}" data-doc-card data-doc-search="${esc(`${p.title} ${p.description} ${categoryLabel(v, p.category)}`)}">
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
          <a class="doc-card" href="${docsHref(ctx, v, p.slug)}" data-doc-card data-doc-search="${esc(`${p.title} ${p.description} ${cat.label}`)}">
            <span class="doc-card-icon">${icon(p.icon)}</span>
            <span class="doc-card-title">${esc(p.title)}${p.category === 'roadmap' ? `<em class="tag-soon">${esc(t.navSoon)}</em>` : ''}</span>
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

function buildPage(ctx, name, outRel, version, extraVars = {}) {
  const src = path.join(SITE, 'pages', ctx.pagesDir, name);
  let html = fs.readFileSync(src, 'utf8');
  html = includePartials(ctx, html);
  html = applyVars(html, {
    ...baseVars(ctx),
    LANG_SWITCH_URL: switchUrl(outRel, ctx),
    ...extraVars
  });
  html = rewriteDocsLinks(ctx, html, version);
  html = injectAlternates(html, outRel);
  writePage(outRel, html);
}

function versionCards(ctx) {
  const t = ctx.labels;
  const d = ctx.ui.docs;
  return ctx.versions.map((v) => `
      <article class="version-card${v.status === 'latest' ? ' is-latest' : ''}">
        <div class="version-card-head">
          <div>
            <p class="version-card-label">${esc(v.label)}</p>
            <h2>${esc(v.library)}</h2>
          </div>
          ${v.status === 'latest' ? `<span class="version-badge">${esc(d.latest)}</span>` : `<span class="version-badge is-muted">${esc(d.maintained)}</span>`}
        </div>
        <p class="version-card-desc">${esc(v.description)}</p>
        <dl class="version-card-meta">
          <div><dt>${esc(d.versionBaseline)}</dt><dd>v${esc(v.baseline)}</dd></div>
          <div><dt>${esc(d.versionLibrary)}</dt><dd>${esc(v.library)}</dd></div>
          <div><dt>${esc(d.versionUpdated)}</dt><dd>${esc(v.updated)}</dd></div>
          <div><dt>${esc(d.versionPages)}</dt><dd>${v.doc.pages.length} ${ctx.id === 'zh' ? '篇' : 'pages'}</dd></div>
        </dl>
        <a class="btn btn-primary btn-block" href="${docsBase(ctx, v)}/">${esc(d.enterVersion)} ${esc(v.label)} <span aria-hidden="true">→</span></a>
      </article>`).join('');
}

function latestScope(ctx, v) {
  return v.doc.categories.map((cat) => {
    const count = v.doc.pages.filter((p) => p.category === cat.id).length;
    return `
          <div>
            <h3><a href="${docsBase(ctx, v)}/#${cat.id}">${esc(cat.label)} · ${count}</a></h3>
            <p>${esc(cat.description || '')}</p>
          </div>`;
  }).join('');
}

function releaseList(ctx) {
  const typeLabel = ctx.id === 'zh'
    ? { major: '主要版本', minor: '功能版本', patch: '修复版本', alpha: '内测版本' }
    : { major: 'Major', minor: 'Feature', patch: 'Patch', alpha: 'Alpha' };
  return ctx.data.releases.map((r, i) => `
      <article class="release${i === 0 ? ' is-latest' : ''}" id="v${r.version}">
        <div class="release-side">
          <div class="release-version">v${r.version}${i === 0 ? `<span class="release-latest">${ctx.id === 'zh' ? '最新' : 'Latest'}</span>` : ''}</div>
          <time datetime="${r.date}">${r.date}</time>
          <span class="release-type ${r.type}">${typeLabel[r.type]}</span>
        </div>
        <div class="release-body">
          <h2>${esc(r.summary)}</h2>
          <ul>
            ${r.items.map((item) => `<li>${esc(item)}</li>`).join('\n            ')}
          </ul>
          <a class="text-link" href="${r.url}" target="_blank" rel="noopener">${ctx.id === 'zh' ? '查看完整发布说明' : 'Read the full release notes'} <span aria-hidden="true">→</span></a>
        </div>
      </article>`).join('');
}

// --- Clean generated output -------------------------------------------------
fs.rmSync(path.join(ROOT, 'docs'), { recursive: true, force: true });
fs.rmSync(path.join(ROOT, 'en'), { recursive: true, force: true });

// --- Generate docs articles, hubs, version centers and site pages -----------
for (const ctx of locales) {
  for (const v of ctx.versions) {
    for (const p of v.doc.pages) buildDocsPage(ctx, v, p);
  }

  const latest = ctx.latest;
  const hub = renderDocsHub(ctx, latest);
  buildPage(ctx, 'docs.html', path.join(ctx.outDir, 'docs', latest.id, 'index.html'), latest, {
    ACTIVE_DOCS: 'is-active',
    DOCS_VERSION: latest.label,
    DOCS_LIBRARY: latest.library,
    DOCS_BASELINE: latest.baseline,
    DOCS_UPDATED: latest.updated,
    DOCS_VERSION_ID: latest.id,
    DOCS_VERSION_OPTIONS: versionOptions(ctx, latest),
    DOCS_FEATURED: hub.featured,
    DOCS_GROUPS: hub.groups,
    DOCS_JSONLD: jsonLd(docsHubLd(ctx, latest))
  });

  buildPage(ctx, 'docs-versions.html', path.join(ctx.outDir, 'docs', 'index.html'), latest, {
    ACTIVE_DOCS: 'is-active',
    VERSION_CARDS: versionCards(ctx),
    LATEST_SCOPE: latestScope(ctx, latest),
    VERSION_COUNT: String(ctx.versions.length),
    VERSION_COUNT_TEXT: ctx.id === 'zh' ? `${ctx.versions.length} 个文档版本` : `${ctx.versions.length} version${ctx.versions.length > 1 ? 's' : ''}`,
    LATEST_VERSION: latest.label,
    LATEST_VERSION_ID: latest.id,
    LATEST_LIBRARY: latest.library,
    LATEST_BASELINE: latest.baseline,
    LATEST_UPDATED: latest.updated,
    LATEST_DOCS_URL: `${docsBase(ctx, latest)}/`,
    LATEST_QUICKSTART_URL: docsHref(ctx, latest, 'quickstart'),
    DOCS_INDEX_JSONLD: jsonLd(docsVersionsLd(ctx))
  });

  buildPage(ctx, 'changelog.html', path.join(ctx.outDir, 'changelog.html'), latest, {
    ACTIVE_CHANGELOG: 'is-active',
    RELEASES: releaseList(ctx)
  });
  buildPage(ctx, 'index.html', path.join(ctx.outDir, 'index.html'), latest, {
    ACTIVE_HOME: 'is-active',
    BENCH_HOME_CARDS: latest.bench ? benchHomeCards(latest.bench) : ''
  });
  buildPage(ctx, 'features.html', path.join(ctx.outDir, 'features.html'), latest, { ACTIVE_FEATURES: 'is-active' });
  buildPage(ctx, 'community.html', path.join(ctx.outDir, 'community.html'), latest, { ACTIVE_COMMUNITY: 'is-active' });
  buildPage(ctx, '404.html', path.join(ctx.outDir, '404.html'), latest, {});
}

// --- Sitemap ---------------------------------------------------------------
const urls = [...new Set(builtUrls)].sort();
const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">
${urls.map((u) => {
  const pair = pairForUrl(u);
  return `  <url>
    <loc>${shared.homepage}${u}</loc>
    <xhtml:link rel="alternate" hreflang="zh-CN" href="${shared.homepage}${pair.zh}"/>
    <xhtml:link rel="alternate" hreflang="en" href="${shared.homepage}${pair.en}"/>
    <xhtml:link rel="alternate" hreflang="x-default" href="${shared.homepage}${pair.zh}"/>
  </url>`;
}).join('\n')}
</urlset>
`;
fs.writeFileSync(path.join(ROOT, 'sitemap.xml'), sitemap, 'utf8');
fs.writeFileSync(path.join(ROOT, 'robots.txt'), `User-agent: *\nAllow: /\n\nSitemap: ${shared.homepage}/sitemap.xml\n`, 'utf8');

console.log(`Built ${locales.length} locales x ${zhLocale.versions.length} docs version(s), ${zhLocale.latestPages.length} pages each, 6 site pages per locale.`);
