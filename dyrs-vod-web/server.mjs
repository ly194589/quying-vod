// 趣影 VOD 聚合服务：适配 4K 站（HTML）与暴风站（MacCMS JSON API），
// 输出统一的 TVBox / MacCMS 兼容接口。列表与详情来自上游公开页面，
// 播放地址实时解析且不落库；海报按需走同源代理。
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { AsyncLocalStorage } from 'node:async_hooks';
import { fileURLToPath } from 'node:url';
import { DEFAULT_LIVE_SOURCES, LIVE_EPG_URL, LIVE_GROUP_NAMES, LiveCollector, buildLiveM3U, queryLiveChannels, readLocalLiveSources } from './live.mjs';
import { isWorkers, ENV, httpGetText as runtimeGetText, readAssetText } from './runtime.mjs';

// Workers 打包后 import.meta.url 可能为空，这里给出可用的兜底目录。
const __dirname = path.dirname(fileURLToPath(import.meta.url || 'file:///bundle/server.mjs'));
const PUBLIC_DIR = path.join(__dirname, 'public');

// 存储实例：
// - Node：进程启动时初始化一次，长驻复用连接池；
// - Workers：每个请求建立一条到 Hyperdrive / Neon 的连接，绑定在 AsyncLocalStorage 上，
//   避免 Cloudflare「I/O 对象不能跨请求复用」的限制。
let rootCatalogStore = null;
let rootAdminStore = null;
let storeBackend = 'none';
let storeInitError = '';
const requestStores = new AsyncLocalStorage();

const STORES = {
  get catalogStore() { return requestStores.getStore()?.catalogStore ?? rootCatalogStore; },
  get adminStore() { return requestStores.getStore()?.adminStore ?? rootAdminStore; },
};

async function initNodeStores() {
  try {
    const { initStores } = await import('./db-layer.mjs');
    const stores = await initStores({ initialPassword: ENV.ADMIN_PASSWORD, moduleDir: __dirname, env: ENV });
    rootCatalogStore = stores.catalogStore;
    rootAdminStore = stores.adminStore;
    storeBackend = stores.backend || 'unknown';
    storeInitError = '';
    if (stores.backend === 'postgres') {
      console.log('[quying] using external PostgreSQL for persistence');
    } else {
      console.warn('[quying] using local SQLite (non-persistent across restarts)');
      if (!ENV.ADMIN_PASSWORD) {
        console.warn('[quying] admin 默认密码 admin123，请登录 /admin 后立即修改');
      }
    }
  } catch (error) {
    storeInitError = String((error && error.message) || error);
    console.warn(`[quying] data store disabled: ${storeInitError}`);
  }
}

if (!isWorkers) await initNodeStores();

const PORT = Number(ENV.PORT) || 8000;
const HOST = '0.0.0.0';
// 暴风资源 mirrors are tried in order when a request fails or returns an
// abnormal response. A comma-separated VOD_UPSTREAM_HOST can override them.
const UPSTREAM_HOSTS = (ENV.VOD_UPSTREAM_HOST || 'https://bfzy1.tv,https://bfzy2.tv,https://bfzy3.tv,https://bfzy4.tv,https://bfzy5.tv')
  .split(',')
  .map((host) => host.trim().replace(/\/+$/, ''))
  .filter(Boolean);
// 主采集源：魔都资源（MacCMS JSON）。暴风站 bfzy1~5 自 2026-11 起全量播放地址失效
// （vod_play_url 里的 CDN 全部 404），只保留作元数据兜底。魔都实测 m3u8 可播。
// 逗号分隔 MODU_UPSTREAM_HOST 可覆盖；mo- 前缀用于区分暴风数字 id 空间。
const MODU_HOSTS = (ENV.MODU_UPSTREAM_HOST || 'https://caiji.moduapi.cc')
  .split(',')
  .map((host) => host.trim().replace(/\/+$/, ''))
  .filter(Boolean);
const MODU_ID_PREFIX = 'mo-';
const MODU_ID_RE = /^mo-\d{1,10}$/;
const ID_TOKEN = '(?:\\d{1,10}|4kcz-\\d{1,8}|mo-\\d{1,10})';
const ID_LIST_RE = new RegExp(`^(?:(?:${ID_TOKEN})|manual-[a-z0-9]{4,24})(?:,(?:(?:${ID_TOKEN})|manual-[a-z0-9]{4,24})){0,19}$`);
const PLAY_ID_RE = new RegExp(`^(?:${ID_TOKEN})$`);
const FOURKCZ_HOSTS = (ENV.FOURKCZ_UPSTREAM_HOST || 'https://www.4kcz.com,https://4kcz.com,https://czzy.top,https://cz4k.com')
  .split(',')
  .map((host) => host.trim().replace(/\/+$/, ''))
  .filter(Boolean);
const FOURKCZ_CATEGORY_PATHS = {
  '1': ['/movie_bt_series/dyy'],
  '2': ['/movie_bt_series/guochanju', '/movie_bt_series/mj', '/movie_bt_series/hj', '/movie_bt_series/rj'],
  '4': ['/movie_bt_view_cat/fjj'],
};
const PROVIDER_TYPES = {
  '1': { head: 20, children: [21, 22, 23, 24, 25, 26, 27, 28, 29, 50, 67] },
  '2': { head: 30, children: [31, 32, 33, 34, 35, 36, 37, 38] },
  '3': { head: 45, children: [46, 47, 48, 49] },
  '4': { head: 39, children: [40, 41, 42, 43, 44] },
  '5': { head: 58, children: [59, 60, 61, 62, 63, 64, 65, 66, 68] },
  '6': { head: 52, children: [52] },
  '7': { head: 53, children: [54, 55, 56, 57] },
};
// A head type maps to its headline id for 4K fallback and client display.
const HEAD_TO_HEADLINE = { 20: '1', 30: '2', 45: '3', 39: '4', 58: '5', 52: '6', 53: '7' };
// Child types map back to headline as well.
const CHILD_TO_HEADLINE = {};
for (const [hid, info] of Object.entries(PROVIDER_TYPES)) {
  for (const c of info.children) CHILD_TO_HEADLINE[c] = hid;
}
// MacCMS 原始 type_id → 显示中文名称（前端下拉/子分类展示用）
const TYPE_NAMES = {
  20: '电影片', 21: '动作片', 22: '喜剧片', 23: '恐怖片', 24: '科幻片', 25: '爱情片', 26: '剧情片', 27: '战争片', 28: '纪录片', 29: '理论片', 50: '动画片', 67: '福利',
  30: '连续剧', 31: '国产剧', 32: '欧美剧', 33: '香港剧', 34: '韩国剧', 35: '台湾剧', 36: '日本剧', 37: '海外剧', 38: '泰国剧',
  45: '综艺片', 46: '大陆综艺', 47: '港台综艺', 48: '日韩综艺', 49: '欧美综艺',
  39: '动漫片', 40: '国产动漫', 41: '日韩动漫', 42: '欧美动漫', 43: '港台动漫', 44: '海外动漫',
  58: '短剧大全', 59: '重生民国', 60: '穿越年代', 61: '现代言情', 62: '反转爽文', 63: '女恋总裁', 64: '闪婚离婚', 65: '都市脑洞', 66: '古装仙侠', 68: 'AI漫剧',
  52: '预告片', 53: '体育赛事', 54: '足球', 55: '篮球', 56: '网球', 57: '斯诺克',
};
// 4K 列表卡片的分类标签（.furk）与清晰度标签（.qb）。默认「全部」列表按
// 4K 站实际展示的分类标注，而不是把所有条目都归为电影。
const FOURK_CARD_TYPES = {
  '电影': 1, '1080P': 1, '720P': 1, '4K': 1, '高清': 1, '蓝光': 1,
  '动作片': 21, '喜剧片': 22, '恐怖片': 23, '科幻片': 24, '爱情片': 25, '剧情片': 26, '战争片': 27, '纪录片': 28, '动画片': 50,
  '国产剧': 31, '欧美剧': 32, '香港剧': 33, '港剧': 33, '韩国剧': 34, '韩剧': 34, '台湾剧': 35, '台剧': 35, '日本剧': 36, '日剧': 36, '海外剧': 37, '泰国剧': 38, '泰剧': 38,
  '综艺': 3, '综艺片': 45, '大陆综艺': 46, '港台综艺': 47, '日韩综艺': 48, '欧美综艺': 49,
  '动漫': 4, '动漫片': 39, '番剧': 41, '国产动漫': 40, '日韩动漫': 41, '欧美动漫': 42, '港台动漫': 43, '海外动漫': 44,
  '短剧': 5, '短剧大全': 58,
};
const FOURK_QUALITY_LABELS = new Set(['1080P', '720P', '4K', '高清', '蓝光']);
const PUBLIC_URL = (ENV.PUBLIC_URL || '').replace(/\/+$/, '');
const UPSTREAM_TIMEOUT_MS = Number(ENV.UPSTREAM_TIMEOUT_MS) || 5000;
const PAGE_SIZE = Math.max(10, Math.min(100, Number(ENV.VOD_PAGE_SIZE) || 40));
// 卡密强制授权开关：设为 1 时，片库列表/详情/搜索/TVBox 配置/播放解析均需携带有效卡密。
// 默认关闭，保持向后兼容；线上 Vercel 配置 REQUIRE_CARD=1 开启。
const REQUIRE_CARD = ENV.REQUIRE_CARD === '1';
// 需要授权才能访问的路径（精确匹配）。公开路径：/api/health、/api/site、/api/ads、
// /api/card/verify、/img、/api/admin/*、/admin 与静态前端。
const AUTH_PROTECTED_PATHS = new Set([
  '/api/vod/tvbox',
  '/api/vod/list', '/api/vod/search', '/api/vod/detail',
  '/api/vod/kan', '/api/vod/channels', '/api/vod/channel',
  '/api.php/provide/vod', '/vod',
  '/play.m3u8', '/resolve',
  '/api/live/groups', '/api/live/channels', '/live.m3u',
]);
// Upstream providers cap every page at 20 rows regardless of the limit param.
const UPSTREAM_PAGE_SIZE = 20;
// Upstream sites rate-limit aggressive clients. Space requests per host, keep
// cached pages longer, and rest a failing host before touching it again.
const UPSTREAM_MIN_INTERVAL_MS = Math.max(0, Number(ENV.UPSTREAM_MIN_INTERVAL_MS ?? '400') || 0);
const UPSTREAM_COOLDOWN_MS = Math.max(0, Number(ENV.UPSTREAM_COOLDOWN_MS ?? '120000') || 0);
const UPSTREAM_BLOCK_COOLDOWN_MS = Math.max(0, Number(ENV.UPSTREAM_BLOCK_COOLDOWN_MS ?? '900000') || 0);
const IMAGE_PROXY_ENABLED = ENV.DISABLE_IMAGE_PROXY !== '1';
const PIC_KEYS = new Set(['vod_pic', 'vod_pic_slide', 'vod_pic_thumb']);
const CN_NUM = ['一', '二', '三', '四', '五', '六', '七', '八', '九', '十'];
const TTL = {
  // 采集结果落库更久：列表 60 分钟、详情 12 小时。上游魔都的剧集与地址都很稳定，
  // 前端刷新不必每次回源；优先用 UPSTREAM_*_TTL_MS 覆盖。
  list: Math.max(60000, Number(ENV.UPSTREAM_LIST_TTL_MS) || 3600000),
  detail: Math.max(60000, Number(ENV.UPSTREAM_DETAIL_TTL_MS) || 43200000),
  tvbox: 3600000,
};
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const LIVE_FETCH_TIMEOUT_MS = Math.max(1000, Number(ENV.LIVE_FETCH_TIMEOUT_MS) || 20000);
const LIVE_CACHE_TTL_MS = Math.max(0, Number(ENV.LIVE_CACHE_TTL_MS ?? '21600000') || 0);
// 冷启动保护：进程刚起来、快照还没采完时，首个请求最多同步等待这么久；
// 超时就先回 collecting 占位，把采集留在后台继续，避免整套采集撞上网关 30s 超时变成 504。
const LIVE_COLD_WAIT_MS = Math.max(0, Number(ENV.LIVE_COLD_WAIT_MS ?? '8000') || 0);
// 启动即预热直播快照（仅长驻进程，Vercel 每请求冷启动不适用）。
const LIVE_WARMUP = ENV.LIVE_WARMUP !== '0';
const LIVE_SOURCES = ENV.LIVE_SOURCES
  ? ENV.LIVE_SOURCES.split(',').map((url) => url.trim()).filter(Boolean).map((url, index) => ({ name: `自定义源${index + 1}`, url }))
  : DEFAULT_LIVE_SOURCES;

// Headline categories exposed by this site. The provider only tags leaves, so
// each headline category is the merge of its child type ids.
const CATEGORIES = [
  { id: 'new', name: '最新' },
  { id: 'hot', name: '最热' },
  { id: 'score', name: '高分推荐' },
  { id: '1', name: '电影' },
  { id: '2', name: '电视剧' },
  { id: '3', name: '综艺' },
  { id: '4', name: '动漫' },
  { id: '5', name: '短剧' },
  { id: '6', name: '预告片' },
  { id: '7', name: '体育' },
];
const CATEGORY_BY_ID = new Map(CATEGORIES.map((c) => [c.id, c]));
const LEAF_TO_CATEGORY = new Map(Object.entries(CHILD_TO_HEADLINE));
const CLASSES = CATEGORIES.map((c) => ({ type_id: c.id, type_pid: 0, type_name: c.name, children: (PROVIDER_TYPES[c.id]?.children || []).map((tid) => ({ type_id: tid, type_pid: c.id, type_name: TYPE_NAMES[tid] || '' })) }));

// Poster CDN hosts observed on the resource API. Unlisted hosts are left
// untouched so the browser can load them directly.
const IMAGE_HOST_RES = [
  /(^|\.)bfp\d+\.com$/i,
  /(^|\.)sinaimg\.cn$/i,
  /(^|\.)tupian\.click$/i,
  /(^|\.)zbds\.top$/i,
  /(^|\.)gitee\.com$/i,
  /(^|\.)mdzypic\.com$/i,
];

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8',
};

function sendJson(res, status, data) {
  const body = Buffer.from(JSON.stringify(data));
  writeBody(res, status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Cache-Control': 'no-store',
  }, body);
}

// 文本类响应按 Accept-Encoding 开 gzip：app.js 77KB 压到约 18KB，
// 直接降低 traesite 每日 10GiB 出网限额的消耗；二进制（图片/字体/媒体分片）不压。
const COMPRESSIBLE_TYPE_RE = /^text\/|json|javascript|svg\+xml|xml|mpegurl/i;
function writeBody(res, status, headers, body) {
  const req = res.req;
  const type = String(headers['Content-Type'] || '');
  const acceptsGzip = !!req && /\bgzip\b/.test(String(req.headers['accept-encoding'] || ''));
  const base = { ...headers, Vary: 'Accept-Encoding' };
  // Workers 由 Cloudflare 边缘自动压缩，这里只处理 Node 长驻进程。
  if (!isWorkers && acceptsGzip && COMPRESSIBLE_TYPE_RE.test(type)) {
    const gz = zlib.gzipSync(body, { level: 6 });
    res.writeHead(status, { ...base, 'Content-Encoding': 'gzip', 'Content-Length': gz.length });
    res.end(gz);
    return;
  }
  res.writeHead(status, { ...base, 'Content-Length': body.length });
  res.end(body);
}

// 公共只读响应：交给 Vercel 边缘 CDN 缓存，s-maxage 内不回源，降低冷启动与跨境延迟。
function sendJsonCacheable(res, status, data, { maxAge = 30, sMaxAge = 900, stale = 7200 } = {}) {
  // 授权模式下缩短边缘缓存：卡密可能已过期，避免边缘仍命中旧的 200 响应。
  if (REQUIRE_CARD) {
    sMaxAge = Math.min(sMaxAge, 60);
    stale = Math.min(stale, 120);
  }
  const body = Buffer.from(JSON.stringify(data));
  // 同一份内容固定同一枚弱 ETag：电视端重复进入列表/详情可拿到 304，既省流量也省解码。
  const etag = `W/"${body.length.toString(16)}-${crypto.createHash('sha1').update(body).digest('base64url').slice(0, 16)}"`;
  const cacheHeaders = {
    ETag: etag,
    'Cache-Control': `public, max-age=${maxAge}, s-maxage=${sMaxAge}, stale-while-revalidate=${stale}`,
    // Vercel 边缘会消费并剥掉 Cache-Control 里的 s-maxage / stale-while-revalidate 再转发，
    // 导致下游 Cloudflare 看不到缓存指令（cf-cache-status 一直是 DYNAMIC）。
    // 用 Cloudflare 专用头让 CF 边缘也缓存，且不影响浏览器与 Vercel 自身缓存。
    'Cloudflare-CDN-Cache-Control': `max-age=${sMaxAge}, stale-while-revalidate=${stale}`,
  };
  const req = res.req;
  const inm = String((req && req.headers && req.headers['if-none-match']) || '');
  if (inm && inm.split(',').some((token) => token.trim() === etag)) {
    res.writeHead(304, { ...cacheHeaders, 'Access-Control-Allow-Origin': '*', Vary: 'Accept-Encoding' });
    res.end();
    return;
  }
  writeBody(res, status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    ...cacheHeaders,
  }, body);
}

// Public base URL for client-facing links. The upstream origin never leaks.
function publicBase(req) {
  if (PUBLIC_URL) return PUBLIC_URL;
  const host = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  if (!host) return '';
  const proto = String(req.headers['x-forwarded-proto'] || 'http').split(',')[0].trim();
  return `${proto}://${host}`;
}

function imageHostAllowed(hostname) {
  const h = String(hostname || '').toLowerCase();
  return IMAGE_HOST_RES.some((re) => re.test(h));
}

function proxyImage(raw, base) {
  if (!IMAGE_PROXY_ENABLED || !raw || typeof raw !== 'string') return raw;
  let u = raw.trim();
  if (u.startsWith('//')) u = 'https:' + u;
  let parsed;
  try { parsed = new URL(u); } catch { return raw; }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return raw;
  if (!imageHostAllowed(parsed.hostname)) return raw;
  return `${base}/img?url=${encodeURIComponent(u)}`;
}

function mapTypeId(tid) {
  return LEAF_TO_CATEGORY.get(String(tid)) || String(tid);
}

const MEDIA_RE = /\.(m3u8|mp4|m4v|mpd)(?:[?#]|$)/i;

// Keep only playable media groups; 4K episode pages are resolved on demand.
function playableGroups(playUrl, allowEpisodePages = false) {
  const groups = [];
  for (const raw of String(playUrl || '').split(/\${3,}/)) {
    const eps = [];
    for (const pair of String(raw).split('#')) {
      const idx = pair.indexOf('$');
      if (idx < 0) continue;
      const mediaUrl = pair.slice(idx + 1).trim();
      let episodePage = false;
      if (allowEpisodePages) {
        try { episodePage = /^\/v_play\/[A-Za-z0-9_=-]+\.html$/.test(new URL(mediaUrl).pathname); } catch {}
      }
      if (!mediaUrl || (!MEDIA_RE.test(mediaUrl) && !episodePage)) continue;
      eps.push({ name: pair.slice(0, idx) || '播放', url: mediaUrl });
    }
    if (eps.length) groups.push(eps);
  }
  return groups;
}

function rewritePlay(vod, base) {
  const groups = playableGroups(vod.vod_play_url, String(vod.vod_id || '').startsWith('4kcz-'));
  if (!groups.length) return { from: '趣影一线', url: '' };
  const id = encodeURIComponent(String(vod.vod_id || ''));
  let seq = 0;
  const urls = groups.map((eps) => eps.map((ep) => {
    const link = `${base}/play.m3u8?id=${id}&p=${seq}`;
    seq += 1;
    return `${ep.name}$${link}`;
  }).join('#'));
  const from = groups.map((_g, i) => `趣影${CN_NUM[i] || (i + 1)}线`).join('$$$');
  return { from, url: urls.join('$$$') };
}

// ---- 双源播放列表：暴风=趣影一线，4K=趣影二线 ----
const PAIR_TTL_MS = 1800000;
const pairCache = new Map();

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms)),
  ]);
}

function firstGroupUrl(v, base) {
  const play = rewritePlay(v, base);
  return play.url ? play.url.split('$$$')[0] : '';
}

async function counterpartUrl(v, base) {
  // 魔都条目本身就是可播的，跨源「第二线路」对暴风已无意义（暴风地址全站 404）。
  if (MODU_ID_RE.test(String(v.vod_id || ''))) return '';
  const isFourk = String(v.vod_id || '').startsWith('4kcz-');
  const name = String(v.vod_name || '').trim();
  const want = name.toLowerCase();
  if (!want) return '';
  const key = `${isFourk ? 'f' : 'b'}:${want}:${v.vod_year || ''}`;
  const hit = pairCache.get(key);
  if (hit && Date.now() - hit.at < PAIR_TTL_MS) return hit.url;
  let url = '';
  const sameYear = (it) => !v.vod_year || !it.vod_year || String(it.vod_year) === String(v.vod_year);
  try {
    if (!isFourk) {
      const res = await withTimeout(fetchFourkSearch(name, 1), 8000);
      const cand = (res.list || []).find((it) => String(it.vod_name || '').trim().toLowerCase() === want && sameYear(it));
      if (cand) {
        const rows = await withTimeout(fetchFourkDetail(String(cand.vod_id)), 8000);
        const row = Array.isArray(rows) ? rows[0] : rows;
        if (row) url = firstGroupUrl(row, base);
      }
    } else {
      const data = await withTimeout(fetchBfzySearch(name, 1, true), 8000);
      const cand = (data.list || []).find((it) => String(it.vod_name || '').trim().toLowerCase() === want && sameYear(it));
      if (cand) url = firstGroupUrl(cand, base);
    }
  } catch {
    url = '';
  }
  pairCache.set(key, { at: Date.now(), url });
  return url;
}

async function withPlaySources(v, base) {
  if (!v || typeof v !== 'object' || String(v.vod_id || '').startsWith('manual-')) return v;
  const isFourk = String(v.vod_id || '').startsWith('4kcz-');
  const own = firstGroupUrl(v, base);
  const other = await counterpartUrl(v, base);
  const groups = [];
  if (isFourk) {
    if (other) groups.push({ from: '趣影一线', url: other });
    if (own) groups.push({ from: '趣影二线', url: own });
  } else {
    if (own) groups.push({ from: '趣影一线', url: own });
    if (other) groups.push({ from: '趣影二线', url: other });
  }
  if (!groups.length) return v;
  if (!other) groups[0].from = isFourk ? '趣影二线' : '趣影一线';
  return { ...v, __play: { from: groups.map((g) => g.from).join('$$$'), url: groups.map((g) => g.url).join('$$$') } };
}

// Normalise one upstream record into our public shape.
function normalizeItem(v, base, withPlay) {
  if (!v || typeof v !== 'object') return null;
  // Keep the original MacCMS type_id for client-side filtering, but show a
  // human-readable Chinese name (supports both headline ids and raw sub-type ids).
  const rawId = String(v.type_id || '');
  const item = { ...v };
  item.type_id = rawId;
  // 上游自带分类名时优先展示（4K 列表即为站点卡片上的原始标签）。
  item.type_name = v.type_name || categoryName(rawId) || '';
  for (const key of PIC_KEYS) if (typeof item[key] === 'string') item[key] = proxyImage(item[key], base);
  // 手动添加的影片直接使用后台填写的播放地址，不走上游播放页改写。
  const isManual = String(v.vod_id || '').startsWith('manual-');
  const play = withPlay && v.__play
    ? v.__play
    : withPlay && isManual
      ? { from: v.vod_play_from || '手动线路', url: v.vod_play_url || '' }
      : withPlay ? rewritePlay(v, base) : { from: '趣影一线', url: '' };
  delete item.__play;
  item.vod_play_from = play.from;
  item.vod_play_url = play.url;
  return item;
}

// 上游抓取：Node 用 http/https（可指定 TLS 1.2 绕开 4K WAF），
// Workers 用 fetch；两者返回结构一致，便于全站共用。
function httpGetText(target, opts = {}) {
  return runtimeGetText(target, { timeout: UPSTREAM_TIMEOUT_MS, ...opts });
}

// Build an upstream query string. The resource site sits behind a WAF rule
// that rejects literal "wd=", so the search key is sent percent-encoded.
function buildQuery(params) {
  const parts = [];
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === '') continue;
    const key = k === 'wd' ? '%77%64' : encodeURIComponent(k);
    parts.push(`${key}=${encodeURIComponent(String(v))}`);
  }
  return parts.join('&');
}

const jsonCache = new Map();
const inflightJson = new Map();
const hostSlots = new Map();
const hostCooldowns = new Map();

// Serialize requests to the same upstream host so a burst never leaves at once.
function hostSlot(key) {
  if (!UPSTREAM_MIN_INTERVAL_MS) return Promise.resolve();
  const now = Date.now();
  const start = Math.max(now, hostSlots.get(key) || 0);
  hostSlots.set(key, start + UPSTREAM_MIN_INTERVAL_MS);
  const wait = start - now;
  if (wait <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, wait));
}

function hostCooling(key) {
  const until = hostCooldowns.get(key) || 0;
  if (!until) return false;
  if (until > Date.now()) return true;
  hostCooldowns.delete(key);
  return false;
}

function coolHost(key, ms) {
  if (ms > 0) hostCooldowns.set(key, Date.now() + ms);
}

function upstreamFailure(message, { blocked = false } = {}) {
  const error = new Error(message);
  error.hostCool = true;
  error.blocked = blocked;
  return error;
}

// ---- Live-stream proxy helpers: serve HTTP live sources under our origin so HTTPS pages can play them ----
function isPrivateHost(hostname) {
  const h = String(hostname || '').toLowerCase().trim();
  if (!h) return true;
  if (/^(localhost|0\.0\.0\.0|broadcasthost|ip6-localhost|ip6-loopback|ip6-none|ip6-allnodes|ip6-allrouters)$/.test(h)) return true;
  const ipv4 = h.match(/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (ipv4) {
    const parts = ipv4[1].split('.').map(Number);
    const [a, b] = parts;
    if (a === 127 || a === 10) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    if (a === 169 && b === 254) return true;
    if (a === 0) return true;
  }
  return false;
}

function liveUrlAllowed(raw) {
  let parsed;
  try { parsed = new URL(raw); } catch { return false; }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return false;
  // 测试/开发环境允许 localhost 与 127.0.0.1 以便本地联调代理端点
  if (ENV.ALLOW_LOCAL_PROXY === '1') return true;
  if (isPrivateHost(parsed.hostname)) return false;
  return true;
}

function rewriteM3u8Segments(text, base, manifestUrl) {
  const manifest = new URL(manifestUrl);
  const proxySeg = (target) => `${base}/live-seg?url=${encodeURIComponent(target)}`;
  return String(text || '').split(/\r?\n/).map((line) => {
    const t = line.trim();
    if (!t || t.startsWith('#')) return line;
    if (/^https?:\/\//i.test(t)) return proxySeg(t);
    if (t.startsWith('//')) return proxySeg(manifest.protocol + t);
    try { return proxySeg(new URL(t, manifest).toString()); } catch { return line; }
  }).join('\n');
}

async function pipeUpstream(target, res, opts = {}) {
  let parsed;
  try { parsed = new URL(target); } catch { res.writeHead(400); res.end('bad url'); return; }
  if ((parsed.protocol !== 'https:' && parsed.protocol !== 'http:') || isPrivateHost(parsed.hostname)) {
    res.writeHead(403, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'forbidden host' }));
    return;
  }
  const headers = opts.headers || { 'User-Agent': UA, Accept: '*/*' };
  // 不跟随重定向：分片源可能 302 到 CDN，交给客户端跟随，避免服务端放大流量。
  const r = await httpGetText(parsed.toString(), { headers, timeout: opts.timeout || 15000 });
  if (r.error) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: 'upstream error' }));
    return;
  }
  const status = r.status;
  const ctype = String((r.headers && r.headers['content-type']) || '');
  if (status >= 300 && status < 400 && r.headers.location) {
    res.writeHead(302, { Location: r.headers.location, 'Cache-Control': 'no-store' });
    res.end();
    return;
  }
  if (status < 200 || status >= 400) {
    res.writeHead(status, { 'Content-Type': ctype || 'text/plain' });
    res.end(r.body);
    return;
  }
  const outHeaders = {};
  if (ctype) outHeaders['Content-Type'] = ctype;
  if (r.body && r.body.length) outHeaders['Content-Length'] = r.body.length;
  outHeaders['Cache-Control'] = 'public, max-age=60';
  outHeaders['Access-Control-Allow-Origin'] = '*';
  res.writeHead(status, outHeaders);
  res.end(r.body);
}

// 本地兜底清单：Node 读 assets/ 目录；Workers 通过 Static Assets 读取构建期清单。
async function loadLocalLiveSources() {
  if (!isWorkers) return readLocalLiveSources();
  const manifestText = await readAssetText('/assets/manifest.json');
  if (!manifestText) return [];
  let names = [];
  try { names = JSON.parse(manifestText); } catch { return []; }
  if (!Array.isArray(names)) return [];
  const sources = [];
  for (const name of names) {
    if (typeof name !== 'string' || !/\.m3u$/i.test(name) || name.includes('/')) continue;
    const text = await readAssetText(`/assets/${encodeURIComponent(name)}`);
    if (!text || !text.replace(/^\uFEFF/, '').trimStart().startsWith('#EXTM3U')) continue;
    sources.push({ name: `本地-${name.replace(/\.m3u$/i, '')}`, text, always: /-live\.m3u$/i.test(name) });
  }
  return sources;
}

const liveCollector = new LiveCollector({
  sources: LIVE_SOURCES,
  ttl: LIVE_CACHE_TTL_MS,
  disableLocal: ENV.LIVE_DISABLE_LOCAL === '1',
  loadLocalSources: loadLocalLiveSources,
  fetchText: async (target) => {
    const origin = new URL(target).origin;
    if (hostCooling(origin)) throw new Error('直播源冷却中');
    await hostSlot(origin);
    const r = await httpGetText(target, {
      headers: { 'User-Agent': UA, Accept: 'application/x-mpegURL,text/plain,*/*', 'Accept-Encoding': 'identity' },
      timeout: LIVE_FETCH_TIMEOUT_MS,
      deadline: Date.now() + LIVE_FETCH_TIMEOUT_MS,
      followRedirects: true,
    });
    if (r.error || r.status < 200 || r.status >= 300) {
      coolHost(origin, UPSTREAM_COOLDOWN_MS);
      throw new Error(r.error || `HTTP ${r.status}`);
    }
    if (!r.body || r.body.length > 40 * 1024 * 1024) throw new Error('直播清单为空或过大');
    const body = r.body.toString('utf8');
    if (!body.replace(/^\uFEFF/, '').trimStart().startsWith('#EXTM3U')) throw new Error('直播清单格式无效');
    return body;
  },
});

// 冷启动读快照：已有快照时照常返回（过期则由 LiveCollector 自行刷新）；
// 完全没有快照时，最多等 LIVE_COLD_WAIT_MS，等不到就返回 null，让调用方回 collecting 占位。
async function liveSnapshotOrNull() {
  if (liveCollector.snapshot) return liveCollector.getSnapshot();
  const pending = liveCollector.getSnapshot();
  pending.catch(() => {}); // 超时先行返回时，后台失败不能变成 unhandledRejection
  if (LIVE_COLD_WAIT_MS <= 0) return null;
  let timer = null;
  try {
    return await Promise.race([
      pending,
      new Promise((resolve) => {
        timer = setTimeout(() => resolve(null), LIVE_COLD_WAIT_MS);
        if (timer.unref) timer.unref();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function upstreamJson(params, ttl) {
  const qs = buildQuery(params);
  const cacheKey = `bfzy:${qs}`;
  const now = Date.now();
  const hit = jsonCache.get(cacheKey);
  if (hit && hit.expires > now) return hit.value;
  const running = inflightJson.get(cacheKey);
  if (running) return running;

  const task = (async () => {
    let lastError = null;
    for (const baseHost of UPSTREAM_HOSTS) {
      if (hostCooling(baseHost)) continue;
      await hostSlot(baseHost);
      const r = await httpGetText(`${baseHost}/api.php/provide/vod/?${qs}`, {
        headers: { 'User-Agent': UA, Accept: 'application/json,*/*', 'Accept-Encoding': 'identity', Referer: `${baseHost}/` },
        timeout: UPSTREAM_TIMEOUT_MS,
      });
      let data = null;
      if (!r.error && r.status >= 200 && r.status < 300) {
        try { data = JSON.parse(r.body.toString('utf8')); } catch {}
      }
      if (!data || typeof data !== 'object') {
        lastError = upstreamFailure('bfzy provider unavailable');
        coolHost(baseHost, UPSTREAM_COOLDOWN_MS);
        continue;
      }
      if (ttl > 0) {
        jsonCache.set(cacheKey, { expires: Date.now() + ttl, value: data });
        if (jsonCache.size > 400) {
          for (const [k, v] of jsonCache) if (v.expires <= Date.now()) jsonCache.delete(k);
        }
      }
      return data;
    }
    throw lastError || new Error('all upstream providers unavailable');
  })();

  inflightJson.set(cacheKey, task);
  try {
    return await task;
  } finally {
    inflightJson.delete(cacheKey);
  }
}

// 魔都资源（主源）的 JSON 抓取，结构与暴风一致，独立缓存键避免串味。
async function moduJson(params, ttl) {
  const qs = buildQuery(params);
  const cacheKey = `modu:${qs}`;
  const now = Date.now();
  const hit = jsonCache.get(cacheKey);
  if (hit && hit.expires > now) return hit.value;
  const running = inflightJson.get(cacheKey);
  if (running) return running;

  const task = (async () => {
    let lastError = null;
    for (const baseHost of MODU_HOSTS) {
      if (hostCooling(baseHost)) continue;
      await hostSlot(baseHost);
      const r = await httpGetText(`${baseHost}/api.php/provide/vod/?${qs}`, {
        headers: { 'User-Agent': UA, Accept: 'application/json,*/*', 'Accept-Encoding': 'identity', Referer: `${baseHost}/` },
        timeout: UPSTREAM_TIMEOUT_MS,
      });
      let data = null;
      if (!r.error && r.status >= 200 && r.status < 300) {
        try { data = JSON.parse(r.body.toString('utf8')); } catch {}
      }
      if (!data || typeof data !== 'object') {
        lastError = upstreamFailure('modu provider unavailable');
        coolHost(baseHost, UPSTREAM_COOLDOWN_MS);
        continue;
      }
      if (ttl > 0) {
        jsonCache.set(cacheKey, { expires: Date.now() + ttl, value: data });
        if (jsonCache.size > 400) {
          for (const [k, v] of jsonCache) if (v.expires <= Date.now()) jsonCache.delete(k);
        }
      }
      return data;
    }
    throw lastError || new Error('modu provider unavailable');
  })();

  inflightJson.set(cacheKey, task);
  try {
    return await task;
  } finally {
    inflightJson.delete(cacheKey);
  }
}

// 魔都的分类树和我们（暴风口径）的 id 完全不同，按中文分类名做映射。
let moduClassCache = null;

async function moduClasses() {
  const now = Date.now();
  if (moduClassCache && moduClassCache.expires > now) return moduClassCache.value;
  const data = await moduJson({ ac: 'list' }, 6 * 60 * 60 * 1000);
  const classes = (Array.isArray(data.class) ? data.class : []).map((c) => ({
    id: Number(c.type_id),
    pid: Number(c.type_pid),
    name: String(c.type_name || '').trim(),
  })).filter((c) => Number.isFinite(c.id));
  const byName = new Map();
  const byId = new Map();
  for (const c of classes) {
    if (c.name && !byName.has(c.name)) byName.set(c.name, c.id);
    byId.set(c.id, c);
  }
  const value = { classes, byName, byId };
  moduClassCache = { expires: now + 6 * 60 * 60 * 1000, value };
  return value;
}

// 我们的一级分类 → 魔都的一级分类名（魔都把动漫拆成多个根，短剧只有 AI 漫剧一路）。
const MODU_HEADLINE_NAMES = {
  '1': ['电影'],
  '2': ['连续剧'],
  '3': ['综艺'],
  '4': ['国产动漫', '日韩动漫', '欧美动漫', '港台动漫', '动漫电影'],
  '5': ['AI漫剧'],
  '7': ['体育'],
};
const MODU_NAME_ALIASES = {
  短剧: 'AI漫剧',
  短剧大全: 'AI漫剧',
  电视剧: '连续剧',
  动漫片: '国产动漫',
  记录片: '纪录片',
};
// 一级分类只取前若干个子类各 1 页：魔都单请求约 1.2s，枚举全部子类会让冷启动拖到十几秒。
const MODU_HEADLINE_LEAVES = 4;

async function moduTypeSet(cat) {
  const { classes, byName } = await moduClasses();
  if (cat.kind === 'headline') {
    const roots = (MODU_HEADLINE_NAMES[cat.value] || [])
      .map((name) => byName.get(name))
      .filter((id) => Number.isFinite(id));
    if (!roots.length) throw new Error('modu category unsupported');
    const ids = new Set(roots);
    let changed = true;
    while (changed) {
      changed = false;
      for (const c of classes) {
        if (!ids.has(c.id) && ids.has(c.pid)) { ids.add(c.id); changed = true; }
      }
    }
    return [...ids].sort((a, b) => a - b);
  }
  const name = TYPE_NAMES[cat.value] || '';
  const target = MODU_NAME_ALIASES[name] || name;
  const id = byName.get(target);
  if (!Number.isFinite(id)) throw new Error('modu category unsupported');
  return [id];
}

const fourkCache = new Map();

function fourkCacheGet(key) {
  const hit = fourkCache.get(key);
  return hit && hit.expires > Date.now() ? hit.value : null;
}

function fourkCacheSet(key, ttl, value) {
  if (ttl <= 0) return;
  fourkCache.set(key, { expires: Date.now() + ttl, value });
  if (fourkCache.size > 400) {
    for (const [k, v] of fourkCache) if (v.expires <= Date.now()) fourkCache.delete(k);
  }
}

function hostMatches(hostname, domain) {
  const host = String(hostname || '').toLowerCase();
  return host === domain || host.endsWith(`.${domain}`);
}

function fourkSourceAllowed(url) {
  return FOURKCZ_HOSTS.some((configured) => {
    try {
      const base = new URL(configured);
      const names = [base.hostname, base.hostname.startsWith('www.') ? base.hostname.slice(4) : `www.${base.hostname}`];
      return url.protocol === base.protocol && names.includes(url.hostname.toLowerCase());
    } catch { return false; }
  });
}

function fourkPlayerAllowed(url) {
  if (fourkSourceAllowed(url)) return true;
  return url.protocol === 'https:' && ['py1080p.com', '4kcz.com', 'czzy.top', 'cz4k.com']
    .some((domain) => hostMatches(url.hostname, domain));
}

function fourkMediaAllowed(url) {
  return url.protocol === 'https:' && ['bytetos.com', 'bytedtos.com', 'py1080p.com']
    .some((domain) => hostMatches(url.hostname, domain));
}

function decodeHtml(value) {
  const entities = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ' };
  return String(value || '').replace(/&(#x[\da-f]+|#\d+|amp|lt|gt|quot|apos|nbsp);/gi, (_m, entity) => {
    if (entity[0] !== '#') return entities[entity.toLowerCase()] || '';
    const hex = entity[1].toLowerCase() === 'x';
    const code = parseInt(entity.slice(hex ? 2 : 1), hex ? 16 : 10);
    return Number.isFinite(code) ? String.fromCodePoint(code) : '';
  });
}

function htmlAttr(tag, name) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = String(tag || '').match(new RegExp(`\\s${escaped}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, 'i'));
  return match ? decodeHtml(match[1] ?? match[2] ?? match[3] ?? '') : '';
}

function htmlText(value) {
  return decodeHtml(String(value || '').replace(/<script\b[\s\S]*?<\/script>|<style\b[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')).replace(/\s+/g, ' ').trim();
}

function cleanField(value) {
  return htmlText(value).replace(/\$/g, '＄').replace(/#/g, '＃');
}

function assertFourkHtml(html) {
  if (!/<html[\s>]/i.test(html)) throw upstreamFailure('4K provider returned non-html');
  if (/访问已被拦截|站点已经被管理员停止运行|管理员停止运行|人机验证|验证码/.test(html)) {
    throw upstreamFailure('4K provider unavailable', { blocked: true });
  }
}

async function fourkFetch(baseHost, path, deadline) {
  const target = new URL(path, `${baseHost}/`);
  if (!fourkSourceAllowed(target)) throw new Error('4K source url rejected');
  const queued = Date.now();
  await hostSlot(baseHost);
  const effective = deadline ? deadline + (Date.now() - queued) : undefined;
  const r = await httpGetText(target.toString(), {
    headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml,*/*', 'Accept-Encoding': 'identity', Referer: `${baseHost}/` },
    deadline: effective,
    followRedirects: true,
    redirectAllowed: fourkSourceAllowed,
    tls12: true,
  });
  const html = r.body ? r.body.toString('utf8') : '';
  if (r.error || r.status < 200 || r.status >= 300) throw upstreamFailure('4K source request failed');
  assertFourkHtml(html);
  return html;
}

function parseFourkListing(html, baseHost, options = {}) {
  assertFourkHtml(html);
  const page = options.page || 1;
  const typeId = options.typeId || '1';
  const rows = [];
  const seen = new Set();
  for (const item of String(html).matchAll(/<li\b[^>]*>[\s\S]*?<\/li>/gi)) {
    const block = item[0];
    if (!/\bdytit\b/.test(block)) continue;
    const heading = [...block.matchAll(/<h3\b[^>]*>[\s\S]*?<\/h3>/gi)]
      .map((match) => match[0])
      .find((tag) => /(?:^|\s)dytit(?:\s|$)/.test(htmlAttr(tag, 'class'))) || '';
    let link = null;
    let id = '';
    let name = '';
    for (const anchor of heading.matchAll(/<a\b[^>]*>[\s\S]*?<\/a>/gi)) {
      try {
        const href = htmlAttr(anchor[0], 'href');
        const match = new URL(href, `${baseHost}/`).pathname.match(/^\/movie\/(\d{1,8})\.html$/);
        if (!match) continue;
        link = anchor[0];
        id = match[1];
        name = htmlText(anchor[0]);
        break;
      } catch {}
    }
    if (!link || !id || seen.has(id)) continue;
    seen.add(id);
    const image = block.match(/<img\b[^>]*>/i)?.[0] || '';
    const badge = block.match(/<[^>]*class=["'][^"']*\bjidi\b[^"']*["'][^>]*>([\s\S]*?)<\/[^>]+>/i)?.[1] || '';
    const actor = block.match(/<[^>]*class=["'][^"']*\binzhuy\b[^"']*["'][^>]*>([\s\S]*?)<\/[^>]+>/i)?.[1] || '';
    // 卡片上的分类/清晰度标签，与 4K 站列表展示保持一致。
    const cardLabel = htmlText(block.match(/<span\b[^>]*class=["'][^"']*\b(?:furk|qb)\b[^"']*["'][^>]*>([\s\S]*?)<\/span>/i)?.[1] || '');
    const labelType = FOURK_CARD_TYPES[cardLabel] || '';
    let pic = '';
    try {
      const src = htmlAttr(image, 'data-original') || htmlAttr(image, 'src');
      if (src) pic = new URL(src, `${baseHost}/`).toString();
    } catch {}
    rows.push({
      vod_id: `4kcz-${id}`,
      vod_name: name,
      vod_pic: pic,
      vod_remarks: htmlText(badge),
      vod_year: '',
      vod_actor: htmlText(actor).split('：').slice(1).join('：').trim(),
      type_id: labelType ? String(labelType) : typeId,
      type_name: labelType
        ? (FOURK_QUALITY_LABELS.has(cardLabel) ? categoryName(labelType) : cardLabel)
        : categoryName(typeId),
    });
  }
  if (!rows.length && !options.allowEmpty) throw new Error('4K listing structure changed');
  if (!rows.length && options.allowEmpty && !/<main\b|\bmi_cont\b/.test(html)) throw new Error('4K search structure changed');
  const currentPath = (options.path || '/movie_bt').replace(/\/page\/\d+\/?$/, '').replace(/\/$/, '');
  const pages = [1];
  for (const anchor of String(html).matchAll(/<a\b[^>]*>/gi)) {
    try {
      const pathname = new URL(htmlAttr(anchor[0], 'href'), `${baseHost}/`).pathname;
      const match = pathname.match(new RegExp(`^${currentPath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/page/(\\d+)/?$`));
      if (match) pages.push(Number(match[1]));
    } catch {}
  }
  const pagecount = Math.max(...pages);
  const pageSize = Math.max(24, rows.length);
  const total = page === pagecount ? (pagecount - 1) * pageSize + rows.length : pagecount * pageSize;
  return { page, pagecount, total, list: rows };
}

function parseFourkDetail(html, baseHost, id) {
  assertFourkHtml(html);
  const heading = html.match(/<h1\b[^>]*>([\s\S]*?)<\/h1>/i)?.[1] || '';
  const name = htmlText(heading);
  if (!name) throw new Error('4K detail structure changed');
  const info = {};
  for (const item of String(html).matchAll(/<li\b[^>]*>([\s\S]*?)<\/li>/gi)) {
    const text = htmlText(item[1]);
    const idx = text.indexOf('：');
    if (idx > 0) info[text.slice(0, idx).trim()] = text.slice(idx + 1).trim();
  }
  const imageBlock = html.match(/<[^>]*class=["'][^"']*\bdyimg\b[^"']*["'][^>]*>[\s\S]*?<\/[^>]+>/i)?.[0] || '';
  const image = imageBlock.match(/<img\b[^>]*>/i)?.[0] || '';
  let pic = '';
  try { if (image) pic = new URL(htmlAttr(image, 'src'), `${baseHost}/`).toString(); } catch {}
  let plot = '';
  for (const meta of String(html).matchAll(/<meta\b[^>]*>/gi)) {
    const kind = htmlAttr(meta[0], 'property') || htmlAttr(meta[0], 'name');
    if ((kind === 'og:description' || kind === 'description') && htmlAttr(meta[0], 'content')) {
      plot = htmlAttr(meta[0], 'content');
      break;
    }
  }
  const episodes = [];
  for (const anchor of String(html).matchAll(/<a\b[^>]*>[\s\S]*?<\/a>/gi)) {
    try {
      const target = new URL(htmlAttr(anchor[0], 'href'), `${baseHost}/`);
      if (!/^\/v_play\/[A-Za-z0-9_=-]+\.html$/.test(target.pathname)) continue;
      episodes.push({ name: cleanField(anchor[0]) || '播放', url: target.toString() });
    } catch {}
  }
  if (!episodes.length) throw new Error('4K detail has no episodes');
  const kind = `${info['类型'] || ''} ${info['分类'] || ''}`;
  const typeId = /综艺/.test(kind) ? '3' : /动漫|番剧/.test(kind) ? '4' : /短剧/.test(kind) ? '5' : /电视剧|国产剧|美剧|韩剧|日剧|欧美剧/.test(kind) || episodes.length > 1 ? '2' : '1';
  return {
    vod_id: id,
    vod_name: name,
    type_id: typeId,
    type_name: categoryName(typeId),
    vod_pic: pic,
    vod_year: info['年份'] || '',
    vod_area: info['地区'] || '',
    vod_lang: info['语言'] || '',
    vod_actor: info['主演'] || '',
    vod_director: info['导演'] || '',
    vod_remarks: episodes[0].name,
    vod_content: plot,
    vod_play_from: '4kcz',
    vod_play_url: episodes.map((ep) => `${ep.name}$${ep.url}`).join('#'),
  };
}

const inflightFourk = new Map();

async function withFourkHosts(key, ttl, operation) {
  const cached = fourkCacheGet(key);
  if (cached) return cached;
  const running = inflightFourk.get(key);
  if (running) return running;

  const task = (async () => {
    let lastError = null;
    for (const baseHost of FOURKCZ_HOSTS) {
      if (hostCooling(baseHost)) continue;
      const deadline = Date.now() + UPSTREAM_TIMEOUT_MS;
      try {
        const value = await operation(baseHost, deadline);
        fourkCacheSet(key, ttl, value);
        return value;
      } catch (error) {
        lastError = error;
        if (error && error.hostCool) {
          coolHost(baseHost, error.blocked ? UPSTREAM_BLOCK_COOLDOWN_MS : UPSTREAM_COOLDOWN_MS);
        }
      }
    }
    throw lastError || new Error('all 4K providers unavailable');
  })();

  inflightFourk.set(key, task);
  try {
    return await task;
  } finally {
    inflightFourk.delete(key);
  }
}

async function fetchFourkList({ pg, t }) {
  const page = parsePage(pg);
  // t 已是 4K 支持的一级分类 id（'1'-'5'）；传入对象会导致分类路径查不到而误回退暴风源。
  const cid = String(t || '').trim();
  const paths = cid ? FOURKCZ_CATEGORY_PATHS[cid] : ['/movie_bt'];
  if (!paths) throw new Error('4K category unsupported');
  const pagesNeeded = Math.max(1, Math.ceil(PAGE_SIZE / UPSTREAM_PAGE_SIZE));
  const upstreamStart = (page - 1) * pagesNeeded + 1;
  return withFourkHosts(`list:${cid}:${page}:${PAGE_SIZE}`, TTL.list, async (baseHost, deadline) => {
    const seen = new Set();
    const list = [];
    let totalPagecount = 1;
    let totalAll = 0;
    const jobs = [];
    for (const basePath of paths) {
      for (let i = 0; i < pagesNeeded; i += 1) {
        const upPage = upstreamStart + i;
        jobs.push({ upPage, path: upPage > 1 ? `${basePath}/page/${upPage}` : basePath });
      }
    }
    // 4K 站是 HTML 页面，逐页串行时每页都要单独等往返；两页并行足以减半首屏等待。
    const parsedPages = new Array(jobs.length).fill(null);
    await runLimited(jobs, 2, async (job) => {
      const index = jobs.indexOf(job);
      try {
        parsedPages[index] = parseFourkListing(
          await fourkFetch(baseHost, job.path, deadline),
          baseHost,
          { page: job.upPage, path: job.path, typeId: cid || '1' },
        );
      } catch (e) {
        console.warn(`[quying] 4K page ${job.path} failed: ${String(e && e.message || e)}`);
      }
    });
    for (const parsed of parsedPages) {
      if (!parsed) continue;
      totalPagecount = Math.max(totalPagecount, parsed.pagecount || 1);
      totalAll += Number(parsed.total) || 0;
      for (const row of (Array.isArray(parsed.list) ? parsed.list : [])) {
        if (seen.has(row.vod_id)) continue;
        seen.add(row.vod_id);
        if (cid) { row.type_id = cid; row.type_name = categoryName(cid); }
        list.push(row);
      }
    }
    // 全部页面都抓失败时向上抛出，交给镜像切换逻辑处理。
    if (!list.length && jobs.length) throw new Error('4K list pages unavailable');
    return {
      page,
      pagecount: Math.max(1, Math.floor(totalPagecount / pagesNeeded)),
      total: Math.ceil(totalAll / paths.length) * paths.length,
      list: list.slice(0, PAGE_SIZE),
    };
  });
}

async function fetchFourkSearch(wd, pg) {
  const page = parsePage(pg);
  const path = `/search.php?searchword=${encodeURIComponent(wd)}&page=${page}`;
  return withFourkHosts(`search:${wd}:${page}`, TTL.list, async (baseHost, deadline) => {
    const html = await fourkFetch(baseHost, path, deadline);
    return parseFourkListing(html, baseHost, { page, path: '/search.php', allowEmpty: true });
  });
}

async function fetchFourkDetail(id) {
  const match = /^4kcz-(\d{1,8})$/.exec(id);
  if (!match) throw new Error('invalid 4K id');
  return withFourkHosts(`detail:${id}`, TTL.detail, async (baseHost, deadline) => {
    const html = await fourkFetch(baseHost, `/movie/${match[1]}.html`, deadline);
    return parseFourkDetail(html, baseHost, id);
  });
}

// 上游每个镜像都有排队间隔，串行叠加会让首屏多等好几秒；这里限定并发跑完一批请求。
async function runLimited(items, limit, worker) {
  if (!items.length) return;
  const size = Math.max(1, Math.min(limit || 1, items.length));
  let cursor = 0;
  const runners = [];
  for (let i = 0; i < size; i += 1) {
    runners.push((async () => {
      while (cursor < items.length) {
        const item = items[cursor];
        cursor += 1;
        await worker(item);
      }
    })());
  }
  await Promise.all(runners);
}

function parsePage(value) {
  const n = parseInt(String(value || '1'), 10);
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(n, 99999);
}

function parseYear(value) {
  const s = String(value || '').trim();
  return /^\d{4}$/.test(s) ? s : '';
}

function categoryParam(value) {
  const s = String(value || '').trim();
  // Accept our headline ids ('1'-'5') or raw MacCMS type_id numbers.
  if (CATEGORY_BY_ID.has(s)) return { kind: 'headline', value: s, headline: s };
  if (/^\d{1,3}$/.test(s) && TYPE_NAMES[s]) {
    const n = Number(s);
    const headline = HEAD_TO_HEADLINE[n] || CHILD_TO_HEADLINE[n] || '';
    return { kind: 'raw', value: n, headline };
  }
  return null;
}

function categoryName(cid) {
  const c = CATEGORY_BY_ID.get(String(cid));
  if (c) return c.name;
  const n = Number(cid);
  return TYPE_NAMES[n] || '';
}

// bfzy list. For a headline category, child types are merged by page.
// Also accepts a raw MacCMS type_id number to fetch that specific sub-category.
async function fetchBfzyList({ pg, t, year, withPlay }) {
  const page = parsePage(pg);
  const pagesNeeded = Math.max(1, Math.ceil(PAGE_SIZE / UPSTREAM_PAGE_SIZE));
  const upstreamStart = (page - 1) * pagesNeeded + 1;
  const cat = categoryParam(t);

  // Build the list of MacCMS type_ids to query.
  let macTypes = null; // null = no type filter; array of nums = specific types
  if (cat) {
    if (cat.kind === 'headline') {
      macTypes = PROVIDER_TYPES[cat.value].children;
    } else {
      // raw MacCMS type_id — fetch exactly that sub-category
      macTypes = [cat.value];
    }
  }
  // Fetch from bfzy with the resolved type filter.

  const seen = new Set();
  const merged = [];
  let total = 0;
  let upstreamPagecount = 1;

  // Helper to fetch one upstream page for one type filter and merge in.
  async function mergeOnePage(typeNum, upPage) {
    const params = { ac: withPlay ? 'detail' : 'list', pg: upPage };
    if (typeNum != null) params.t = typeNum;
    if (year) params.year = year;
    const data = await upstreamJson(params, TTL.list).catch(() => null);
    if (!data) return;
    total += Number(data.total) || 0;
    upstreamPagecount = Math.max(upstreamPagecount, Number(data.pagecount) || 1);
    for (const item of (Array.isArray(data.list) ? data.list : [])) {
      const key = String(item.vod_id || '');
      if (!key || seen.has(key)) continue;
      seen.add(key);
      merged.push(item);
    }
  }

  const jobs = [];
  if (macTypes && macTypes.length) {
    for (const typeNum of macTypes) {
      for (let i = 0; i < pagesNeeded; i += 1) jobs.push([typeNum, upstreamStart + i]);
    }
  } else {
    for (let i = 0; i < pagesNeeded; i += 1) jobs.push([null, upstreamStart + i]);
  }
  // 一个分类可能要抓 2 页 × 多个子分类，串行时每页都要等满排队间隔 + 网络往返。
  // 上游按域名排队，这里并发 4 个只是把等待重叠起来，不会突破镜像的节奏。
  await runLimited(jobs, 4, ([typeNum, upPage]) => mergeOnePage(typeNum, upPage));

  merged.sort((a, b) => String(b.vod_time || '').localeCompare(String(a.vod_time || '')));
  const list = merged.slice(0, PAGE_SIZE);
  if (!withPlay) await hydrate(list);
  return {
    page,
    pagecount: Math.max(1, Math.floor(upstreamPagecount / pagesNeeded)),
    total,
    list,
  };
}

// Slim list rows omit year/area on this provider; fill them from one batch
// detail call for the visible page.
async function hydrate(list) {
  // 只有暴风精简列表缺字段需要补抓；魔都取的就是 detail，4K/手动条目也不走这里。
  const ids = list.map((v) => String(v.vod_id || '')).filter((id) => /^\d{1,10}$/.test(id)).slice(0, PAGE_SIZE).join(',');
  if (!ids) return;
  const data = await upstreamJson({ ac: 'detail', ids }, TTL.list).catch(() => null);
  if (!data || !Array.isArray(data.list)) return;
  const byId = new Map(data.list.map((v) => [String(v.vod_id), v]));
  for (let i = 0; i < list.length; i += 1) {
    const full = byId.get(String(list[i].vod_id));
    if (full) list[i] = { ...list[i], ...full };
  }
}

async function hydrateFourk(list) {
  let hydrated = 0;
  const rows = [];
  // One detail page at a time: the 4K mirrors rate-limit bursts.
  for (const row of list) {
    try {
      const full = await fetchFourkDetail(String(row.vod_id));
      hydrated += 1;
      rows.push({ ...row, ...full });
    } catch {
      rows.push(row);
    }
  }
  if (list.length && !hydrated) throw new Error('4K details unavailable');
  return rows;
}

async function fetchListLive({ pg, t, year, withPlay }) {
  // 主源先行：魔都播放地址可用，分类按中文名映射，支持一级分类和原始子分类。
  try {
    const data = await fetchModuList({ pg, t, year, withPlay });
    if (data && Array.isArray(data.list) && data.list.length) return data;
  } catch {}
  const cat = categoryParam(t);
  // Raw MacCMS type_ids (sub-categories like 21=动作片) must go to bfzy,
  // because 4K only supports headline categories and cannot filter by sub-type.
  if (cat && cat.kind === 'raw') return fetchBfzyList({ pg, t, year, withPlay });
  // Determine the headline id for 4K path lookup.
  const headlineId = cat
    ? (cat.kind === 'headline' ? cat.value : cat.headline)
    : '';
  const canUse4k = !year && (!headlineId || FOURKCZ_CATEGORY_PATHS[headlineId]);
  if (canUse4k) {
    try {
      const data = await fetchFourkList({ pg, t: headlineId });
      if (withPlay) data.list = await hydrateFourk(data.list);
      return data;
    } catch {}
  }
  return fetchBfzyList({ pg, t, year, withPlay });
}

async function fetchBfzyDetail(ids) {
  const data = await upstreamJson({ ac: 'detail', ids }, TTL.detail);
  return Array.isArray(data.list) ? data.list : [];
}

async function fetchDetailLive(ids) {
  const idList = String(ids).split(',');
  const bfzyIds = [...new Set(idList.filter((id) => /^\d{1,10}$/.test(id)))];
  const fourkIds = [...new Set(idList.filter((id) => /^4kcz-\d{1,8}$/.test(id)))];
  const moduIds = [...new Set(idList.filter((id) => MODU_ID_RE.test(id)))];
  if (bfzyIds.length + fourkIds.length + moduIds.length !== idList.length) throw new Error('invalid vod id');
  const byId = new Map();
  const tasks = [];
  if (moduIds.length) {
    tasks.push(fetchModuDetail(moduIds).then((rows) => {
      for (const row of rows) byId.set(String(row.vod_id), row);
    }));
  }
  if (bfzyIds.length) {
    tasks.push(fetchBfzyDetail(bfzyIds.join(',')).then((rows) => {
      for (const row of rows) byId.set(String(row.vod_id), row);
    }));
  }
  for (const id of fourkIds) {
    tasks.push(fetchFourkDetail(id).then((row) => byId.set(id, row)));
  }
  await Promise.all(tasks);
  return idList.map((id) => byId.get(id)).filter(Boolean);
}

async function fetchBfzySearch(wd, pg, withPlay) {
  const data = await upstreamJson({ ac: withPlay ? 'detail' : 'list', wd, pg: parsePage(pg) }, TTL.list);
  const list = Array.isArray(data.list) ? data.list : [];
  if (!withPlay) await hydrate(list);
  return {
    page: parsePage(pg),
    pagecount: Number(data.pagecount) || 1,
    total: Number(data.total) || 0,
    list,
  };
}

async function fetchSearchLive(wd, pg, withPlay) {
  try {
    const data = await fetchModuSearch(wd, pg, withPlay);
    if (data && Array.isArray(data.list) && data.list.length) return data;
  } catch {}
  try {
    const data = await fetchFourkSearch(wd, pg);
    if (withPlay) data.list = await hydrateFourk(data.list);
    return data;
  } catch {}
  return fetchBfzySearch(wd, pg, withPlay);
}

// ---- 魔都资源（主源）----
// 魔都一页固定 20 条，`ac=detail` 直接带全集播放地址，所以列表也用 detail，省掉二次回源。
async function fetchModuList({ pg, t, year, withPlay }) {
  const page = parsePage(pg);
  const pagesNeeded = Math.max(1, Math.ceil(PAGE_SIZE / UPSTREAM_PAGE_SIZE));
  const upstreamStart = (page - 1) * pagesNeeded + 1;
  const cat = categoryParam(t);
  const types = cat ? await moduTypeSet(cat) : null;
  const seen = new Set();
  const merged = [];
  let total = 0;
  let upstreamPagecount = 1;

  async function mergeOne(typeNum, upPage) {
    const params = { ac: 'detail', pg: upPage };
    if (typeNum != null) params.t = typeNum;
    if (year) params.year = year;
    const data = await moduJson(params, TTL.list).catch(() => null);
    if (!data) return;
    total += Number(data.total) || 0;
    upstreamPagecount = Math.max(upstreamPagecount, Number(data.pagecount) || 1);
    for (const item of (Array.isArray(data.list) ? data.list : [])) {
      const key = String(item.vod_id || '');
      if (!key || seen.has(key)) continue;
      seen.add(key);
      merged.push(item);
    }
  }

  const jobs = [];
  if (types && types.length) {
    const budget = cat.kind === 'headline' ? Math.min(types.length, MODU_HEADLINE_LEAVES) : types.length;
    const chosen = types.slice(0, budget);
    const perLeafPages = Math.max(1, Math.ceil(PAGE_SIZE / (UPSTREAM_PAGE_SIZE * chosen.length)));
    for (const typeNum of chosen) {
      for (let i = 0; i < perLeafPages; i += 1) jobs.push([typeNum, upstreamStart + i]);
    }
  } else {
    for (let i = 0; i < pagesNeeded; i += 1) jobs.push([null, upstreamStart + i]);
  }
  await runLimited(jobs, 4, ([typeNum, upPage]) => mergeOne(typeNum, upPage));

  // 一级分类只取了前几个子类；不够一屏时把其余子类各补一页。
  if (types && types.length && merged.length < PAGE_SIZE) {
    const done = new Set(jobs.map(([typeNum]) => typeNum));
    const extra = types.filter((typeNum) => !done.has(typeNum));
    if (extra.length) {
      await runLimited(extra.map((typeNum) => [typeNum, upstreamStart]), 4, ([typeNum, upPage]) => mergeOne(typeNum, upPage));
    }
  }

  if (!merged.length) throw new Error('modu list unavailable');
  merged.sort((a, b) => String(b.vod_time || '').localeCompare(String(a.vod_time || '')));
  const list = merged.slice(0, PAGE_SIZE).map((v) => ({ ...v, vod_id: `${MODU_ID_PREFIX}${v.vod_id}` }));
  return {
    page,
    pagecount: Math.max(1, Math.floor(upstreamPagecount / pagesNeeded)),
    total,
    list,
  };
}

async function fetchModuDetail(ids) {
  const clean = ids.map((id) => String(id).replace(MODU_ID_PREFIX, '')).filter((id) => /^\d{1,10}$/.test(id));
  if (!clean.length) return [];
  const data = await moduJson({ ac: 'detail', ids: clean.join(',') }, TTL.detail);
  const rows = Array.isArray(data.list) ? data.list : [];
  return rows.map((v) => ({ ...v, vod_id: `${MODU_ID_PREFIX}${v.vod_id}` }));
}

async function fetchModuSearch(wd, pg, withPlay) {
  const data = await moduJson({ ac: 'detail', wd, pg: parsePage(pg) }, TTL.list);
  const list = (Array.isArray(data.list) ? data.list : []).map((v) => ({ ...v, vod_id: `${MODU_ID_PREFIX}${v.vod_id}` }));
  return {
    page: parsePage(pg),
    pagecount: Number(data.pagecount) || 1,
    total: Number(data.total) || list.length,
    list,
  };
}

let catalogWarningShown = false;
function catalogFailed(error) {
  if (catalogWarningShown) return;
  catalogWarningShown = true;
  console.warn(`[quying] catalog store error: ${String((error && error.message) || error)}`);
}

// 数据库读超时兜底：Neon 冷启动偶尔要几秒，超过 2 秒就当作未命中，避免拖垮播放解析。
function withTimeout(promise, ms, fallback) {
  return Promise.race([
    promise,
    new Promise((resolve) => setTimeout(() => resolve(fallback), ms)),
  ]);
}

async function readCatalogQuery(key) {
  if (!STORES.catalogStore) return null;
  try {
    return await withTimeout(STORES.catalogStore.getQuery(key), 2000, null);
  } catch (error) { catalogFailed(error); return null; }
}

async function saveCatalogQuery(key, value, ttl) {
  if (!STORES.catalogStore) return false;
  try {
    const done = await withTimeout(STORES.catalogStore.saveQuery(key, value, ttl).then(() => true), 4000, false);
    if (!done) catalogFailed(new Error('save timeout'));
    return done;
  } catch (error) { catalogFailed(error); return false; }
}

const catalogRefreshQueue = [];
const catalogRefreshing = new Set();
const catalogRefreshRetryAt = new Map();
let catalogRefreshActive = false;

async function drainCatalogRefresh() {
  while (catalogRefreshQueue.length) {
    const task = catalogRefreshQueue.shift();
    try {
      const value = await task.loader();
      catalogRefreshRetryAt.delete(task.key);
      if (!(await saveCatalogQuery(task.key, value, task.ttl))) {
        catalogRefreshRetryAt.set(task.key, Date.now() + UPSTREAM_COOLDOWN_MS);
      }
    } catch {
      // Stale rows stay available; avoid refreshing the same failed key per request.
      catalogRefreshRetryAt.set(task.key, Date.now() + UPSTREAM_COOLDOWN_MS);
    } finally {
      catalogRefreshing.delete(task.key);
    }
  }
  catalogRefreshActive = false;
}

function scheduleCatalogRefresh(key, ttl, loader) {
  if (!STORES.catalogStore || catalogRefreshing.has(key)) return;
  if ((catalogRefreshRetryAt.get(key) || 0) > Date.now()) return;
  catalogRefreshing.add(key);
  catalogRefreshQueue.push({ key, ttl, loader });
  if (!catalogRefreshActive) {
    catalogRefreshActive = true;
    setImmediate(drainCatalogRefresh);
  }
}

// 实例级页面缓存：没有配置数据库时也能让热实例直接返回整页，
// 不必每次刷新都回采集源。有效期取 TTL（列表 60 分钟、详情 12 小时）。
const PAGE_MEM = new Map();
const PAGE_MEM_MAX = 6 * 60 * 60 * 1000;
const PAGE_MEM_KEYS_MAX = 400;

function pageMemGet(key) {
  const entry = PAGE_MEM.get(key);
  if (!entry) return null;
  if (entry.exp <= Date.now()) { PAGE_MEM.delete(key); return null; }
  return entry.value;
}

function pageMemSet(key, value, ttl) {
  if (!value) return;
  if (PAGE_MEM.size >= PAGE_MEM_KEYS_MAX) PAGE_MEM.clear();
  PAGE_MEM.set(key, { value, exp: Date.now() + Math.min(Math.max(ttl, 60000), PAGE_MEM_MAX) });
}

async function catalogQuery(key, ttl, loader, localLoader) {
  const memo = pageMemGet(key);
  if (memo) return memo;
  const cached = await readCatalogQuery(key);
  if (cached) {
    if (!cached.stale) return cached.value;
    // Serverless 实例在响应结束后随时会被回收，后台刷新往往来不及落库。
    // 代价是过期后的第一次请求要同步刷新，但能保证采集结果必定写入数据库。
    if (ENV.VERCEL || isWorkers) {
      try {
        const value = await loader();
        await saveCatalogQuery(key, value, ttl);
        return value;
      } catch {
        return cached.value;
      }
    }
    scheduleCatalogRefresh(key, ttl, loader);
    return cached.value;
  }
  if (STORES.catalogStore && localLoader) {
    let local = null;
    try { local = await localLoader(); } catch (error) { catalogFailed(error); }
    if (local && Array.isArray(local.list) && local.list.length) {
      if (await saveCatalogQuery(key, local, ttl)) scheduleCatalogRefresh(key, ttl, loader);
      return local;
    }
  }
  const value = await loader();
  await saveCatalogQuery(key, value, ttl);
  pageMemSet(key, value, ttl);
  return value;
}

function listCacheKey({ pg, t, year, withPlay } = {}) {
  // 查询键必须用字符串，否则原始二级分类会全部落到同一个 [object Object] 缓存上。
  const cat = categoryParam(t);
  const catKey = cat ? `${cat.kind}:${cat.value}` : '';
  return `list:v6:${withPlay ? 1 : 0}:${parsePage(pg)}:${PAGE_SIZE}:${catKey}:${parseYear(year)}`;
}

const RANK_KINDS = new Set(['new', 'hot', 'score']);
const RANK_SOURCE_PAGES = 6;

// ---- 豆瓣榜单 ----：最新/最热/高分的候选与排序来自豆瓣，再按片名精确匹配本站可播片源。
// 豆瓣不可用（被墙/风控/结构变化）时自动回退到上游排序。
const DOUBAN_API_BASE = 'https://m.douban.com/rexxar/api/v2/subject_collection';
const DOUBAN_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const DOUBAN_REFERER = 'https://m.douban.com/movie/';
// 最新 = 豆瓣新片榜；最热 = 实时热门电影 + 热门剧集；高分 = 豆瓣电影 Top250。
const DOUBAN_COLLECTIONS = {
  new: ['movie_latest'],
  hot: ['movie_hot', 'tv_hot'],
  score: ['movie_top250'],
};
const DOUBAN_CANDIDATE_LIMIT = 100; // 每个榜单最多取多少条豆瓣候选
const DOUBAN_RESOLVE_LIMIT = 100;   // 最多为多少条候选做片名匹配
const RANK_POOL_LIMIT = 80;         // 榜单池上限，决定可翻页的总条数

// 直接向指定镜像发起一次搜索请求；镜像各自独立排队，并行匹配时可分摊压力。
async function bfzySearchOnHost(host, wd, isModu = false) {
  await hostSlot(host);
  const qs = buildQuery({ ac: isModu ? 'detail' : 'list', wd, pg: 1 });
  const r = await httpGetText(`${host}/api.php/provide/vod/?${qs}`, {
    headers: { 'User-Agent': UA, Accept: 'application/json,*/*', 'Accept-Encoding': 'identity', Referer: `${host}/` },
    timeout: UPSTREAM_TIMEOUT_MS,
  });
  if (r.error || r.status < 200 || r.status >= 300) {
    coolHost(host, UPSTREAM_COOLDOWN_MS);
    return null;
  }
  let data;
  try { data = JSON.parse(r.body.toString('utf8')); } catch { return null; }
  const rows = Array.isArray(data && data.list) ? data.list : [];
  return isModu ? rows.map((v) => ({ ...v, vod_id: `${MODU_ID_PREFIX}${v.vod_id}` })) : rows;
}

// 把待匹配的豆瓣标题分摊到各健康镜像上并行搜索，避免串行等满 400ms×N。
// 主源（魔都）的条目带着可播地址，优先在它这里找片；暴风只在魔都没命中时补位。
async function searchTitlesOnHosts(candidates, out) {
  const moduHosts = MODU_HOSTS.filter((h) => !hostCooling(h));
  const bfzyHosts = UPSTREAM_HOSTS.filter((h) => !hostCooling(h));
  const hosts = [...moduHosts.map((host) => ({ host, isModu: true })), ...bfzyHosts.map((host) => ({ host, isModu: false }))];
  if (!hosts.length) return;
  let cursor = 0;
  await Promise.all(hosts.map(async ({ host, isModu }) => {
    while (true) {
      const i = cursor;
      cursor += 1;
      if (i >= candidates.length) return;
      const c = candidates[i];
      if (out.has(c.key)) continue;
      const list = await bfzySearchOnHost(host, c.title, isModu).catch(() => null);
      if (!list) continue;
      const row = list.find((v) => titleKey(v && v.vod_name) === c.key) || null;
      if (row) out.set(c.key, { row, year: c.year });
    }
  }));
}

async function fetchDoubanCollection(id, count) {
  // 豆瓣接口单次最多返回 50 条，需要翻页才能拿满候选。
  const target = Math.max(1, Number(count) || 1);
  const items = [];
  for (let start = 0; start < target; start += 50) {
    const size = Math.min(50, target - start);
    const url = `${DOUBAN_API_BASE}/${id}/items?start=${start}&count=${size}`;
    const resp = await httpGetText(url, {
      timeout: UPSTREAM_TIMEOUT_MS,
      headers: {
        'User-Agent': DOUBAN_UA,
        Referer: DOUBAN_REFERER,
        Accept: 'application/json, text/plain, */*',
        'Accept-Encoding': 'identity',
      },
    });
    if (resp.status < 200 || resp.status >= 300) throw new Error(`douban collection failed: ${resp.status}`);
    let json;
    try { json = JSON.parse(resp.body.toString('utf8')); } catch { throw new Error('douban invalid json'); }
    const page = Array.isArray(json && json.subject_collection_items) ? json.subject_collection_items : [];
    items.push(...page);
    if (page.length < size) break;
  }
  return items
    .map((it) => ({
      title: normalizedTitle(it && it.title),
      year: doubanItemYear(it),
    }))
    .filter((it) => it.title);
}

// Top250 等集合不返回 year 字段，需要从 card_subtitle 的「1994 / 美国 / …」里补。
function doubanItemYear(item) {
  const direct = String((item && item.year) || '').trim();
  if (/^\d{4}$/.test(direct)) return direct;
  const match = String((item && item.card_subtitle) || '').match(/^(\d{4})\b/);
  return match ? match[1] : '';
}

function rankYearMatches(year, y) {
  if (!y) return true;
  const n = parseInt(String(year || ''), 10);
  if (!Number.isFinite(n)) return false;
  if (y === 'older') return n < 2020;
  return String(n) === String(y);
}

async function fetchDoubanRankPool(kind, y) {
  const ids = DOUBAN_COLLECTIONS[kind] || [];
  const candidates = [];
  const seen = new Set();
  for (const id of ids) {
    const list = await fetchDoubanCollection(id, DOUBAN_CANDIDATE_LIMIT).catch(() => []);
    for (const item of list) {
      if (!rankYearMatches(item.year, y)) continue;
      const key = titleKey(item.title);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      candidates.push({ title: item.title, key, year: item.year });
    }
  }
  if (!candidates.length) throw new Error('douban rank unavailable');

  // 命中本地库的候选直接复用；其余按片名分摊到多个镜像并行搜索。
  const matched = new Map();
  const pending = [];
  for (const c of candidates) {
    let row = null;
    try { row = await findLocalExactVod(c.title); } catch {}
    if (row) matched.set(c.key, { row, year: c.year });
    else pending.push(c);
  }
  await searchTitlesOnHosts(pending.slice(0, DOUBAN_RESOLVE_LIMIT), matched);

  const rows = [];
  const rowSeen = new Set();
  for (const c of candidates) {
    const entry = matched.get(c.key);
    if (!entry) continue;
    let row = entry.row;
    // 上游精简列表可能缺年份，用豆瓣年份补齐，保证卡片展示完整。
    if (!row.vod_year && entry.year) row = { ...row, vod_year: entry.year };
    if (!rankYearMatches(row.vod_year, y)) continue;
    const id = String(row.vod_id || '');
    if (!id || rowSeen.has(id)) continue;
    rowSeen.add(id);
    rows.push(row);
    if (rows.length >= RANK_POOL_LIMIT) break;
  }
  if (!rows.length) throw new Error('douban rank no playable match');
  return rows;
}

// 最新/最热/高分推荐：优先使用豆瓣榜单，失败时回退上游排序。
async function fetchRankedList(kind, pg, withPlay, year) {
  const page = parsePage(pg);
  const y = parseYear(year);
  try {
    // 快照必须是 { page, pagecount, total, list } 结构，才能被查询缓存正确存取。
    const cached = await catalogQuery(`rank:douban:v3:${kind}:${y || ''}`, TTL.list, async () => {
      const list = await fetchDoubanRankPool(kind, y);
      return { page: 1, pagecount: 1, total: list.length, list };
    });
    const pool = cached && Array.isArray(cached.list) ? cached.list : [];
    if (!pool.length) throw new Error('douban rank empty');
    const start = (page - 1) * PAGE_SIZE;
    const list = pool.slice(start, start + PAGE_SIZE).map((v) => ({ ...v }));
    if (!withPlay) await hydrate(list);
    return {
      page,
      pagecount: Math.max(1, Math.ceil(pool.length / PAGE_SIZE)),
      total: pool.length,
      list,
    };
  } catch (error) {
    console.error('[rank] douban fallback:', (error && error.message) || error);
    return fetchRankedListUpstream(kind, pg, withPlay, year);
  }
}

// 上游排序回退：上游不支持排序参数，取前几页本地排序后分页。
// 最新 = 上游默认 vod_time 倒序，直接透传分页；
// 最热 = 按 vod_hits 排序（该字段只在 ac=detail 返回）；高分 = 按 vod_score 排序。
async function fetchRankedListUpstream(kind, pg, withPlay, year) {
  const page = parsePage(pg);
  const y = parseYear(year);
  if (kind === 'new' && !y) {
    const data = await fetchModuList({ pg: page, t: '', year: '', withPlay });
    return {
      page,
      pagecount: Math.max(1, Number(data.pagecount) || 1),
      total: Number(data.total) || (data.list || []).length,
      list: data.list || [],
    };
  }
  const all = [];
  const seen = new Set();
  // 榜单回退要扫多页上游，串行等于把每页的排队间隔叠起来；并发着色但保持页序稳定。
  // 走魔都 detail 才有 vod_hits / vod_score，并顺手带上可播地址。
  const pageNos = Array.from({ length: RANK_SOURCE_PAGES }, (_, i) => i + 1);
  const pages = await Promise.all(pageNos.map((p) => {
    const params = { ac: 'detail', pg: p };
    if (y) params.year = y;
    return moduJson(params, TTL.list).catch(() => null);
  }));
  for (const data of pages) {
    if (!data) continue;
    for (const item of (Array.isArray(data.list) ? data.list : [])) {
      const key = String(item.vod_id || '');
      if (!key || seen.has(key)) continue;
      if (y && String(item.vod_year || '') !== y) continue;
      seen.add(key);
      all.push({ ...item, vod_id: `${MODU_ID_PREFIX}${item.vod_id}` });
    }
  }
  const metric = kind === 'hot'
    ? (v) => Number(v.vod_hits) || 0
    : kind === 'score'
      ? (v) => Number(v.vod_score) || 0
      : (v) => String(v.vod_time || '');
  all.sort((a, b) => {
    const av = metric(a);
    const bv = metric(b);
    return typeof av === 'string' ? String(bv).localeCompare(String(av)) : bv - av;
  });
  const start = (page - 1) * PAGE_SIZE;
  const list = all.slice(start, start + PAGE_SIZE);
  if (!withPlay) await hydrate(list);
  return {
    page,
    pagecount: Math.max(1, Math.ceil(all.length / PAGE_SIZE)),
    total: all.length,
    list,
  };
}

async function fetchList({ pg, t, year, withPlay }) {
  const cat = categoryParam(t);
  if (cat && cat.kind === 'headline' && RANK_KINDS.has(cat.value)) {
    const key = `rank:${cat.value}:v4:${withPlay ? 1 : 0}:${parsePage(pg)}:${PAGE_SIZE}:${parseYear(year)}`;
    return catalogQuery(key, TTL.list, () => fetchRankedList(cat.value, pg, withPlay, year));
  }
  return catalogQuery(listCacheKey({ pg, t, year, withPlay }), TTL.list, () => fetchListLive({ pg, t, year, withPlay }));
}

async function fetchDetail(ids) {
  const idList = String(ids).split(',').map((s) => s.trim()).filter(Boolean);
  const manualIds = idList.filter((id) => id.startsWith('manual-'));
  const upstreamIds = idList.filter((id) => !id.startsWith('manual-'));
  const rows = [];
  for (const id of manualIds) {
    const vod = await manualVodById(id);
    if (vod) rows.push(vod);
  }
  if (upstreamIds.length) {
    const key = `detail:${upstreamIds.join(',')}`;
    const data = await catalogQuery(key, TTL.detail, async () => {
      const list = await fetchDetailLive(upstreamIds.join(','));
      return { page: 1, pagecount: 1, total: list.length, list };
    });
    rows.push(...data.list);
  }
  return rows;
}

async function fetchSearch(wd, pg, withPlay) {
  const page = parsePage(pg);
  const title = normalizedTitle(wd).toLowerCase();
  const key = `search:v2:${withPlay ? 1 : 0}:${title}:${page}`;
  return catalogQuery(
    key,
    TTL.list,
    () => fetchSearchLive(wd, page, withPlay),
    () => STORES.catalogStore.searchVods(title, page, withPlay),
  );
}

// ---- 首页/栏目推荐：只采集标题，再匹配本站片源；播放仍走现有 4K/暴风源 ----
const KAN_API_BASE = 'https://api.web.360kan.com/v1';
const KAN_SECTIONS = {
  home: { title: '首页推荐', cat: null, rankCat: null, ourType: '' },
  movie: { title: '电影推荐', cat: '1', rankCat: 2, ourType: '1' },
  tv: { title: '电视剧推荐', cat: '2', rankCat: 3, ourType: '2' },
  variety: { title: '综艺推荐', cat: '3', rankCat: 4, ourType: '3' },
  anime: { title: '动漫推荐', cat: '4', rankCat: 5, ourType: '4' },
};
const KAN_CANDIDATE_LIMIT = 30;
const KAN_TITLE_CACHE = new Map();

async function fetchKanJson(url) {
  const resp = await httpGetText(url, {
    timeout: UPSTREAM_TIMEOUT_MS,
    headers: {
      'User-Agent': UA,
      Referer: 'https://www.360kan.com/',
      Accept: 'application/json,text/plain,*/*',
      'Accept-Encoding': 'identity',
    },
  });
  if (resp.status < 200 || resp.status >= 300) throw new Error(`recommend request failed: ${resp.status}`);
  let json;
  try { json = JSON.parse(resp.body.toString('utf8')); } catch { throw new Error('recommend invalid json'); }
  if (json && json.errno !== 0) throw new Error(`recommend error: ${json.msg || 'unknown'}`);
  return json;
}

async function fetchKanBlock(blockid, size) {
  const url = `${KAN_API_BASE}/block?blockid=${encodeURIComponent(blockid)}&size=${Math.max(1, Number(size) || KAN_CANDIDATE_LIMIT)}`;
  const json = await fetchKanJson(url);
  const list = json && json.data && Array.isArray(json.data.lists) ? json.data.lists : [];
  return list.filter((item) => item && item.title);
}

async function fetchKanRank(cat, size) {
  const url = `${KAN_API_BASE}/rank?cat=${encodeURIComponent(cat)}&size=${Math.max(1, Number(size) || KAN_CANDIDATE_LIMIT)}`;
  const json = await fetchKanJson(url);
  const list = Array.isArray(json && json.data) ? json.data : [];
  return list.filter((item) => item && item.title);
}

async function findLocalExactVod(title) {
  if (!STORES.catalogStore) return null;
  try {
    const data = await STORES.catalogStore.searchVods(title, 1, false);
    const list = Array.isArray(data && data.list) ? data.list : [];
    const key = titleKey(title);
    return list.find((v) => titleKey(v.vod_name) === key) || null;
  } catch (error) { catalogFailed(error); return null; }
}

async function resolveKanVod(item) {
  const title = normalizedTitle(item && item.title);
  const key = titleKey(title);
  if (!key) return null;
  if (KAN_TITLE_CACHE.has(key)) return KAN_TITLE_CACHE.get(key) || null;
  let row = await findLocalExactVod(title);
  if (!row) {
    try {
      const data = await fetchModuSearch(title, 1, false);
      const candidates = Array.isArray(data.list) ? data.list : [];
      row = candidates.find((v) => titleKey(v.vod_name) === key) || null;
    } catch {}
  }
  if (!row) {
    try {
      const data = await fetchBfzySearch(title, 1, false);
      const candidates = Array.isArray(data.list) ? data.list : [];
      row = candidates.find((v) => titleKey(v.vod_name) === key) || null;
    } catch {}
  }
  KAN_TITLE_CACHE.set(key, row || null);
  return row;
}

async function collectKanRecommendations(section) {
  const cfg = KAN_SECTIONS[section];
  if (!cfg) throw new Error('unknown kan section');
  const rows = section === 'home'
    ? await fetchKanBlock(522, KAN_CANDIDATE_LIMIT)
    : await fetchKanRank(cfg.rankCat, KAN_CANDIDATE_LIMIT);
  const seeds = rows.filter((item) => !cfg.cat || String(item.cat || '') === cfg.cat);
  const list = [];
  const seen = new Set();
  for (const item of seeds) {
    if (list.length >= 12) break;
    const row = await resolveKanVod(item);
    if (!row) continue;
    const id = String(row.vod_id);
    if (seen.has(id)) continue;
    seen.add(id);
    list.push(row);
  }
  if (!list.length) throw new Error('recommendation unavailable');
  return { page: 1, pagecount: 1, total: list.length, list, source: 'recommend' };
}

async function fetchKanRecommendations(section, limit) {
  const max = Math.max(1, Math.min(30, Number(limit) || 12));
  return catalogQuery(
    `kan:v2:${section}:${max}`,
    TTL.list,
    () => collectKanRecommendations(section).then((data) => ({ ...data, list: data.list.slice(0, max) })),
  );
}

// ---- 创意频道：数据库筛选优先，不足时上游采集并自动入库 ----
// 每个频道是一份「候选池快照」：去重排序后整池 saveQuery（所有候选自动 saveVod 入库），
// 分页只从快照切片，避免逐页回源造成重复/漂移。采集统一走暴风 MacCMS（含演员/评分/热度字段）。
const CHANNEL_POOL_LIMIT = 120;
const CHANNELS = [
  { id: 'guess', name: '猜你喜欢', icon: '💡', desc: '越看越懂你：从你最近点开的作品出发，挑出气质相近、值得继续追下去的片单。', kind: 'guess' },
  { id: 'score', name: '高分榜', icon: '🏆', desc: '口碑数字先说话：只收录有明确评分的作品，从高到低慢慢品味。', kind: 'score' },
  { id: 'praise', name: '口碑榜', icon: '👍', desc: '经得起反复打开的作品：筛掉低分噪音，按热度与认可度呈现口碑之选。', kind: 'praise' },
  { id: 'marvel', name: '漫威宇宙', icon: '🦸', desc: '超级英雄集结时刻：钢铁侠、美队、蜘蛛侠……一起重返漫威宇宙的热血与浪漫。', kind: 'series', keywords: ['复仇者联盟', '钢铁侠', '美国队长', '雷神', '蜘蛛侠', '奇异博士', '银河护卫队', '黑豹'] },
  { id: 'harrypotter', name: '哈利·波特', icon: '⚡', desc: '九又四分之三站台已开启：霍格沃茨、魔法部与神奇动物，陪你重返那个有求必应的世界。', kind: 'series', keywords: ['哈利波特', '哈利·波特', '神奇动物在哪里', '神奇动物：'] },
  { id: 'fastfurious', name: '速度与激情', icon: '🏎️', desc: '引擎轰鸣，家族不散：从街头飙车到全球冒险，一次补齐肾上腺素名场面。', kind: 'series', keywords: ['速度与激情'] },
  { id: 'detectivechinatown', name: '唐人街探案', icon: '🕵️', desc: '唐仁与秦风再度合体：搞笑探案、反转谜局，一路笑着追到真相。', kind: 'series', keywords: ['唐人街探案', '唐探'] },
  { id: 'star-cheng-long', name: '成龙频道', icon: '🥋', desc: '动作招牌，笑点也硬：成龙式搏命动作与功夫喜剧，一次看个痛快。', kind: 'star', actor: '成龙' },
  { id: 'star-zhou-xing-chi', name: '周星驰频道', icon: '😂', desc: '无厘头里藏着温柔：周星驰经典银幕时刻，笑过之后还想再刷。', kind: 'star', actor: '周星驰' },
  { id: 'star-wu-jing', name: '吴京频道', icon: '🎖️', desc: '硬汉气质拉满：拳拳到肉的动作场面与家国叙事，热血值直接拉满。', kind: 'star', actor: '吴京' },
  { id: 'star-shen-teng', name: '沈腾频道', icon: '🤣', desc: '笑点长在他身上：沈腾式喜感与名场面，适合随时来一集解压。', kind: 'star', actor: '沈腾' },
  { id: 'guoman', name: '国漫频道', icon: '🐉', desc: '国风想象力正在起飞：热血少年、东方神话与手绘美学，一起见证国漫高光。', kind: 'guoman' },
];
const CHANNEL_BY_ID = new Map(CHANNELS.map((c) => [c.id, c]));

function channelRule(ch) {
  if (ch.kind === 'series') return { kind: 'series', keywords: ch.keywords, limit: CHANNEL_POOL_LIMIT };
  if (ch.kind === 'star') return { kind: 'star', actors: [ch.actor], limit: CHANNEL_POOL_LIMIT };
  return { kind: ch.kind, limit: CHANNEL_POOL_LIMIT };
}

function channelScore(v) { const n = Number(v && v.vod_score); return Number.isFinite(n) ? n : 0; }
function channelHits(v) { const n = Number(v && v.vod_hits); return Number.isFinite(n) ? n : 0; }

function channelItemMatches(ch, v) {
  if (!v) return false;
  if (ch.kind === 'score') return channelScore(v) > 0;
  if (ch.kind === 'praise') return channelScore(v) >= 6;
  if (ch.kind === 'series') {
    const name = normalizedTitle(v.vod_name || '').toLowerCase();
    return (ch.keywords || []).some((k) => {
      const key = normalizedTitle(k).toLowerCase();
      return key && name.includes(key);
    });
  }
  if (ch.kind === 'star') return String(v.vod_actor || '').includes(ch.actor);
  if (ch.kind === 'guoman') {
    const type = String(v.type_id || '');
    const area = String(v.vod_area || '');
    return type === '40' || (['4', '41', '42', '43'].includes(type) && area.includes('大陆'));
  }
  return true;
}

function sortChannelPool(ch, all) {
  // 最终兜底校验：采集和库筛合并后，仍按频道主题规则过滤一次，避免出现随机/无关内容。
  const list = all.filter((v) => channelItemMatches(ch, v));
  const metric = ch.kind === 'praise' ? channelHits : channelScore;
  return list
    .sort((a, b) => metric(b) - metric(a) || String(a.vod_id).localeCompare(String(b.vod_id)))
    .slice(0, CHANNEL_POOL_LIMIT);
}

async function collectChannelPool(ch) {
  const seen = new Set();
  const all = [];
  const push = (items) => {
    for (const item of items || []) {
      const id = String(item && item.vod_id || '');
      if (!id || seen.has(id)) continue;
      seen.add(id);
      all.push(item);
    }
  };
  if (ch.kind === 'score' || ch.kind === 'praise') {
    const ac = ch.kind === 'praise' ? 'detail' : 'list';
    for (let p = 1; p <= RANK_SOURCE_PAGES; p += 1) {
      const data = await upstreamJson({ ac, pg: p }, TTL.list).catch(() => null);
      if (data) push(data.list);
    }
  } else if (ch.kind === 'guoman') {
    for (let p = 1; p <= 3; p += 1) {
      const data = await upstreamJson({ ac: 'detail', t: 40, pg: p }, TTL.list).catch(() => null);
      if (data) push(data.list);
    }
  } else if (ch.kind === 'series') {
    const keys = ch.keywords.slice(0, 8).map((k) => normalizedTitle(k).toLowerCase());
    for (const kw of ch.keywords.slice(0, 8)) {
      const data = await fetchBfzySearch(kw, 1, true).catch(() => null);
      // 上游搜索是模糊匹配，必须再按关键词过滤片名，与库内筛选口径一致。
      if (data) push((data.list || []).filter((v) => {
        const name = normalizedTitle(v.vod_name || '').toLowerCase();
        return keys.some((k) => k && name.includes(k));
      }));
    }
  } else if (ch.kind === 'star') {
    // wd 搜索仅用于缩小候选，必须再按演员字段严格过滤，标题命中不算数。
    for (let p = 1; p <= 2; p += 1) {
      const data = await fetchBfzySearch(ch.actor, p, true).catch(() => null);
      if (data) push((data.list || []).filter((v) => String(v.vod_actor || '').includes(ch.actor)));
    }
  }
  const list = sortChannelPool(ch, all);
  if (!list.length) throw new Error('频道片源暂不可用');
  return { page: 1, pagecount: 1, total: list.length, list };
}

async function fetchChannelPool(ch) {
  const key = `channel:${ch.id}:v1:${CHANNEL_POOL_LIMIT}`;
  return catalogQuery(
    key,
    TTL.list,
    async () => {
      const fresh = await collectChannelPool(ch);
      // 与库内已有候选合并：上游部分页失败导致采集变少时，快照不会因此缩水。
      if (STORES.catalogStore) {
        try {
          const local = await STORES.catalogStore.channelVods(channelRule(ch));
          const seen = new Set(fresh.list.map((v) => String(v.vod_id)));
          const merged = [...fresh.list];
          for (const v of local.list || []) {
            if (!seen.has(String(v.vod_id))) { seen.add(String(v.vod_id)); merged.push(v); }
          }
          const list = sortChannelPool(ch, merged);
          return { page: 1, pagecount: 1, total: list.length, list };
        } catch (error) { catalogFailed(error); }
      }
      return fresh;
    },
    STORES.catalogStore ? () => STORES.catalogStore.channelVods(channelRule(ch)) : undefined,
  );
}

function sliceChannelPool(pool, pg) {
  const page = parsePage(pg);
  const total = Number(pool.total) || (pool.list || []).length;
  const pagecount = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const start = (page - 1) * PAGE_SIZE;
  return { page, pagecount, total, list: (pool.list || []).slice(start, start + PAGE_SIZE) };
}

// 猜你喜欢：种子仅取库内已有详情派生演员/类型，不写共享缓存，无有效种子时回退口碑榜。
async function fetchGuessPool(seeds, exclude) {
  if (!STORES.catalogStore || !seeds.length) return null;
  let info = [];
  try { info = await STORES.catalogStore.seedsInfo(seeds); } catch (error) { catalogFailed(error); return null; }
  if (!info.length) return null;
  const actors = [...new Set(info
    .flatMap((r) => String(r.actor || '').split(/[,，/、]/).map((s) => s.trim()))
    .filter((s) => s && s.length <= 12 && !/^\d+$/.test(s)))].slice(0, 6);
  const typeIds = [...new Set(info.map((r) => String(r.type_id || '')).filter(Boolean))].slice(0, 8);
  if (!actors.length && !typeIds.length) return null;
  const pool = await STORES.catalogStore.channelVods({ kind: 'guess', actors, typeIds, exclude, limit: CHANNEL_POOL_LIMIT });
  return pool.list.length ? pool : null;
}

// Fetch a single detail record for playback resolution without crossing ID schemes.
// 先吃数据库里 12 小时有效的详情（含全集地址），命中就不回采集源。
async function detailForPlay(id) {
  const key = `detail:${id}`;
  // 数据库里的详情存的是对外 /play.m3u8 链接，拿不到真实媒体地址，
  // 播放解析仍回采集源；结果按 PLAY_URL_CACHE_TTL（30 分钟）留在实例内、边缘缓存 1 小时。
  const row = /^4kcz-\d{1,8}$/.test(id)
    ? await fetchFourkDetail(id)
    : MODU_ID_RE.test(id)
      ? (await fetchModuDetail([id])).find((item) => String(item.vod_id) === String(id)) || null
      : (await fetchBfzyDetail(id)).find((item) => String(item.vod_id) === String(id)) || null;
  // 采集到的详情同样回写数据库，供后续 ac=detail 直接命中。
  if (row) await saveCatalogQuery(key, { page: 1, pagecount: 1, total: 1, list: [row] }, TTL.detail);
  return row;
}

function normalizedTitle(value) {
  return String(value || '').normalize('NFKC').replace(/\s+/g, ' ').trim();
}

function titleKey(value) {
  return normalizedTitle(value)
    .toLowerCase()
    .replace(/[\s:：·,，.。!！?？'’"“”()（）\-_/\\|【】\[\]{}]+/g, '');
}

function normalizedEpisode(value) {
  return normalizedTitle(value).toLowerCase();
}

function episodeNumber(value) {
  const text = normalizedTitle(value);
  const match = text.match(/第\s*0*(\d+)\s*集/i) || text.match(/\bE?P\s*0*(\d+)\b/i);
  return match ? Number(match[1]) : null;
}

async function bfzyTitleCandidates(row) {
  const title = normalizedTitle(row && row.vod_name);
  if (!title) return [];
  const data = await upstreamJson({ ac: 'detail', wd: title }, 0);
  const seen = new Set();
  const titled = (Array.isArray(data.list) ? data.list : []).filter((candidate) => {
    const id = String(candidate.vod_id || '');
    if (!/^\d{1,10}$/.test(id) || seen.has(id) || normalizedTitle(candidate.vod_name) !== title) return false;
    seen.add(id);
    return true;
  });
  const sourceYear = String(row.vod_year || '').trim();
  if (!sourceYear) return titled.length === 1 ? titled : [];
  return titled.filter((candidate) => {
    const year = String(candidate.vod_year || '').trim();
    return year ? year === sourceYear : titled.length === 1;
  });
}

function mapBfzyEpisode(sourceRow, sourceEpisodes, sourceEp, sourceSeq, candidate) {
  const episodes = playableGroups(candidate.vod_play_url).flat();
  if (!episodes.length) return null;
  const bothMovies = mapTypeId(sourceRow.type_id) === '1' && mapTypeId(candidate.type_id) === '1';
  if (bothMovies && (sourceEpisodes.length !== 1 || episodes.length !== 1 || sourceSeq !== 0)) return null;
  let matches = episodes.filter((ep) => normalizedEpisode(ep.name) === normalizedEpisode(sourceEp.name));
  if (!matches.length) {
    const number = episodeNumber(sourceEp.name);
    if (number !== null) matches = episodes.filter((ep) => episodeNumber(ep.name) === number);
  }
  return matches.length === 1 ? matches[0] : null;
}

async function resolveBfzyFallback(row, sourceEpisodes, sourceEp, sourceSeq) {
  const candidates = await bfzyTitleCandidates(row);
  const matches = [];
  for (const candidate of candidates) {
    const ep = mapBfzyEpisode(row, sourceEpisodes, sourceEp, sourceSeq, candidate);
    if (ep) matches.push(ep);
  }
  if (matches.length !== 1) throw new Error('no unambiguous bfzy fallback');
  return new URL(matches[0].url);
}

async function resolveFourkMedia(ep) {
  let episodePath;
  try { episodePath = new URL(ep.url).pathname; } catch { throw new Error('bad 4K episode url'); }
  if (!/^\/v_play\/[A-Za-z0-9_=-]+\.html$/.test(episodePath)) throw new Error('bad 4K episode path');
  return withFourkHosts(`play:${episodePath}`, 0, async (baseHost, deadline) => {
    const html = await fourkFetch(baseHost, episodePath, deadline);
    const iframe = [...String(html).matchAll(/<iframe\b[^>]*>/gi)]
      .map((match) => match[0])
      .find((tag) => /(?:^|\s)viframe(?:\s|$)/.test(htmlAttr(tag, 'class')) && htmlAttr(tag, 'src'));
    if (!iframe) throw new Error('4K player unavailable');
    const player = new URL(htmlAttr(iframe, 'src'), `${baseHost}/`);
    if (!fourkPlayerAllowed(player)) throw new Error('4K player rejected');
    const response = await httpGetText(player.toString(), {
      headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml,*/*', 'Accept-Encoding': 'identity', Referer: `${baseHost}/` },
      deadline,
      followRedirects: true,
      redirectAllowed: fourkPlayerAllowed,
    });
    const playerHtml = response.body ? response.body.toString('utf8') : '';
    if (response.error || response.status < 200 || response.status >= 300) throw new Error('4K player request failed');
    const match = playerHtml.match(/\b(?:const|let|var)\s+mysvg\s*=\s*(['"])(https?:\/\/.*?)\1\s*;/)
      || playerHtml.match(/\bart\.url\s*=\s*(['"])(https?:\/\/.*?)\1\s*;/);
    if (!match) throw new Error('4K media unavailable');
    const media = new URL(match[2]);
    if (!fourkMediaAllowed(media)) throw new Error('4K media rejected');
    return media;
  });
}

// 播放地址短时内存缓存：同一集 5 分钟内复用已解析结果，避免重复回源采集（不入库，符合短期有效约束）。
const PLAY_URL_CACHE = new Map();
const PLAY_URL_CACHE_TTL = 30 * 60 * 1000;
const PLAY_URL_INFLIGHT = new Map();

async function resolvePlayback(id, seq, refresh = false) {
  const key = `${id}:${seq}`;
  if (!refresh) {
    const hit = PLAY_URL_CACHE.get(key);
    if (hit && Date.now() - hit.ts < PLAY_URL_CACHE_TTL) return hit.value;
    const pending = PLAY_URL_INFLIGHT.get(key);
    if (pending) return pending;
  }
  const task = (async () => {
    const row = await detailForPlay(id);
    const isFourk = id.startsWith('4kcz-');
    const episodes = row ? playableGroups(row.vod_play_url, isFourk).flat() : [];
    const ep = episodes[seq];
    if (!ep) {
      const error = new Error('episode not found');
      error.status = 404;
      throw error;
    }
    let media;
    if (!isFourk) {
      media = new URL(ep.url);
    } else {
      try {
        media = await resolveFourkMedia(ep);
      } catch {
        media = await resolveBfzyFallback(row, episodes, ep, seq);
      }
    }
    const value = { media, name: ep.name };
    PLAY_URL_CACHE.set(key, { value, ts: Date.now() });
    if (PLAY_URL_CACHE.size > 2000) {
      for (const [k, v] of PLAY_URL_CACHE) {
        if (Date.now() - v.ts >= PLAY_URL_CACHE_TTL) PLAY_URL_CACHE.delete(k);
      }
    }
    return value;
  })();
  PLAY_URL_INFLIGHT.set(key, task);
  try {
    return await task;
  } finally {
    PLAY_URL_INFLIGHT.delete(key);
  }
}

function serveFile(res, filePath, versioned = false) {
  if (isWorkers) {
    // Workers 下静态文件由 worker.mjs 的 Assets 绑定直接返回，不应走到这里。
    sendJson(res, 404, { ok: false, error: 'not found' });
    return;
  }
  import('node:fs').then(({ default: fs }) => {
    fs.readFile(filePath, (err, data) => {
      if (err) { sendJson(res, 404, { ok: false, error: 'not found' }); return; }
      const ext = path.extname(filePath).toLowerCase();
      let cache;
      if (ext === '.html') cache = 'no-cache'; // 入口 HTML 始终轻量校验，保证新版本即时生效
      else if (versioned) cache = 'public, max-age=31536000, immutable'; // 带 ?v= 的版本化资源由边缘 CDN 长期缓存
      else cache = 'public, max-age=86400';
      writeBody(res, 200, {
        'Content-Type': MIME[ext] || 'application/octet-stream',
        'Cache-Control': cache,
      }, data);
    });
  }).catch(() => sendJson(res, 500, { ok: false, error: 'static unavailable' }));
}

async function fetchImage(target) {
  return httpGetText(target, {
    headers: { 'User-Agent': UA, Accept: 'image/*,*/*', 'Accept-Encoding': 'identity' },
  });
}

const M3U8_AD_RULES = [
  { hostname: 'v.fengbao14.com', segmentPath: '/video/adjump/' },
];

function m3u8AdRule(url) {
  const hostname = url.hostname.toLowerCase();
  return M3U8_AD_RULES.find((rule) => rule.hostname === hostname) || null;
}

function absolutePlaylistUri(uri, base) {
  try { return new URL(uri, base).toString(); } catch { return uri; }
}

// Remove only provider-tagged ad blocks. Segment URIs must become absolute
// because the filtered playlist is served from our own origin.
function filterM3u8(body, media, rule) {
  const output = [];
  let pending = [];
  let removedAd = false;
  for (const raw of body.split(/\r?\n/)) {
    const trimmed = raw.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const resolved = new URL(trimmed, media);
      if (resolved.hostname.toLowerCase() === rule.hostname && resolved.pathname.startsWith(rule.segmentPath)) {
        pending = [];
        removedAd = true;
        continue;
      }
      if (removedAd) pending = pending.filter((line) => line.trim() !== '#EXT-X-DISCONTINUITY');
      output.push(...pending, resolved.toString());
      pending = [];
      removedAd = false;
      continue;
    }
    pending.push(raw.replace(/URI="([^"]*)"/g, (_match, uri) => `URI="${absolutePlaylistUri(uri, media)}"`));
  }
  output.push(...pending);
  return output.join('\n');
}

// ---- MacCMS-compatible payloads ----
function listPayload(data, base, withPlay) {
  return {
    code: 1,
    msg: '数据列表',
    page: data.page,
    pagecount: data.pagecount,
    limit: PAGE_SIZE,
    total: data.total,
    list: data.list.map((v) => normalizeItem(v, base, withPlay)).filter(Boolean),
    class: CLASSES,
  };
}

// ---- 屏蔽词与手动影片（进程内缓存 30 秒，后台修改后立即失效） ----
let insightCache = { at: 0, words: [], manual: [] };

function resetInsights() { insightCache = { at: 0, words: [], manual: [] }; }

async function insights() {
  if (!STORES.adminStore) return { words: [], manual: [] };
  if (Date.now() - insightCache.at < 30000) return insightCache;
  try {
    const settings = await STORES.adminStore.getSettings();
    const words = String(settings.blocked_words || '')
      .split(/[\n,，]/)
      .map((w) => w.trim().toLowerCase())
      .filter(Boolean)
      .slice(0, 500);
    const manual = await STORES.adminStore.listManualVods();
    insightCache = { at: Date.now(), words, manual };
  } catch (error) {
    console.warn('[quying] insight load failed:', error.message);
  }
  return insightCache;
}

function blockedName(name, words) {
  if (!words || !words.length) return false;
  const key = normalizedTitle(name).toLowerCase();
  return words.some((w) => key.includes(w));
}

function manualMatchType(vod, t) {
  const cat = categoryParam(t);
  if (!cat) return true;
  const tid = Number(vod.type_id);
  if (cat.kind === 'headline') return (PROVIDER_TYPES[cat.value]?.children || []).includes(tid);
  return tid === Number(cat.value);
}

function manualEpisodes(raw) {
  const lines = String(raw || '').split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  return lines.map((line, index) => {
    const cut = line.indexOf('|');
    const name = cut > 0 ? line.slice(0, cut).trim() : `第${index + 1}集`;
    const url = (cut > 0 ? line.slice(cut + 1) : line).trim();
    return `${name}$${url}`;
  }).join('#');
}

async function manualVodById(id) {
  const { manual } = await insights();
  return manual.find((v) => String(v.vod_id) === String(id)) || null;
}

// 列表/搜索统一出口：过滤屏蔽词，并在第一页混入手动影片。
async function publicListPayload(data, base, withPlay, opts = {}) {
  const { words, manual } = await insights();
  const page = Number(data.page) || 1;
  let list = data.list.filter((v) => !blockedName(v.vod_name, words));
  if (page === 1 && manual.length && opts.manual !== false) {
    const wdKey = opts.wd ? normalizedTitle(opts.wd).toLowerCase() : '';
    const extra = manual
      .filter((v) => !blockedName(v.vod_name, words))
      .filter((v) => !wdKey || normalizedTitle(v.vod_name).toLowerCase().includes(wdKey))
      .filter((v) => opts.wd || manualMatchType(v, opts.t))
      .filter((v) => !opts.year || String(v.vod_year || '') === String(opts.year));
    const seen = new Set(list.map((v) => String(v.vod_id)));
    list = [...extra.filter((v) => !seen.has(String(v.vod_id))), ...list];
  }
  return {
    code: 1,
    msg: '数据列表',
    page,
    pagecount: data.pagecount,
    limit: PAGE_SIZE,
    total: Math.max(Number(data.total) || 0, list.length),
    list: list.map((v) => normalizeItem(v, base, withPlay)).filter(Boolean),
    class: CLASSES,
  };
}

async function publicDetailPayload(rows, base, withPlay) {
  const { words } = await insights();
  const enriched = withPlay
    ? await Promise.all(rows.map((row, index) => (index < 3 ? withPlaySources(row, base).catch(() => row) : row)))
    : rows;
  const list = enriched.filter((v) => !blockedName(v.vod_name, words));
  return {
    code: 1,
    msg: '数据列表',
    page: 1,
    pagecount: 1,
    limit: PAGE_SIZE,
    total: list.length,
    list: list.map((v) => normalizeItem(v, base, withPlay)).filter(Boolean),
    class: CLASSES,
  };
}

// ---- 手动影片与采集任务 ----
const COLLECT_CATS = ['', '1', '2', '3', '4', '5', '6', '7'];
const COLLECT_MAX_PAGES = 6;

function manualError(message) {
  return Object.assign(new Error(message), { status: 400 });
}

function normalizeManualVod(input = {}) {
  const name = String(input.vod_name || '').trim().slice(0, 120);
  if (!name) throw manualError('影片名称必填');
  const typeId = Number(input.type_id);
  if (!Number.isInteger(typeId) || !TYPE_NAMES[typeId]) throw manualError('分类无效');
  const pic = String(input.vod_pic || '').trim().slice(0, 800);
  if (pic && !/^https?:\/\//i.test(pic)) throw manualError('海报必须是 http/https 地址');
  const playUrl = manualEpisodes(input.vod_play_url);
  if (!playUrl) throw manualError('至少填写一集播放地址');
  const urls = playUrl.split('#').map((ep) => ep.split('$')[1] || '');
  if (urls.some((u) => !/^https?:\/\//i.test(u))) throw manualError('播放地址必须是 http/https 链接');
  const id = /^manual-[a-z0-9]{4,24}$/.test(String(input.vod_id || ''))
    ? String(input.vod_id)
    : `manual-${Date.now().toString(36)}${crypto.randomBytes(3).toString('hex')}`;
  return {
    vod_id: id,
    vod_name: name,
    type_id: String(typeId),
    type_name: TYPE_NAMES[typeId] || '',
    vod_year: String(input.vod_year || '').trim().slice(0, 10),
    vod_area: String(input.vod_area || '').trim().slice(0, 30),
    vod_remarks: String(input.vod_remarks || '').trim().slice(0, 60),
    vod_actor: String(input.vod_actor || '').trim().slice(0, 200),
    vod_director: String(input.vod_director || '').trim().slice(0, 120),
    vod_content: String(input.vod_content || '').trim().slice(0, 2000),
    vod_pic: pic,
    vod_play_from: '手动线路',
    vod_play_url: playUrl,
    manual: true,
    updated_at: Date.now(),
  };
}

function parseCollectCursor(value) {
  try {
    const parsed = JSON.parse(String(value || ''));
    const ci = Number(parsed.ci);
    const page = Number(parsed.page);
    if (Number.isInteger(ci) && ci >= 0 && ci < COLLECT_CATS.length && Number.isInteger(page) && page >= 1) {
      return { ci, page };
    }
  } catch { /* 使用默认游标 */ }
  return { ci: 0, page: 1 };
}

// 分批采集：每次最多处理若干页，避免撞上 Serverless 执行时长上限。
async function collectBatch({ pages = 3, force = false } = {}) {
  const settings = await STORES.adminStore.getSettings();
  let cursor = parseCollectCursor(settings.collect_cursor);
  const limit = Math.min(Math.max(Number(pages) || 3, 1), COLLECT_MAX_PAGES);
  const started = Date.now();
  let processed = 0;
  let skipped = 0;
  let cycleDone = false;
  for (let i = 0; i < limit; i += 1) {
    if (i > 0 && Date.now() - started > 45000) break;
    const t = COLLECT_CATS[cursor.ci];
    const key = listCacheKey({ pg: cursor.page, t, year: '' });
    const cached = await STORES.catalogStore.getQuery(key);
    let pagecount = cached && cached.value ? Number(cached.value.pagecount) || 1 : 1;
    if (cached && !cached.stale && !force) {
      skipped += 1;
    } else {
      const data = await fetchList({ pg: cursor.page, t, year: '' });
      pagecount = Number(data.pagecount) || 1;
      processed += 1;
    }
    if (cursor.page >= pagecount) {
      cursor = { ci: (cursor.ci + 1) % COLLECT_CATS.length, page: 1 };
      if (cursor.ci === 0) cycleDone = true;
    } else {
      cursor = { ci: cursor.ci, page: cursor.page + 1 };
    }
  }
  const total = (Number(settings.collect_total) || 0) + processed;
  await STORES.adminStore.updateSettings({
    collect_cursor: JSON.stringify(cursor),
    collect_last_run: String(Date.now()),
    collect_total: String(total),
  });
  return { processed, skipped, cursor, cycleDone, total };
}

function tvboxConfig(base, code) {
  // 授权模式下把卡密注入 api 地址，TVBox 后续 list/detail/play 请求自动透传。
  const api = code
    ? `${base}/api.php/provide/vod?code=${encodeURIComponent(code)}`
    : `${base}/api.php/provide/vod`;
  return {
    name: '趣影',
    sites: [{
      key: 'quying',
      name: '趣影',
      type: 1,
      api,
      searchable: 1,
      quickSearch: 1,
      filterable: 1,
    }],
    lives: [{
      name: '趣影直播',
      type: 0,
      url: `${base}/live.m3u${code ? `?code=${encodeURIComponent(code)}` : ''}`,
      epg: LIVE_EPG_URL,
    }],
    parses: [],
    flags: [],
  };
}

// ---- 后台：公开站点配置、广告与鉴权 ----
async function sitePayload() {
  const settings = STORES.adminStore ? await STORES.adminStore.getSettings() : {};
  return {
    ok: true,
    name: settings.site_name || '趣影',
    subtitle: settings.site_subtitle || '高清影视 · 即搜即看',
    notice: settings.site_notice || '',
    footer: settings.footer_text || '',
    adsEnabled: (settings.ads_enabled == null ? '1' : String(settings.ads_enabled)) !== '0',
    carouselInterval: Math.max(2000, Math.min(15000, Number(settings.carousel_interval) || 5000)),
    requireCard: REQUIRE_CARD,
  };
}

function parseCookies(req) {
  const out = {};
  for (const part of String(req.headers.cookie || '').split(';')) {
    const idx = part.indexOf('=');
    if (idx < 0) continue;
    out[part.slice(0, idx).trim()] = decodeURIComponent(part.slice(idx + 1).trim());
  }
  return out;
}

// 从请求中提取授权卡密：query ?code= → X-Auth-Code 头 → qy_card cookie。
// TVBox 配置地址带 ?code=XXX，后续请求会透传；Web 前台用 cookie。
function extractCardCode(req, url) {
  const q = url.searchParams.get('code');
  if (q) return q.trim();
  const h = req.headers['x-auth-code'];
  if (h) return String(h).trim();
  const c = parseCookies(req).qy_card;
  if (c) return c.trim();
  return '';
}

// 卡密强制授权中间件。返回 true 放行，false 表示已写入 401/503 响应，调用方应直接 return。
async function enforceCardAuth(req, res, p, url) {
  if (!REQUIRE_CARD || !AUTH_PROTECTED_PATHS.has(p)) return true;
  if (!STORES.adminStore) {
    sendJson(res, 503, { ok: false, error: '授权未启用：数据库不可用' });
    return false;
  }
  const result = await STORES.adminStore.authorizeCard(extractCardCode(req, url));
  if (!result.valid) {
    sendJson(res, 401, { ok: false, error: result.reason || '需要有效授权卡密', code: 401 });
    return false;
  }
  return true;
}

async function adminToken(req) {
  const token = parseCookies(req).qy_admin;
  return STORES.adminStore && await STORES.adminStore.validateSession(token) ? token : '';
}

function readJsonBody(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) { reject(new Error('请求体过大')); req.destroy(); return; }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { reject(new Error('JSON 格式错误')); }
    });
    req.on('error', reject);
  });
}

function sameOrigin(req) {
  const origin = req.headers.origin;
  // 浏览器同源请求（含 fetch 的 PUT/POST）一定带 Origin，缺失即视为不可信
  if (!origin) return false;
  try { return new URL(origin).host === String(req.headers.host || ''); } catch { return false; }
}

function csvCell(value) {
  return `"${String(value == null ? '' : value).replace(/"/g, '""')}"`;
}

async function handleAdmin(req, res, p, url) {
  if (!STORES.adminStore) {
    sendJson(res, 503, { ok: false, error: '后台未启用：数据库不可用' });
    return;
  }
  const method = req.method;

  if (p === '/api/admin/login' && method === 'POST') {
    let body;
    try { body = await readJsonBody(req); } catch (e) { sendJson(res, 400, { ok: false, error: e.message }); return; }
    if (!await STORES.adminStore.verifyPassword(body.password || '')) {
      sendJson(res, 401, { ok: false, error: '密码错误' });
      return;
    }
    const token = await STORES.adminStore.createSession();
    res.setHeader('Set-Cookie', `qy_admin=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${7 * 24 * 3600}`);
    sendJson(res, 200, { ok: true });
    return;
  }

  if (p === '/api/admin/session' && method === 'GET') {
    sendJson(res, 200, { ok: true, authed: !!(await adminToken(req)) });
    return;
  }

  // 采集接口：允许后台会话，或用采集密钥（供 GitHub Actions 定时调用）
  if (p === '/api/admin/collect' && (method === 'POST' || method === 'GET')) {
    let body = {};
    if (method === 'POST') {
      try { body = await readJsonBody(req); } catch { body = {}; }
    }
    if (!await adminToken(req)) {
      const settings = await STORES.adminStore.getSettings();
      const expected = String(settings.collect_token || '');
      const given = String(body.token || url.searchParams.get('token') || '');
      if (!expected || given !== expected) {
        sendJson(res, 401, { ok: false, error: '采集密钥无效' });
        return;
      }
    }
    const result = await collectBatch({
      pages: body.pages || url.searchParams.get('pages'),
      force: body.force === true || url.searchParams.get('force') === '1',
    });
    sendJson(res, 200, { ok: true, ...result });
    return;
  }

  const token = await adminToken(req);
  if (!token) {
    sendJson(res, 401, { ok: false, error: '请先登录' });
    return;
  }
  if (method !== 'GET' && !sameOrigin(req)) {
    sendJson(res, 403, { ok: false, error: '跨站请求被拒绝' });
    return;
  }

  try {
    if (p === '/api/admin/logout' && method === 'POST') {
      await STORES.adminStore.destroySession(token);
      res.setHeader('Set-Cookie', 'qy_admin=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0');
      sendJson(res, 200, { ok: true });
      return;
    }

    if (p === '/api/admin/overview' && method === 'GET') {
      sendJson(res, 200, { ok: true, data: await STORES.adminStore.overview() });
      return;
    }

    if (p === '/api/admin/settings' && method === 'GET') {
      sendJson(res, 200, { ok: true, data: await STORES.adminStore.getSettings() });
      return;
    }
    if (p === '/api/admin/categories' && method === 'GET') {
      sendJson(res, 200, { ok: true, list: CLASSES });
      return;
    }
    if (p === '/api/admin/settings' && method === 'PUT') {
      const body = await readJsonBody(req);
      const data = await STORES.adminStore.updateSettings(body);
      resetInsights();
      sendJson(res, 200, { ok: true, data });
      return;
    }

    // ---- 手动影片 ----
    if (p === '/api/admin/videos' && method === 'GET') {
      sendJson(res, 200, { ok: true, list: await STORES.adminStore.listManualVods() });
      return;
    }
    if (p === '/api/admin/videos' && method === 'POST') {
      const body = await readJsonBody(req);
      const vod = normalizeManualVod(body);
      await STORES.adminStore.saveManualVod(vod);
      resetInsights();
      sendJson(res, 200, { ok: true, data: vod });
      return;
    }
    const videoMatch = p.match(/^\/api\/admin\/videos\/(manual-[a-z0-9]{4,24})$/);
    if (videoMatch && method === 'DELETE') {
      await STORES.adminStore.deleteManualVod(videoMatch[1]);
      resetInsights();
      sendJson(res, 200, { ok: true, deleted: true });
      return;
    }

    // ---- 采集状态与密钥 ----
    if (p === '/api/admin/collect/status' && method === 'GET') {
      const settings = await STORES.adminStore.getSettings();
      sendJson(res, 200, {
        ok: true,
        data: {
          cursor: parseCollectCursor(settings.collect_cursor),
          token: String(settings.collect_token || ''),
          total: Number(settings.collect_total) || 0,
          lastRun: Number(settings.collect_last_run) || 0,
          categories: COLLECT_CATS.length,
        },
      });
      return;
    }
    if (p === '/api/admin/collect/token' && method === 'POST') {
      const tokenValue = crypto.randomBytes(16).toString('hex');
      await STORES.adminStore.updateSettings({ collect_token: tokenValue });
      sendJson(res, 200, { ok: true, token: tokenValue });
      return;
    }

    if (p === '/api/admin/password' && method === 'POST') {
      const body = await readJsonBody(req);
      if (!await STORES.adminStore.verifyPassword(body.oldPassword || '')) {
        sendJson(res, 400, { ok: false, error: '原密码错误' });
        return;
      }
      const next = String(body.newPassword || '');
      if (next.length < 6) { sendJson(res, 400, { ok: false, error: '新密码至少 6 位' }); return; }
      await STORES.adminStore.setPassword(next);
      await STORES.adminStore.destroySession(token);
      res.setHeader('Set-Cookie', 'qy_admin=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0');
      sendJson(res, 200, { ok: true, relogin: true });
      return;
    }

    if (p === '/api/admin/ads' && method === 'GET') {
      sendJson(res, 200, { ok: true, list: await STORES.adminStore.listAds(url.searchParams.get('slot')) });
      return;
    }
    if (p === '/api/admin/ads' && method === 'POST') {
      const body = await readJsonBody(req);
      sendJson(res, 200, { ok: true, data: await STORES.adminStore.createAd(body) });
      return;
    }
    const adMatch = p.match(/^\/api\/admin\/ads\/(\d+)$/);
    if (adMatch && method === 'PUT') {
      const body = await readJsonBody(req);
      sendJson(res, 200, { ok: true, data: await STORES.adminStore.updateAd(adMatch[1], body) });
      return;
    }
    if (adMatch && method === 'DELETE') {
      sendJson(res, 200, { ok: true, deleted: await STORES.adminStore.deleteAd(adMatch[1]) });
      return;
    }

    if (p === '/api/admin/cards' && method === 'GET') {
      sendJson(res, 200, {
        ok: true,
        data: await STORES.adminStore.listCards({
          status: url.searchParams.get('status') || '',
          q: url.searchParams.get('q') || '',
          page: url.searchParams.get('page'),
          pageSize: url.searchParams.get('pageSize'),
        }),
      });
      return;
    }
    if (p === '/api/admin/cards/generate' && method === 'POST') {
      const body = await readJsonBody(req);
      const codes = await STORES.adminStore.generateCards(body);
      sendJson(res, 200, { ok: true, codes });
      return;
    }
    if (p === '/api/admin/cards/export' && method === 'GET') {
      const rows = ['卡密,天数,状态,备注,创建时间,使用时间'];
      for (const card of await STORES.adminStore.exportCards()) {
        rows.push([
          card.code, Number(card.days) ? card.days : '永久', card.status, card.note,
          card.createdAt ? new Date(card.createdAt).toISOString() : '',
          card.usedAt ? new Date(card.usedAt).toISOString() : '',
        ].map(csvCell).join(','));
      }
      const body = Buffer.from('\uFEFF' + rows.join('\r\n'), 'utf8');
      writeBody(res, 200, {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': 'attachment; filename="card-keys.csv"',
        'Cache-Control': 'no-store',
      }, body);
      return;
    }
    const cardMatch = p.match(/^\/api\/admin\/cards\/(\d+)$/);
    if (cardMatch && method === 'PATCH') {
      const body = await readJsonBody(req);
      sendJson(res, 200, { ok: true, updated: await STORES.adminStore.setCardStatus(cardMatch[1], body.status) });
      return;
    }
    if (cardMatch && method === 'DELETE') {
      sendJson(res, 200, { ok: true, deleted: await STORES.adminStore.deleteCard(cardMatch[1]) });
      return;
    }

    sendJson(res, 404, { ok: false, error: '后台接口不存在' });
  } catch (error) {
    sendJson(res, 400, { ok: false, error: String((error && error.message) || error) });
  }
}

// 请求处理主体：既供 Node http.createServer 使用，也由 requestHandler 在 Workers 中调用。
async function handleRequest(req, res) {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    res.end();
    return;
  }

  let url;
  try {
    url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch {
    sendJson(res, 400, { ok: false, error: 'bad request' });
    return;
  }
  const p = url.pathname.length > 1 ? url.pathname.replace(/\/+$/, '') : url.pathname;
  const base = publicBase(req);

  // 后台接口：自身处理登录态与请求方法
  if (p === '/api/admin' || p.startsWith('/api/admin/')) {
    await handleAdmin(req, res, p, url);
    return;
  }

  if (req.method !== 'GET' && !(req.method === 'POST' && (p === '/api/vod/channel' || p === '/api/live/refresh'))) {
    sendJson(res, 405, { ok: false, error: 'method not allowed' });
    return;
  }

  if (p === '/api/health') {
    sendJsonCacheable(res, 200, {
      ok: true,
      ts: Date.now(),
      store: storeBackend,
      storeError: storeInitError.slice(0, 160),
      dbProbe: await (async () => {
        if (!STORES.catalogStore) return 'none';
        try {
          const q = await STORES.catalogStore.getQuery(listCacheKey({ pg: 1 }));
          return q ? `rows=${q.value.list.length} stale=${q.stale}` : 'miss';
        } catch (error) { return `err:${String((error && error.message) || error).slice(0, 80)}`; }
      })(),
      hasDbEnv: Boolean(process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.POSTGRES_URL_NON_POOLING || (process.env.PGHOST && process.env.PGUSER)),
      cacheTtl: { list: TTL.list, detail: TTL.detail, play: PLAY_URL_CACHE_TTL },
    }, { sMaxAge: 10, maxAge: 5 });
    return;
  }

  // 前台公开配置与广告位
  if (p === '/api/site') {
    sendJsonCacheable(res, 200, await sitePayload(), { sMaxAge: 300 });
    return;
  }
  if (p === '/api/ads') {
    const slot = url.searchParams.get('slot') === 'nav' ? 'nav' : 'carousel';
    sendJsonCacheable(res, 200, { ok: true, list: STORES.adminStore ? await STORES.adminStore.publicAds(slot) : [] }, { sMaxAge: 300 });
    return;
  }
  if (p === '/api/card/verify') {
    const code = (url.searchParams.get('code') || '').trim();
    if (!code) { sendJson(res, 400, { ok: false, error: 'code is required' }); return; }
    sendJsonCacheable(res, 200, { ok: true, ...(STORES.adminStore ? await STORES.adminStore.verifyCard(code) : { valid: false, reason: '后台未启用' }) }, { sMaxAge: 60, maxAge: 10 });
    return;
  }

  // ---- 卡密强制授权：受保护路径（列表/详情/搜索/TVBox/播放）需携带有效卡密 ----
  if (!(await enforceCardAuth(req, res, p, url))) return;

  // ---- 直播：多源 M3U 采集、同名合并与分页查询，播放地址由客户端直连 ----
  if (p === '/api/live/refresh') {
    if (req.method !== 'POST') { sendJson(res, 405, { ok: false, error: 'method not allowed' }); return; }
    if (!STORES.adminStore) { sendJson(res, 503, { ok: false, error: '后台未启用：数据库不可用' }); return; }
    if (!await adminToken(req)) { sendJson(res, 401, { ok: false, error: '请先登录' }); return; }
    if (!sameOrigin(req)) { sendJson(res, 403, { ok: false, error: '跨站请求被拒绝' }); return; }
    try {
      const snapshot = await liveCollector.getSnapshot({ force: true });
      sendJson(res, 200, { ok: true, updatedAt: snapshot.updatedAt, total: snapshot.channels.length, groups: snapshot.groups, sources: snapshot.sources });
    } catch (error) {
      sendJson(res, 502, { ok: false, error: String((error && error.message) || error) });
    }
    return;
  }

  if (p === '/api/live/groups' || p === '/api/live/channels' || p === '/live.m3u') {
    let snapshot;
    try {
      snapshot = await liveSnapshotOrNull();
    } catch (error) {
      sendJson(res, 502, { ok: false, error: String((error && error.message) || error) });
      return;
    }
    // 冷启动首次采集未完成：立刻回占位（no-store，避免 CDN 缓存空结果），采集在后台继续，客户端稍后重试。
    if (!snapshot) {
      if (p === '/api/live/groups') {
        res.setHeader('Retry-After', '10');
        sendJson(res, 200, { ok: true, collecting: true, total: 0, updatedAt: null, stale: true, groups: [], sources: [] });
        return;
      }
      if (p === '/api/live/channels') {
        const size = Math.max(1, Math.min(100, Number.parseInt(url.searchParams.get('size'), 10) || 60));
        res.setHeader('Retry-After', '10');
        sendJson(res, 200, { ok: true, collecting: true, list: [], total: 0, page: 1, pagecount: 1, size, updatedAt: null, stale: true });
        return;
      }
      writeBody(res, 200, {
        'Content-Type': 'application/x-mpegURL; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
        'Retry-After': '10',
      }, Buffer.from('#EXTM3U\n'));
      return;
    }
    if (p === '/api/live/groups') {
      // 直播快照会随采集刷新变化，必须实时：禁用边缘/浏览器缓存，避免前端拿到上一版分组数字。
      sendJson(res, 200, {
        ok: true,
        total: snapshot.channels.length,
        updatedAt: snapshot.updatedAt,
        stale: snapshot.stale,
        groups: snapshot.groups,
        sources: snapshot.sources,
      });
      return;
    }
    if (p === '/api/live/channels') {
      const group = String(url.searchParams.get('group') || '').trim();
      if (group && !LIVE_GROUP_NAMES.includes(group)) { sendJson(res, 400, { ok: false, error: 'unknown live group' }); return; }
      const result = queryLiveChannels(snapshot.channels, {
        group,
        wd: String(url.searchParams.get('wd') || '').trim().slice(0, 60),
        page: url.searchParams.get('pg'),
        size: url.searchParams.get('size'),
      });
      sendJson(res, 200, { ok: true, ...result, updatedAt: snapshot.updatedAt, stale: snapshot.stale });
      return;
    }
    const body = Buffer.from(buildLiveM3U(snapshot.channels));
    const edgeAge = REQUIRE_CARD ? 60 : 3600;
    writeBody(res, 200, {
      'Content-Type': 'application/x-mpegURL; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': `public, max-age=60, s-maxage=${edgeAge}, stale-while-revalidate=300`,
      'Cloudflare-CDN-Cache-Control': `max-age=${edgeAge}, stale-while-revalidate=300`,
    }, body);
    return;
  }

  // 后台页面
  if (p === '/admin' || p === '/admin/') {
    serveFile(res, path.join(PUBLIC_DIR, 'admin.html'));
    return;
  }

  // Poster proxy: only known poster CDN hosts are fetched server-side.
  if (p === '/img') {
    const raw = url.searchParams.get('url') || '';
    let parsed;
    try { parsed = new URL(raw); } catch { sendJson(res, 400, { ok: false, error: 'bad url' }); return; }
    if ((parsed.protocol !== 'https:' && parsed.protocol !== 'http:') || !imageHostAllowed(parsed.hostname)) {
      sendJson(res, 403, { ok: false, error: 'host not allowed' });
      return;
    }
    const r = await fetchImage(parsed.toString());
    const ct = (r.headers && r.headers['content-type']) || '';
    if (r.error || r.status < 200 || r.status >= 300 || !ct.startsWith('image/')) {
      sendJson(res, 404, { ok: false, error: 'image unavailable' });
      return;
    }
    res.writeHead(200, {
      'Content-Type': ct,
      'Content-Length': r.body.length,
      'Cache-Control': 'public, max-age=86400',
    });
    res.end(r.body);
    return;
  }

  // Live-stream proxy: serve HTTP live m3u8 under our origin so HTTPS pages can play it.
  if (p === '/live-proxy.m3u8' || p === '/live-seg') {
    if (req.method !== 'GET') { sendJson(res, 405, { ok: false, error: 'method not allowed' }); return; }
    const raw = url.searchParams.get('url') || '';
    if (!liveUrlAllowed(raw)) { sendJson(res, 403, { ok: false, error: 'forbidden host' }); return; }
    if (p === '/live-proxy.m3u8') {
      const target = new URL(raw);
      const r = await httpGetText(target.toString(), {
        headers: { 'User-Agent': UA, Accept: 'application/x-mpegURL,*/*' },
        timeout: LIVE_FETCH_TIMEOUT_MS,
        deadline: Date.now() + LIVE_FETCH_TIMEOUT_MS,
        followRedirects: true,
      });
      if (r.error || r.status < 200 || r.status >= 300) { sendJson(res, 502, { ok: false, error: 'manifest unavailable' }); return; }
      let body = (r.body || Buffer.alloc(0)).toString('utf8');
      if (!body.replace(/^\uFEFF/, '').trimStart().startsWith('#EXTM3U')) { sendJson(res, 400, { ok: false, error: 'not an m3u8' }); return; }
      body = rewriteM3u8Segments(body, base, target.toString());
      writeBody(res, 200, {
        'Content-Type': 'application/x-mpegURL; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=30',
      }, Buffer.from(body));
      return;
    }
    await pipeUpstream(raw, res, { timeout: 15000 });
    return;
  }

  if (p === '/api/vod/tvbox') {
    // 非授权模式不把卡密写进配置，避免公开配置里泄露卡密
    const code = REQUIRE_CARD ? extractCardCode(req, url) : '';
    sendJsonCacheable(res, 200, tvboxConfig(base, code), { sMaxAge: 3600, maxAge: 60 });
    return;
  }

  // ---- Playback: resolve 4K in real time, then fall back to an exact bfzy match ----
  if (p === '/play.m3u8' || p === '/resolve') {
    const id = String(url.searchParams.get('id') || '').trim();
    const seq = String(url.searchParams.get('p') || '0').trim();
    // r=1 表示客户端播放失败后的强制重解析：跳过 5 分钟地址缓存，重新向采集源取一次。
    const refresh = url.searchParams.get('r') === '1';
    if (!PLAY_ID_RE.test(id) || !/^\d{1,5}$/.test(seq)) {
      sendJson(res, 400, { ok: false, error: 'id/p required' });
      return;
    }
    const fail = (status, error) => {
      const browserNavigation = p === '/play.m3u8' && /text\/html/i.test(String(req.headers.accept || ''));
      if (status >= 500 && browserNavigation) {
        res.writeHead(302, { Location: '/?error=playback', 'Cache-Control': 'no-store' });
        res.end();
        return;
      }
      sendJson(res, status, { ok: false, error: String((error && error.message) || error) });
    };
    let resolved;
    try {
      resolved = await resolvePlayback(id, Number(seq), refresh);
      if (resolved.media.protocol !== 'https:' && resolved.media.protocol !== 'http:') throw new Error('bad media protocol');
    } catch (error) {
      fail(error.status || 502, error);
      return;
    }
    const { media, name } = resolved;
    const adRule = m3u8AdRule(media);
    if (p === '/play.m3u8' && adRule) {
      const r = await httpGetText(media.toString(), {
        headers: { 'User-Agent': UA, Accept: 'application/vnd.apple.mpegurl,application/x-mpegURL,*/*', 'Accept-Encoding': 'identity' },
        timeout: UPSTREAM_TIMEOUT_MS,
      });
      const body = r.body ? r.body.toString('utf8') : '';
      if (r.error || r.status < 200 || r.status >= 300 || !body.trimStart().startsWith('#EXTM3U')) {
        fail(502, 'media playlist unavailable');
        return;
      }
      let filtered;
      try { filtered = filterM3u8(body, media, adRule); } catch (error) { fail(502, error); return; }
      res.writeHead(200, {
        'Content-Type': 'application/vnd.apple.mpegurl; charset=utf-8',
        'Content-Length': Buffer.byteLength(filtered),
        'Cache-Control': 'no-store',
        'Access-Control-Allow-Origin': '*',
      });
      res.end(filtered);
      return;
    }
    if (p === '/resolve') {
      sendJsonCacheable(res, 200, { code: 1, msg: 'ok', url: media.toString(), name }, { sMaxAge: 300, maxAge: 30 });
      return;
    }
    res.writeHead(302, {
      Location: media.toString(),
      // 同一集的解析地址在魔都侧很稳定，边缘缓存 1 小时，避免每次开播都回源解析。
      // r=1（客户端强刷）不缓存，保证拿到最新地址。
      'Cache-Control': refresh
        ? 'no-store'
        : 'public, max-age=600, s-maxage=3600, stale-while-revalidate=7200',
      'Access-Control-Allow-Origin': '*',
    });
    res.end();
    return;
  }

  // ---- Frontend JSON API ----
  if (p === '/api/vod/kan') {
    const section = String(url.searchParams.get('section') || '');
    const cfg = KAN_SECTIONS[section];
    if (!cfg) { sendJson(res, 400, { ok: false, error: 'unknown kan section' }); return; }
    try {
      const limit = Math.max(1, Math.min(30, Number(url.searchParams.get('limit')) || 12));
      const data = await fetchKanRecommendations(section, limit);
      const payload = await publicListPayload(data, publicBase(req), false, { manual: false, t: cfg.ourType || undefined });
      payload.source = 'recommend';
      payload.section = section;
      sendJsonCacheable(res, 200, payload, { sMaxAge: 1800 });
      return;
    } catch {
      try {
        const fallback = await fetchListLive({ pg: 1, t: cfg.ourType || '', year: '', withPlay: false });
        const payload = await publicListPayload(
          { ...fallback, list: (fallback.list || []).slice(0, 12) },
          publicBase(req),
          false,
          { manual: false, t: cfg.ourType || undefined },
        );
        payload.source = 'fallback';
        payload.section = section;
        sendJsonCacheable(res, 200, payload, { sMaxAge: 1800 });
        return;
      } catch (error) {
        sendJson(res, 502, { ok: false, error: (error && error.message) || 'recommendation unavailable' });
        return;
      }
    }
  }

  if (p === '/api/vod/channels') {
    sendJsonCacheable(res, 200, {
      code: 1,
      list: CHANNELS.map(({ id, name, icon, desc, kind }) => ({ id, name, icon, desc, kind })),
    }, { sMaxAge: 1800 });
    return;
  }

  if (p === '/api/vod/channel') {
    const ch = CHANNEL_BY_ID.get(String(url.searchParams.get('id') || ''));
    if (!ch) { sendJson(res, 400, { ok: false, error: 'unknown channel id' }); return; }
    try {
      if (ch.kind === 'guess') {
        let body = {};
        if (req.method === 'POST') { try { body = await readJsonBody(req); } catch { body = {}; } }
        const idRe = /^(?:\d{1,10}|4kcz-\d{1,8})$/;
        const seeds = (Array.isArray(body.seeds) ? body.seeds : []).filter((s) => typeof s === 'string' && idRe.test(s)).slice(0, 5);
        const exclude = (Array.isArray(body.exclude) ? body.exclude : []).filter((s) => typeof s === 'string' && idRe.test(s)).slice(0, 20);
        const personalized = seeds.length ? await fetchGuessPool(seeds, exclude) : null;
        const pool = personalized || await fetchChannelPool(CHANNEL_BY_ID.get('praise'));
        const payload = await publicListPayload(sliceChannelPool(pool, url.searchParams.get('pg')), base, false, { manual: false });
        payload.channel = { id: ch.id, name: ch.name, icon: ch.icon, desc: ch.desc, personalized: Boolean(personalized) };
        // 浏览历史属于个人隐私：不写共享缓存，响应禁止缓存。
        res.setHeader('Cache-Control', 'no-store');
        sendJson(res, 200, payload);
        return;
      }
      const pool = await fetchChannelPool(ch);
      const payload = await publicListPayload(sliceChannelPool(pool, url.searchParams.get('pg')), base, false, { manual: false });
      payload.channel = { id: ch.id, name: ch.name, icon: ch.icon, desc: ch.desc };
      sendJsonCacheable(res, 200, payload, { sMaxAge: 120 });
      return;
    } catch (e) {
      sendJson(res, 502, { code: 0, msg: String(e.message || e), list: [] });
      return;
    }
  }

  if (p === '/api/vod/list' || p === '/api/vod/search' || p === '/api/vod/detail') {
    try {
      if (p === '/api/vod/list') {
        const data = await fetchList({
          pg: url.searchParams.get('pg'),
          t: url.searchParams.get('t'),
          year: parseYear(url.searchParams.get('year')),
        });
        sendJsonCacheable(res, 200, await publicListPayload(data, base, false, {
          t: url.searchParams.get('t'),
          year: parseYear(url.searchParams.get('year')),
        }), { sMaxAge: 1800 });
        return;
      }
      if (p === '/api/vod/search') {
        const wd = (url.searchParams.get('wd') || '').trim();
        if (!wd) { sendJson(res, 400, { ok: false, error: 'wd is required' }); return; }
        const data = await fetchSearch(wd.slice(0, 60), url.searchParams.get('pg'));
        sendJsonCacheable(res, 200, await publicListPayload(data, base, false, { wd }), { sMaxAge: 1800 });
        return;
      }
      const ids = (url.searchParams.get('ids') || '').trim();
      if (!ID_LIST_RE.test(ids)) {
        sendJson(res, 400, { ok: false, error: 'ids is required' });
        return;
      }
      const rows = await fetchDetail(ids);
      sendJsonCacheable(res, 200, await publicDetailPayload(rows, base, true), { sMaxAge: 3600 });
      return;
    } catch (e) {
      sendJson(res, 502, { code: 0, msg: String(e.message || e), list: [] });
      return;
    }
  }

  // ---- TVBox / MacCMS compatible endpoint ----
  if (p === '/api.php/provide/vod' || p === '/vod') {
    const ac = (url.searchParams.get('ac') || 'list').trim();
    const ids = (url.searchParams.get('ids') || '').trim();
    const wd = (url.searchParams.get('wd') || '').trim();
    try {
      if (ac === 'detail' && ID_LIST_RE.test(ids)) {
        const rows = await fetchDetail(ids);
        sendJsonCacheable(res, 200, await publicDetailPayload(rows, base, true), { sMaxAge: 3600 });
        return;
      }
      if (ac === 'detail' && wd) {
        const data = await fetchSearch(wd.slice(0, 60), url.searchParams.get('pg'), true);
        sendJsonCacheable(res, 200, await publicListPayload(data, base, true, { wd }), { sMaxAge: 1800 });
        return;
      }
      if (ac === 'detail') {
        const data = await fetchList({
          pg: url.searchParams.get('pg'),
          t: url.searchParams.get('t'),
          year: parseYear(url.searchParams.get('year')),
          withPlay: true,
        });
        sendJsonCacheable(res, 200, await publicListPayload(data, base, true, {
          t: url.searchParams.get('t'),
          year: parseYear(url.searchParams.get('year')),
        }), { sMaxAge: 1800 });
        return;
      }
      const data = await fetchList({
        pg: url.searchParams.get('pg'),
        t: url.searchParams.get('t'),
        year: parseYear(url.searchParams.get('year')),
      });
      sendJsonCacheable(res, 200, await publicListPayload(data, base, false, {
        t: url.searchParams.get('t'),
        year: parseYear(url.searchParams.get('year')),
      }), { sMaxAge: 1800 });
      return;
    } catch (e) {
      sendJson(res, 502, { code: 0, msg: String(e.message || e), list: [], class: CLASSES });
      return;
    }
  }

  // 未匹配的 API 路径必须返回 JSON 404，不能落到前端页面
  if (p.startsWith('/api')) {
    sendJson(res, 404, { ok: false, error: 'not found' });
    return;
  }

  // Static frontend
  if (isWorkers) {
    // Workers 下静态资源由 worker.mjs 的 Assets 绑定返回，正常不会走到这里。
    sendJson(res, 404, { ok: false, error: 'not found' });
    return;
  }
  let rel = decodeURIComponent(p);
  if (rel === '/' || rel === '') rel = '/index.html';
  const filePath = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    sendJson(res, 403, { ok: false, error: 'forbidden' });
    return;
  }
  import('node:fs').then(({ default: fs }) => {
    fs.stat(filePath, (err, st) => {
      if (err || !st.isFile()) {
        if (!path.extname(rel)) {
          serveFile(res, path.join(PUBLIC_DIR, 'index.html'));
          return;
        }
        sendJson(res, 404, { ok: false, error: 'not found' });
        return;
      }
      serveFile(res, filePath, /[?&]v=/.test(req.url || ''));
    });
  }).catch(() => sendJson(res, 404, { ok: false, error: 'not found' }));
}

// 请求入口：
// - Node：长驻进程直接调用，进程内存储已初始化一次；
// - Workers：每个请求建立一次存储连接，并通过 AsyncLocalStorage 做请求隔离。
export async function requestHandler(req, res) {
  if (!isWorkers) return handleRequest(req, res);
  let stores = null;
  try {
    const { initStores } = await import('./db-layer.mjs');
    stores = await initStores({ initialPassword: ENV.ADMIN_PASSWORD, moduleDir: __dirname, env: ENV });
  } catch (error) {
    console.warn(`[quying] data store disabled: ${String(error.message || error)}`);
  }
  try {
    return await requestStores.run(
      { catalogStore: stores?.catalogStore || null, adminStore: stores?.adminStore || null },
      () => handleRequest(req, res),
    );
  } finally {
    if (stores?.close) { try { await stores.close(); } catch {} }
  }
}

// 直播快照预热：Node 启动时调用；Workers 由 worker.mjs 放进 ctx.waitUntil。
export function warmLiveSnapshot() {
  return liveCollector.getSnapshot();
}

// Node 长驻进程入口（Vercel / Workers 由平台调用 requestHandler，不监听端口）。
if (!isWorkers && !ENV.VERCEL) {
  const { default: http } = await import('node:http');
  const server = http.createServer(requestHandler);

  server.on('error', (e) => {
    console.error('[quying] server error:', (e && e.message) || e);
    process.exit(1);
  });

  server.listen(PORT, HOST, () => {
    console.log(`[quying] listening on http://${HOST}:${PORT}`);
  });
  // 启动即预热直播快照：把十几秒的采集挪到后台，首个用户请求直接命中缓存而不是等采集。
  if (LIVE_WARMUP) {
    warmLiveSnapshot()
      .then((snapshot) => console.log(`[quying] live snapshot warmed: ${snapshot.channels.length} channels`))
      .catch((error) => console.warn('[quying] live warmup failed:', (error && error.message) || error));
  }
}

export default requestHandler;

if (!isWorkers) {
  process.on('uncaughtException', (e) => {
    console.error('[quying] uncaughtException:', (e && e.message) || e);
  });
  process.on('unhandledRejection', (e) => {
    console.error('[quying] unhandledRejection:', (e && e.message) || e);
  });
}


