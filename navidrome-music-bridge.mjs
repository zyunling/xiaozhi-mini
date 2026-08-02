/**
 * Navidrome 音源适配服务（对接 wt512 xiaozhi-plus-firmware 音源API规范）
 *
 * 固件调用流程：
 *   1. POST /SearchMusicList       → Navidrome Subsonic search3
 *   2. GET  /GetMusicDetail?id=xxx → 返回代理流地址 + 歌词
 *   3. GET  /stream/:id            → 代理 Navidrome stream（转明文HTTP + 带认证）
 *   4. GET  /lyrics/:id            → 代理 Navidrome 歌词
 *
 * 配置：通过环境变量（与 xiaozhi-mini 一样走 .env）
 *   NAVIDROME_URL        = http://你的NAS内网IP:4533
 *   NAVIDROME_USERNAME   = 你的Navidrome用户名
 *   NAVIDROME_PASSWORD   = 你的Navidrome密码
 *   MUSIC_BRIDGE_PORT    = 监听端口（默认 8650）
 *   MUSIC_BRIDGE_BIND_IP = 监听网卡IP（默认 0.0.0.0，需用能被BOX2访问到的局域网IP）
 *   MUSIC_BRIDGE_PUB_IP  = 对外公布的IP（用于拼接GetMusicDetail里的url，默认自动取第一个内网IP）
 */

import http from 'http';
import crypto from 'crypto';
import os from 'os';
import { URL } from 'url';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── 读取 .env ────────────────────────────────────────────────
function loadEnvFile(filePath) {
  try {
    if (!fs.existsSync(filePath)) return;
    const content = fs.readFileSync(filePath, 'utf8');
    for (const line of content.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx <= 0) continue;
      const key = trimmed.slice(0, eqIdx).trim();
      let val = trimmed.slice(eqIdx + 1).trim();
      if ((val.startsWith('"') && val.endsWith('"')) ||
          (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      if (!(key in process.env)) {
        process.env[key] = val;
      }
    }
  } catch (err) {
    console.warn('[env] 读取 .env 失败:', err.message);
  }
}
loadEnvFile(path.join(__dirname, '.env'));

// ── 配置 ─────────────────────────────────────────────────────
const NAVIDROME_URL      = process.env.NAVIDROME_URL      || '';
const NAVIDROME_USERNAME = process.env.NAVIDROME_USERNAME || '';
const NAVIDROME_PASSWORD = process.env.NAVIDROME_PASSWORD || '';
const MUSIC_BRIDGE_PORT    = parseInt(process.env.MUSIC_BRIDGE_PORT    || '8650', 10);
const MUSIC_BRIDGE_BIND_IP = process.env.MUSIC_BRIDGE_BIND_IP || '0.0.0.0';
const MUSIC_BRIDGE_PUB_IP  = process.env.MUSIC_BRIDGE_PUB_IP  || getFirstLanIp();

// 支持公网域名 + HTTPS（反向代理场景）
// 如果设了 MUSIC_BRIDGE_PUB_URL，直接用它作为 BRIDGE_BASE（如 https://music-bridge.example.com:17668）
const BRIDGE_BASE = process.env.MUSIC_BRIDGE_PUB_URL || `http://${MUSIC_BRIDGE_PUB_IP}:${MUSIC_BRIDGE_PORT}`;

// Navidrome Subsonic API 参数（固定）
const SUBSONIC_CLIENT  = 'xiaozhi-wt512';
const SUBSONIC_VERSION = '1.16.1';

// ── 工具：取第一个局域网 IPv4 ────────────────────────────────
function getFirstLanIp() {
  const ifaces = os.networkInterfaces();
  const candidates = [];
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name] || []) {
      if (iface.family === 'IPv4' && !iface.internal) {
        candidates.push(iface.address);
      }
    }
  }
  // 优先返回 192.168.x.x / 10.x.x.x / 172.16-31.x.x
  const privateRange = candidates.find(ip =>
    ip.startsWith('192.168.') ||
    ip.startsWith('10.') ||
    /^172\.(1[6-9]|2[0-9]|3[01])\./.test(ip)
  );
  return privateRange || candidates[0] || '127.0.0.1';
}

// ── 工具：Subsonic 认证（token+salt）─────────────────────────
function makeSalt(len = 8) {
  return crypto.randomBytes(len).toString('hex');
}
function buildSubsonicParams(extra = {}) {
  const salt = makeSalt();
  const token = crypto.createHash('md5')
    .update(NAVIDROME_PASSWORD + salt).digest('hex');
  const params = new URLSearchParams({
    u: NAVIDROME_USERNAME,
    t: token,
    s: salt,
    v: SUBSONIC_VERSION,
    c: SUBSONIC_CLIENT,
    f: 'json',
    ...extra,
  });
  return params.toString();
}

// ── 工具：调 Navidrome Subsonic API ──────────────────────────
function callSubsonic(endpoint, params = {}) {
  return new Promise((resolve, reject) => {
    const qs = buildSubsonicParams(params);
    const base = NAVIDROME_URL.replace(/\/$/, '');
    const urlStr = `${base}/rest/${endpoint}?${qs}`;
    const url = new URL(urlStr);

    const req = http.request({
      hostname: url.hostname,
      port:     url.port || 80,
      path:     url.pathname + url.search,
      method:   'GET',
      timeout:  15000,
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve(json);
        } catch (e) {
          reject(new Error(`Subsonic parse error: ${data.substring(0,200)}`));
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Subsonic timeout')); });
    req.end();
  });
}

// ── 工具：代理 Navidrome 二进制流（音频流 / 封面）────────────
function proxySubsonicStream(endpoint, params, req, res) {
  const qs = buildSubsonicParams(params);
  const base = NAVIDROME_URL.replace(/\/$/, '');
  const urlStr = `${base}/rest/${endpoint}?${qs}`;
  const url = new URL(urlStr);

  const proxyReq = http.request({
    hostname: url.hostname,
    port:     url.port || 80,
    path:     url.pathname + url.search,
    method:   'GET',
    headers:  {
      // 透传 Range，支持 ESP32 断点/随机读取
      ...(req.headers.range ? { 'Range': req.headers.range } : {}),
    },
    timeout:  600000, // 10分钟，长音频
  }, (proxyRes) => {
    // 透传状态码和关键头
    const code = proxyRes.statusCode || 200;
    const passHeaders = [
      'content-type', 'content-length', 'content-range',
      'accept-ranges', 'cache-control', 'last-modified', 'etag'
    ];
    for (const h of passHeaders) {
      if (proxyRes.headers[h]) res.setHeader(h, proxyRes.headers[h]);
    }
    // 不缓存流，便于调试
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.writeHead(code);
    proxyRes.pipe(res);
  });
  proxyReq.on('error', (e) => {
    console.error('[stream] 代理错误:', e.message);
    if (!res.headersSent) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
    }
    res.end(JSON.stringify({ error: 'upstream_error', message: e.message }));
  });
  proxyReq.on('timeout', () => proxyReq.destroy());
  req.on('aborted', () => proxyReq.destroy());
  proxyReq.end();
}

// ── 工具：JSON 响应辅助 ──────────────────────────────────────
function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, ApiKey',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

// 读取 JSON body
function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', c => data += c);
    req.on('end', () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); }
      catch (e) { reject(new Error('body parse error')); }
    });
    req.on('error', reject);
    setTimeout(() => reject(new Error('body timeout')), 10000);
  });
}

// ── 核心：搜索歌曲（SearchMusicList）────────────────────────
async function handleSearch(req, res) {
  const body = await readJsonBody(req).catch(() => ({}));
  const keyword = (body.Keyword || req.query?.Keyword || '').toString().trim();
  const page    = parseInt(body.Page    || req.query?.Page    || '1', 10);

  if (!keyword) {
    return sendJson(res, 200, { code: 200, message: 'ok', data: [] });
  }

  try {
    const songCount = 30;
    const offset    = (Math.max(1, page) - 1) * songCount;
    const result = await callSubsonic('search3', {
      query: keyword,
      songCount,
      songOffset: offset,
    });

    const songs = result?.['subsonic-response']?.searchResult3?.song || [];
    const data = songs.map(s => ({
      id:       s.id,
      name:     s.title || '未知歌曲',
      singer:   s.artist || '未知歌手',
      album:    s.album || '',
      duration: s.duration || 0,
      // 保留一些调试字段（固件应该会忽略多余字段）
      _cover:   s.coverArt || '',
    }));

    return sendJson(res, 200, { code: 200, message: 'ok', data });
  } catch (e) {
    console.error('[search] 失败:', e.message);
    return sendJson(res, 500, { code: 500, message: '搜索失败: ' + e.message, data: [] });
  }
}

// ── 核心：获取详情（GetMusicDetail 含 流url + 歌词）───────────
async function handleDetail(req, res) {
  const id = (req.query?.id || '').toString();
  if (!id) {
    return sendJson(res, 400, { code: 400, message: '缺少id参数', data: {} });
  }

  try {
    // 并行拉：歌词（不影响主流程，失败返回空）
    let lyric = '';
    try {
      // 优先用 getLyricsBySongId（Navidrome 特有）
      const lr = await callSubsonic('getLyricsBySongId', { id });
      const lrData = lr?.['subsonic-response']?.lyricsList?.lyrics?.[0]
                  || lr?.['subsonic-response']?.lyrics;
      if (lrData?.value) lyric = lrData.value;
    } catch (_) {
      // 回退：search3 拿到 artist/title 后调 getLyrics(不带ID版本)
      try {
        const info = await callSubsonic('getSong', { id });
        const s = info?.['subsonic-response']?.song;
        if (s?.title && s?.artist) {
          const lr2 = await callSubsonic('getLyrics', { artist: s.artist, title: s.title });
          const lrData2 = lr2?.['subsonic-response']?.lyrics;
          if (lrData2?.value) lyric = lrData2.value;
        }
      } catch (_) {}
    }

    const data = {
      id,
      url:    `${BRIDGE_BASE}/stream/${encodeURIComponent(id)}`,
      lyric,
    };
    return sendJson(res, 200, { code: 200, message: 'ok', data });
  } catch (e) {
    console.error('[detail] 失败:', e.message);
    return sendJson(res, 500, { code: 500, message: '获取详情失败: ' + e.message, data: {} });
  }
}

// ── 推荐列表（SearchMusicRecommendedList，简化实现：随机热歌）
async function handleRecommend(req, res) {
  const body = await readJsonBody(req).catch(() => ({}));
  const size    = parseInt(body.Size    || req.query?.Size    || '20', 10);
  const keyword = (body.Keyword || req.query?.Keyword || '').toString().trim();

  try {
    // 有关键词就按关键词搜；没有就用 getAlbumList2 random 拿一些歌
    let songs = [];
    if (keyword) {
      const r = await callSubsonic('search3', { query: keyword, songCount: size });
      songs = r?.['subsonic-response']?.searchResult3?.song || [];
    } else {
      const r = await callSubsonic('getAlbumList2', { type: 'random', size: 5 });
      const albums = r?.['subsonic-response']?.albumList2?.album || [];
      // 每个专辑抽一首（简化）
      for (const a of albums) {
        if (songs.length >= size) break;
        if (a.song && a.song.length > 0) {
          songs.push(a.song[0]);
        }
      }
      // 不够再用 search3 空搜兜底（搜"周杰伦""Taylor Swift"之类凑数，避免空列表）
      if (songs.length < size) {
        const fallback = await callSubsonic('search3', {
          query: 'the a', songCount: size,
        });
        const more = fallback?.['subsonic-response']?.searchResult3?.song || [];
        for (const s of more) {
          if (songs.length >= size) break;
          if (!songs.find(x => x.id === s.id)) songs.push(s);
        }
      }
    }

    const data = songs.map(s => ({
      id:       s.id,
      name:     s.title || '未知歌曲',
      singer:   s.artist || '未知歌手',
      album:    s.album || '',
      duration: s.duration || 0,
    }));

    return sendJson(res, 200, { code: 200, message: 'ok', data });
  } catch (e) {
    console.error('[recommend] 失败:', e.message);
    return sendJson(res, 500, { code: 500, message: '推荐失败: ' + e.message, data: [] });
  }
}

// ── URL Query 解析 ───────────────────────────────────────────
function parseQuery(urlStr) {
  const idx = urlStr.indexOf('?');
  const q = {};
  if (idx < 0) return q;
  const qs = urlStr.slice(idx + 1);
  for (const pair of qs.split('&')) {
    if (!pair) continue;
    const [k, v = ''] = pair.split('=');
    try { q[decodeURIComponent(k)] = decodeURIComponent(v); }
    catch { q[k] = v; }
  }
  return q;
}

// ── HTTP 路由 ────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  // CORS 预检
  if (req.method === 'OPTIONS') {
    return sendJson(res, 204, {});
  }

  const urlObj = new URL(req.url, 'http://localhost');
  const pathname = urlObj.pathname.replace(/\/$/, '') || '/';
  req.query = parseQuery(req.url);

  // 健康检查
  if (pathname === '/' || pathname === '/health') {
    return sendJson(res, 200, {
      code: 200,
      message: 'Navidrome music bridge is running',
      data: {
        navidrome: NAVIDROME_URL || '未配置',
        username: NAVIDROME_USERNAME || '未配置',
        bridge_base: BRIDGE_BASE,
      },
    });
  }

  // 3 个音源 API（同时支持 POST 业务请求和 GET 健康检查）
  if (pathname === '/SearchMusicList') {
    if (req.method === 'GET' || req.method === 'POST') {
      return handleSearch(req, res).catch(e => sendJson(res, 500, { code: 500, message: e.message, data: [] }));
    }
  }
  if (pathname === '/SearchMusicRecommendedList') {
    if (req.method === 'GET' || req.method === 'POST') {
      return handleRecommend(req, res).catch(e => sendJson(res, 500, { code: 500, message: e.message, data: [] }));
    }
  }
  if (pathname === '/GetMusicDetail') {
    if (req.method === 'GET' || req.method === 'POST') {
      return handleDetail(req, res).catch(e => sendJson(res, 500, { code: 500, message: e.message, data: {} }));
    }
  }

  // 音频流代理
  const streamMatch = pathname.match(/^\/stream\/([^/]+)$/);
  if (streamMatch) {
    const id = decodeURIComponent(streamMatch[1]);
    return proxySubsonicStream('stream', { id, format: 'mp3' }, req, res);
  }

  // 歌词代理（可选，当前 GetMusicDetail 已经把 lyric 直接塞 body 里了）
  const lyricMatch = pathname.match(/^\/lyrics\/([^/]+)$/);
  if (lyricMatch) {
    const id = decodeURIComponent(lyricMatch[1]);
    return (async () => {
      try {
        const r = await callSubsonic('getLyricsBySongId', { id });
        const l = r?.['subsonic-response']?.lyricsList?.lyrics?.[0]
               || r?.['subsonic-response']?.lyrics;
        return sendJson(res, 200, { code: 200, message: 'ok', data: l?.value || '' });
      } catch (e) {
        return sendJson(res, 500, { code: 500, message: e.message, data: '' });
      }
    })();
  }

  // 404
  return sendJson(res, 404, { code: 404, message: 'not found', data: {} });
});

// ── 启动 ─────────────────────────────────────────────────────
function checkConfig() {
  const issues = [];
  if (!NAVIDROME_URL)      issues.push('NAVIDROME_URL 未配置');
  if (!NAVIDROME_USERNAME) issues.push('NAVIDROME_USERNAME 未配置');
  if (!NAVIDROME_PASSWORD) issues.push('NAVIDROME_PASSWORD 未配置');
  // 格式校验
  if (NAVIDROME_URL && !/^https?:\/\//i.test(NAVIDROME_URL)) {
    issues.push('NAVIDROME_URL 需要 http:// 或 https:// 开头');
  }
  return issues;
}

const issues = checkConfig();
if (issues.length > 0) {
  console.error('❌ 配置不完整，请在 .env 中设置以下项：');
  for (const i of issues) console.error('   - ' + i);
  console.error('');
  console.error('   示例：');
  console.error('     NAVIDROME_URL=http://192.168.1.100:4533');
  console.error('     NAVIDROME_USERNAME=admin');
  console.error('     NAVIDROME_PASSWORD=your_password');
}

// 就算配置不完整也起服务（方便 curl /health 排查）
server.listen(MUSIC_BRIDGE_PORT, MUSIC_BRIDGE_BIND_IP, () => {
  console.log('');
  console.log('🎵 Navidrome Music Bridge 已启动');
  console.log('   监听:   http://' + MUSIC_BRIDGE_BIND_IP + ':' + MUSIC_BRIDGE_PORT);
  console.log('   对外:   ' + BRIDGE_BASE);
  console.log('   健康:   GET  ' + BRIDGE_BASE + '/health');
  console.log('');
  console.log('   📎 wt512 固件后台请填入：');
  console.log('     SearchMusicListApiUrl            = ' + BRIDGE_BASE + '/SearchMusicList');
  console.log('     GetMusicDetailApiUrl             = ' + BRIDGE_BASE + '/GetMusicDetail');
  console.log('     SearchMusicRecommendedListApiUrl = ' + BRIDGE_BASE + '/SearchMusicRecommendedList');
  console.log('     ApiKey（留空即可，本服务不强制校验）= ');
  if (issues.length) {
    console.log('');
    console.log('⚠️  配置警告（上述项需要填入后重启才会真正工作）：');
    for (const i of issues) console.log('   - ' + i);
  }
});
