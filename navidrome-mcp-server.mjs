/**
 * Navidrome MCP 服务（纯 Node.js 实现，无 SDK 依赖）
 *
 * 供 xiaozhi-mini 作为 upstream 接入，使小智云端 AI 能直接搜索 Navidrome 曲库
 * 并通过 self.music.play_url 播放，绕开固件自定义音源配置。
 *
 * 暴露 3 个 MCP 工具：
 *   navidrome_search(query, page, page_size)  — 搜索歌曲
 *   navidrome_get_stream_url(song_id)         — 获取播放流URL + 歌词
 *   navidrome_random_song(size)               — 随机推荐
 *
 * 配置：通过环境变量（与 navidrome-music-bridge 共用 .env）
 *   NAVIDROME_URL        = http://你的NAS内网IP:4533
 *   NAVIDROME_USERNAME   = 你的Navidrome用户名
 *   NAVIDROME_PASSWORD   = 你的Navidrome密码
 *   NAVIDROME_MCP_PORT   = MCP 服务端口（默认 8660）
 *   MUSIC_BRIDGE_PUB_IP  = 对外公布的IP（用于拼接流URL）
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
const MCP_PORT          = parseInt(process.env.NAVIDROME_MCP_PORT || '8660', 10);
const MCP_BIND_IP       = process.env.MUSIC_BRIDGE_BIND_IP || '0.0.0.0';
const PUB_IP            = process.env.MUSIC_BRIDGE_PUB_IP  || getFirstLanIp();
const STREAM_PORT       = process.env.MUSIC_BRIDGE_PORT || '8650';

const BRIDGE_BASE = `http://${PUB_IP}:${STREAM_PORT}`;

const SUBSONIC_CLIENT  = 'xiaozhi-mcp';
const SUBSONIC_VERSION = '1.16.1';

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
  const privateRange = candidates.find(ip =>
    ip.startsWith('192.168.') ||
    ip.startsWith('10.') ||
    /^172\.(1[6-9]|2[0-9]|3[01])\./.test(ip)
  );
  return privateRange || candidates[0] || '127.0.0.1';
}

// ── Subsonic 认证 ─────────────────────────────────────────────
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
          reject(new Error(`Subsonic parse error: ${data.substring(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Subsonic timeout')); });
    req.end();
  });
}

// ── 歌曲搜索 ──────────────────────────────────────────────────
async function searchSongs(query, songCount = 30, songOffset = 0) {
  const result = await callSubsonic('search3', { query, songCount, songOffset });
  const songs = result?.['subsonic-response']?.searchResult3?.song || [];
  return songs.map(s => ({
    id: s.id,
    name: s.title || '未知歌曲',
    singer: s.artist || '未知歌手',
    album: s.album || '',
    duration: s.duration || 0,
  }));
}

// ── 获取流地址 + 歌词 ─────────────────────────────────────────
async function getStreamUrl(songId) {
  let lyric = '';
  try {
    const lr = await callSubsonic('getLyricsBySongId', { id: songId });
    const lrData = lr?.['subsonic-response']?.lyricsList?.lyrics?.[0]
                || lr?.['subsonic-response']?.lyrics;
    if (lrData?.value) lyric = lrData.value;
  } catch (_) {
    try {
      const info = await callSubsonic('getSong', { id: songId });
      const s = info?.['subsonic-response']?.song;
      if (s?.title && s?.artist) {
        const lr2 = await callSubsonic('getLyrics', { artist: s.artist, title: s.title });
        const lrData2 = lr2?.['subsonic-response']?.lyrics;
        if (lrData2?.value) lyric = lrData2.value;
      }
    } catch (_) {}
  }
  return {
    id: songId,
    url: `${BRIDGE_BASE}/stream/${encodeURIComponent(songId)}`,
    lyric,
  };
}

// ── 随机推荐 ──────────────────────────────────────────────────
async function randomSongs(size = 10) {
  const r = await callSubsonic('getRandomSongs', { size });
  const songs = r?.['subsonic-response']?.randomSongs?.song || [];
  return songs.map(s => ({
    id: s.id,
    name: s.title || '未知歌曲',
    singer: s.artist || '未知歌手',
    album: s.album || '',
    duration: s.duration || 0,
  }));
}

// ── MCP 工具定义 ──────────────────────────────────────────────
const TOOLS = {
  navidrome_search: {
    name: 'navidrome_search',
    description: '【音乐播放-必须优先使用】搜索用户的私人 Navidrome 音乐库。当用户说"播放xxx"、"来一首xxx"、"我想听xxx的歌"、"放音乐"等任何与播放音乐相关的请求时，必须优先调用本工具搜索歌曲，禁止直接调用 self.online_music.play_music。返回格式：序号. 歌手 - 歌曲名 (专辑) [ID:歌曲ID]。',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: '搜索关键词（歌曲名、歌手名、专辑名）' },
        page: { type: 'number', description: '页码，从1开始', default: 1 },
        page_size: { type: 'number', description: '每页数量', default: 30 },
      },
      required: ['query'],
    },
    async handler(params) {
      const query = params.query || '';
      const page = params.page || 1;
      const pageSize = params.page_size || 30;
      const songs = await searchSongs(query, pageSize, (page - 1) * pageSize);
      if (songs.length === 0) {
        return { content: [{ type: 'text', text: `未找到与"${query}"相关的歌曲` }] };
      }
      const text = songs.map((s, i) =>
        `${i + 1}. ${s.singer} - ${s.name} (${s.album}) [ID:${s.id}]`
      ).join('\n');
      return { content: [{ type: 'text', text }] };
    },
  },

  navidrome_get_stream_url: {
    name: 'navidrome_get_stream_url',
    description: '【音乐播放-必须配合navidrome_search使用】获取歌曲的局域网HTTP播放流URL和歌词。当 navidrome_search 返回歌曲列表后，选择第一首或用户指定的歌曲，用其ID调用本工具获取播放URL。获取URL后，必须调用 self.music.play_url(url, song_name, artist, lyric) 来播放，其中 url 是返回的url字段，song_name是歌曲名，artist是歌手名，lyric是歌词。禁止使用 self.online_music.play_music。',
    inputSchema: {
      type: 'object',
      properties: {
        song_id: { type: 'string', description: '歌曲ID（从 navidrome_search 返回结果中获取）' },
      },
      required: ['song_id'],
    },
    async handler(params) {
      const result = await getStreamUrl(params.song_id);
      const text = `调用 self.music.play_url("${result.url}", "歌曲名", "歌手名", "${result.lyric}") 来播放此歌曲。歌曲ID: ${result.id}`;
      return { content: [{ type: 'text', text }] };
    },
  },

  navidrome_random_song: {
    name: 'navidrome_random_song',
    description: '【音乐播放-随机推荐】从用户的私人 Navidrome 音乐库随机推荐歌曲。当用户说"随便来一首"、"放点音乐"、"来个随机的"等没有指定具体歌曲时使用。返回后用 navidrome_get_stream_url 获取URL再播放。',
    inputSchema: {
      type: 'object',
      properties: {
        size: { type: 'number', description: '推荐数量', default: 10 },
      },
    },
    async handler(params) {
      const size = params.size || 10;
      const songs = await randomSongs(size);
      if (songs.length === 0) {
        return { content: [{ type: 'text', text: 'Navidrome 音乐库为空' }] };
      }
      const text = songs.map((s, i) =>
        `${i + 1}. ${s.singer} - ${s.name} (${s.album}) [ID:${s.id}]`
      ).join('\n');
      return { content: [{ type: 'text', text }] };
    },
  },
};

// ── MCP JSON-RPC 协议处理 ────────────────────────────────────
// 注意：xiaozhi-mini 的 streamable-http 客户端直接调 tools/list，
// 不发 initialize，所以这里不强制要求初始化。

function handleMCPRequest(req, res) {
  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', () => {
    let request;
    try {
      request = JSON.parse(body);
    } catch (e) {
      return sendJson(res, 400, { jsonrpc: '2.0', error: { code: -32700, message: 'Parse error' }, id: null });
    }

    const { method, params, id } = request;

    if (method === 'initialize') {
      return sendJson(res, 200, {
        jsonrpc: '2.0',
        result: {
          protocolVersion: '2024-11-05',
          capabilities: {
            tools: {},
          },
          serverInfo: {
            name: 'navidrome-mcp',
            version: '1.0.0',
          },
        },
        id,
      });
    }

    if (method === 'notifications/initialized') {
      return sendJson(res, 202, {});
    }

    if (method === 'tools/list') {
      const tools = Object.values(TOOLS).map(t => ({
        name: t.name,
        description: t.description,
        inputSchema: t.inputSchema,
      }));
      return sendJson(res, 200, {
        jsonrpc: '2.0',
        result: { tools },
        id,
      });
    }

    if (method === 'tools/call') {
      const toolName = params?.name;
      const arguments_ = params?.arguments || {};
      const tool = TOOLS[toolName];
      if (!tool) {
        return sendJson(res, 200, {
          jsonrpc: '2.0',
          error: { code: -32601, message: `Tool not found: ${toolName}` },
          id,
        });
      }
      tool.handler(arguments_)
        .then(result => {
          sendJson(res, 200, {
            jsonrpc: '2.0',
            result,
            id,
          });
        })
        .catch(e => {
          console.error('[mcp] tool error:', e.message);
          sendJson(res, 200, {
            jsonrpc: '2.0',
            result: { content: [{ type: 'text', text: `工具执行失败: ${e.message}` }], isError: true },
            id,
          });
        });
      return; // 异步处理，不在这里发送
    }

    return sendJson(res, 200, {
      jsonrpc: '2.0',
      error: { code: -32601, message: `Method not found: ${method}` },
      id,
    });
  });
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Content-Length': Buffer.byteLength(body),
  });
  res.end(body);
}

// ── HTTP 服务器 ───────────────────────────────────────────────
const server = http.createServer((req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Accept');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // 健康检查
  if (req.url === '/health') {
    return sendJson(res, 200, {
      status: 'ok',
      navidrome: NAVIDROME_URL || '未配置',
      mcp_port: MCP_PORT,
      tools: Object.keys(TOOLS),
    });
  }

  // MCP 端点
  if (req.url === '/mcp/' || req.url === '/mcp') {
    if (req.method === 'POST') {
      return handleMCPRequest(req, res);
    }
    // GET 请求（可能是 streamable-http 的 SSE 连接）
    // 简单处理：返回 204 No Content
    res.writeHead(204);
    return res.end();
  }

  // 根路径 - 简单状态
  if (req.url === '/') {
    return sendJson(res, 200, {
      name: 'Navidrome MCP Server',
      version: '1.0.0',
      tools: Object.keys(TOOLS),
      mcp_endpoint: '/mcp/',
    });
  }

  // 404
  return sendJson(res, 404, { error: 'not found' });
});

server.listen(MCP_PORT, MCP_BIND_IP, () => {
  console.log('');
  console.log('🎵 Navidrome MCP 服务已启动（纯 Node.js 实现）');
  console.log('   监听:   http://' + MCP_BIND_IP + ':' + MCP_PORT);
  console.log('   对外:   http://' + PUB_IP + ':' + MCP_PORT);
  console.log('   MCP端点: http://' + PUB_IP + ':' + MCP_PORT + '/mcp/');
  console.log('   健康检查: http://' + PUB_IP + ':' + MCP_PORT + '/health');
  console.log('');
  console.log('   📎 在 config.yaml 中添加以下 upstream：');
  console.log('     navidrome:');
  console.log('       type: streamable-http');
  console.log('       url: "http://127.0.0.1:' + MCP_PORT + '/mcp/"');
  console.log('       headers:');
  console.log('         Accept: "application/json, text/event-stream"');
  console.log('');
  console.log('   可用 MCP 工具：');
  for (const t of Object.values(TOOLS)) {
    console.log(`     - ${t.name}`);
  }
  console.log('');
  console.log('   💡 使用方式：');
  console.log('     对小智说"播放七里香"，AI 会自动调用 navidrome_search 搜索，');
  console.log('     再调用 navidrome_get_stream_url 获取流地址，最后用 self.music.play_url 播放。');
  console.log('');
  console.log('   ⚠️  注意：此方式绕开固件音源配置，但需要小智云端 AI 支持 MCP 工具链调用。');
});
