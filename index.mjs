/**
 * xiaozhi-mini v2.8.1
 * 轻量 MCP 聚合桥：小智 wss(MCP) ↔ N 个 upstream MCP Server
 * streamable-http: 自己 fetch + SSE 解析（避开 SDK 路径问题）
 * stdio: 仍用 SDK StdioClientTransport（路径稳定）
 * 支持多个小智接入（每个完整 wss URL 一个 XiaozhiConnection 实例）
 * 所有 token/url 统一放在 .env，config.yaml 用 ${VAR} 引用
 */

import WebSocket from 'ws';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import yaml from 'yaml';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const VERSION = '2.8.2';

// ── 工具：轻量 .env 文件解析（零依赖） ─────────────────────
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
          (val.startsWith("'") && val.endsWith("'")) ||
          (val.startsWith('`') && val.endsWith('`'))) {
        val = val.slice(1, -1);
      }
      if (!(key in process.env)) {
        process.env[key] = val;
      }
    }
  } catch (err) {
    console.warn('⚠️  读取 .env 文件失败:', err.message);
  }
}

loadEnvFile(path.join(__dirname, '.env'));

// ── 加载配置 ───────────────────────────────────────────────
let config;
try {
  config = yaml.parse(fs.readFileSync(path.join(__dirname, 'config.yaml'), 'utf8'));
} catch (err) {
  console.error('❌ config.yaml 解析失败:', err.message);
  process.exit(1);
}

const RECONNECT_DELAY = config.xiaozhi?.reconnect_delay || 2000;
const RECONNECT_MAX_DELAY = config.xiaozhi?.reconnect_max_delay || 60000;
const MESSAGE_QUEUE_MAX_SIZE = config.xiaozhi?.message_queue_max || 100;
const AGGRESSIVE_RECONNECT = config.xiaozhi?.aggressive_reconnect !== false;
const MAX_QUICK_RECONNECT = config.xiaozhi?.max_quick_reconnect || 10;
const INFINITE_RECONNECT_DELAY = config.xiaozhi?.infinite_reconnect_delay || 1800000;
const MAX_INFINITE_RETRIES = config.xiaozhi?.max_infinite_retries || 48;
const SLEEP_THRESHOLD = config.xiaozhi?.sleep_threshold || 12;
const SLEEP_INTERVAL = config.xiaozhi?.sleep_interval || 7200000;

// ── 连接健康检查参数 ──────────────────────────────────────
// 解决"静默断连"问题：WebSocket TCP 连接在中间网络设备（NAT/代理/负载均衡）
// 处被静默回收时，ws 库不会触发 close 事件，客户端误以为连接仍然存活，
// 导致工具列表不再推送。以下机制确保连接真正可用 + 工具定期自动重推。
// health_check_interval: 健康检查周期（ms），默认 60s
// stale_threshold: 连接静默阈值（ms），默认 300s（5 分钟）。
//   小智服务端每 30s 发一次 JSON-RPC ping，5 分钟无任何消息 = 连接已死
// tools_refresh_interval: 工具列表自动重推周期（ms），默认 1800000（30 分钟）
const HEALTH_CHECK_INTERVAL = config.xiaozhi?.health_check_interval || 60000;
const STALE_THRESHOLD = config.xiaozhi?.stale_threshold || 300000;
const TOOLS_REFRESH_INTERVAL = config.xiaozhi?.tools_refresh_interval || 1800000;

const upstreams = new Map();

// ── 工具：日志脱敏，避免 token/密钥泄露到日志 ─────────────
function maskUrl(s) {
  if (s instanceof Error) s = s.message;
  if (typeof s !== 'string') return String(s);
  return s
    .replace(/(token=)[^&\s"']+/gi, '$1***')
    // 遮蔽 URL query 中的各类密钥（tavilyApiKey / api_key / secret 等），
    // 避免 fetch 超时错误把带密钥的完整 URL 打进日志
    .replace(/([?&][^=\s"']*(?:key|token|secret|auth|password)[^=\s"']*=)[^&\s"']+/gi, '$1***')
    .replace(/(\/mcp\/)[^?\s"']+/gi, '$1***')
    .replace(/(\/private_\/)[^?\s"']+/gi, '$1***')
    .replace(/(\/\d{3,5}\/)[A-Za-z0-9_\-]{8,}/gi, '$1***');
}

function interpolateEnv(str) {
  if (typeof str !== 'string') return str;
  return str.replace(/\$\{([^}]+)\}/g, (_, key) => {
    const value = process.env[key];
    if (value === undefined) {
      console.warn(`⚠️  环境变量 ${key} 未定义`);
      return `\${${key}}`;
    }
    return value;
  });
}

// ── 工具：解析 SSE 流，取出 JSON-RPC 响应 ─────────────────
// 兼容两种情况：1) 单个 JSON 响应  2) SSE 流式响应（取完整 JSON）
async function parseSSEResponse(response) {
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`HTTP ${response.status}: ${text.substring(0, 200)}`);
  }
  const text = await response.text();
  if (!text || text.trim() === '') {
    throw new Error('Empty response');
  }
  const lines = text.split('\n');
  const dataLines = [];
  for (const line of lines) {
    if (line.startsWith('data: ')) {
      const data = line.slice(6);
      if (data === '[DONE]') break;
      dataLines.push(data);
    }
  }
  if (dataLines.length === 0) {
    try {
      return JSON.parse(text);
    } catch (e) {
      throw new Error(`JSON parse failed: ${text.substring(0, 200)}`);
    }
  }
  const lastData = dataLines[dataLines.length - 1];
  try {
    return JSON.parse(lastData.trim());
  } catch {
    try {
      return JSON.parse(dataLines.join('').trim());
    } catch (e) {
      throw new Error(`SSE parse failed: ${dataLines.join('').substring(0, 200)}`);
    }
  }
}

// ── 工具：对 streamable-http upstream 做一次 JSON-RPC POST ──
// 必须带超时：否则某个 upstream（如 tavily）连接被黑洞时 fetch 永不返回，
// 会卡死整个 initUpstreams()，导致后面再也不连接小智、工具全部丢失
async function postHA(up, method, params, rpcId, timeoutMs = 15000) {
  let res;
  try {
    res = await fetch(up.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/event-stream',
        ...(up.headers || {})
      },
      body: JSON.stringify({ jsonrpc: '2.0', method, params, id: rpcId }),
      signal: AbortSignal.timeout(timeoutMs)
    });
  } catch (err) {
    if (err.name === 'TimeoutError' || err.name === 'AbortError') {
      throw new Error(`请求超时(${Math.round(timeoutMs / 1000)}s): ${maskUrl(up.url)}`);
    }
    throw err;
  }
  return await parseSSEResponse(res);
}

// ── 1.4 schema / 参数 / 结果的兼容适配（v2.8.2 候选）─────────
//
// 小智云端对 MCP 的支持有限，实测有三类失配：
//  1) 不支持 anyOf 联合类型，会把数组字段序列化成字符串 "[\"media_player\"]"；
//  2) 工具级错误用 HTTP 200 + result.isError 表达，代理若只看 HTTP 会误记 → ok；
//  3) 实体名只能精确匹配，查 "热水器" 拿不到「热水器当前功率」传感器。
// 以下函数只加工「推送给小智的 schema」与「转发前后的参数/结果」，
// 不改变上游 HA 语义；所有新增行为都可用 config.yaml 开关关闭。

// 把 anyOf [string, array<string>] 简化为单一 string（递归、返回深拷贝）
function simplifyAnyOfSchema(schema) {
  if (!schema || typeof schema !== 'object') return schema;
  if (Array.isArray(schema)) return schema.map(simplifyAnyOfSchema);
  const out = {};
  for (const [k, v] of Object.entries(schema)) {
    if (k === 'anyOf' && Array.isArray(v) && v.length === 2) {
      const hasString = v.some(x => x && x.type === 'string');
      const hasStringArray = v.some(x => x && x.type === 'array' &&
        (!x.items || x.items.type === 'string'));
      if (hasString && hasStringArray) {
        out.type = 'string';
        continue;
      }
    }
    out[k] = simplifyAnyOfSchema(v);
  }
  return out;
}

// 还原被小智云端字符串化的数组参数："[\"media_player\"]" -> ["media_player"]
function restoreStringifiedArrays(args, log) {
  const out = { ...args };
  for (const [k, v] of Object.entries(out)) {
    if (typeof v !== 'string') continue;
    const str = v.trim();
    if (!str.startsWith('[') || !str.endsWith(']')) continue;
    try {
      const parsed = JSON.parse(str);
      if (Array.isArray(parsed)) {
        out[k] = parsed;
        log?.(`🔄 参数还原: ${k} → ${JSON.stringify(parsed)}`);
      }
    } catch (e) { /* 非法 JSON 数组，原样透传 */ }
  }
  return out;
}

// ── 1.4b 通用工具结果体量治理 ─────────────────────────────
//
// 小智云端对单次工具结果有隐含上限：实测 409B / 2562B 正常回答，
// 5832B（tavily 搜索）与 8507B（HA 全量）都出现「→ ok 但不回答」。
// 这里对所有 streamable-http 工具结果做统一压缩，而不是给某个工具打补丁：
//   JSON 结果 → 去掉 raw_content/images 等冗余字段 + 递归截断长字符串
//   纯文本   → 按整行保留（不切断实体块）+ 附省略提示
// isError 结果不处理（保留错误信息与名称候选增强）。
// 配置 xiaozhi.max_tool_result_bytes，默认 2800；设 0 关闭。
// 长字符串压缩：含换行时按【整行】保留，不切断实体块或段落；否则按字符截断。
// 这样 HA 的 Live Context 不会留下半个实体，搜索结果也不会被切在句子中间。
function compactString(s, maxStr) {
  if (s.length <= maxStr) return s;
  // 由记录块构成的文本（如 HA Live Context 的 "- names:" 块）按整块保留，
  // 避免最后留下只有 names/domain、缺 state 的半个实体
  if (s.includes('\n- names: ')) {
    const parts = s.split(/\n(?=- names: )/);
    const header = parts[0];
    const blocks = parts.slice(1);
    const kept = [];
    let used = Buffer.byteLength(header);
    for (const b of blocks) {
      const size = Buffer.byteLength(b) + 1;
      if (used + size > maxStr) break;
      kept.push(b); used += size;
    }
    return header + '\n' + kept.join('\n') +
      `\n…(共 ${blocks.length} 个实体，保留前 ${kept.length} 个)`;
  }
  if (s.includes('\n')) {
    const lines = s.split('\n');
    const kept = [];
    let used = 0;
    for (const l of lines) {
      if (used + l.length + 1 > maxStr) break;
      kept.push(l); used += l.length + 1;
    }
    return kept.join('\n') + `\n…(共 ${lines.length} 行，保留前 ${kept.length} 行)`;
  }
  return s.slice(0, maxStr) + '…';
}

// 数组首项通常最相关（搜索结果按相关度排序），给它 2 倍额度，
// 避免为了压体积把第一条里的关键数字也截掉。
function compactJsonValue(v, maxStr, primary = false, maxItems = 8) {
  const cap = primary ? Math.round(maxStr * 2) : maxStr;
  if (typeof v === 'string') return compactString(v, cap);
  if (Array.isArray(v)) return v.slice(0, maxItems)
    .map((x, i) => compactJsonValue(x, maxStr, i === 0, maxItems));
  if (v && typeof v === 'object') {
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      if (/^(raw_content|images|follow_up_questions|request_id|auth_mode|response_time|score|id)$/i.test(k)) continue;
      out[k] = compactJsonValue(val, maxStr, primary, maxItems);
    }
    return out;
  }
  return v;
}

function compactToolResult(result, cfg, log) {
  const raw = cfg?.xiaozhi?.max_tool_result_bytes;
  const limit = (raw === 0 || raw === '0') ? 0 : Math.max(1000, Number(raw) || 2800);
  if (limit <= 0 || !result?.content?.length) return result;
  const items = result.content;
  const total = items.reduce((n, c) => n + Buffer.byteLength(c.text || ''), 0);
  if (total <= limit) return result;

  const newItems = items.map(c => {
    if (typeof c.text !== 'string') return c;
    const t = c.text;
    let obj = null;
    try { obj = JSON.parse(t); } catch (e) { /* 非 JSON */ }
    if (obj && typeof obj === 'object') {
      let out = t;
      // 逐级收紧「单条长度 + 结果条数」；搜索结果按相关度排序，保留前几条即可
      for (const [maxStr, maxItems] of
        [[1200, 3], [900, 3], [700, 3], [550, 2], [400, 2], [300, 2], [220, 2]]) {
        out = JSON.stringify(compactJsonValue(obj, maxStr, false, maxItems));
        if (Buffer.byteLength(out) <= limit) break;
      }
      return { ...c, text: out };
    }
    const lines = t.split('\n');
    const kept = [];
    let bytes = 0;
    for (const l of lines) {
      const b = Buffer.byteLength(l) + 1;
      if (bytes + b > limit - 90) break;
      kept.push(l); bytes += b;
    }
    return { ...c, text: kept.join('\n') +
      `\n(结果过长已截断：共 ${lines.length} 行，保留前 ${kept.length} 行；请缩小查询范围)` };
  });
  const after = newItems.reduce((n, c) => n + Buffer.byteLength(c.text || ''), 0);
  log?.(`📉 结果压缩: ${total}B → ${after}B (上限 ${limit}B)`);
  return { ...result, content: newItems };
}

// 从 MCP CallToolResult 中提取工具级错误（HA 用 HTTP 200 + isError 表达失败）
function toolErrorOf(result) {
  if (!result || typeof result !== 'object') return null;
  const text = (result.content || []).map(p => p?.text || '').join('\n');
  if (result.isError === true) return text.slice(0, 180) || 'isError=true';
  try {
    const obj = JSON.parse(text);
    if (obj && typeof obj.error === 'string') return obj.error.slice(0, 180);
  } catch (e) { /* 非 JSON 文本，按成功处理 */ }
  return null;
}

// ── 1.5 实体解析：把 name 精确匹配增强为包含匹配（方案 A）──────
//
// HA 内置 MCP 的 GetLiveContext.name 是「别名精确匹配」：查 "热水器" 只能拿到
// 开关实体，拿不到名为「热水器当前功率」的传感器。过去靠全量 {} 顺带捞到，
// 但全量约 9KB 会拖慢小智；把实体名枚举进提示词又不可维护。
// 这里在代理层用 HA 的全量上下文做一次运行时实体检索：
//   name 包含匹配 + area 包含 + domain 相等 → 只把相关实体块重拼返回。
// 模型照旧发 {"name":"热水器"}，一次拿到开关 + 功率 + 耗电量；
// 新增实体 / 改别名 / 换设备均自动生效，提示词无需写任何设备名。
const entityContextCache = new Map();

async function fetchEntityCatalog(up, key, cacheMs, rpcId) {
  if (cacheMs > 0) {
    const hit = entityContextCache.get(key);
    if (hit && Date.now() - hit.ts < cacheMs) return hit.body;
  }
  const resp = await postHA(
    up, 'tools/call',
    { name: 'homeassistant__GetLiveContext', arguments: {} },
    `${rpcId}-catalog`, 15000
  );
  const raw = (resp?.result?.content || []).map(p => p?.text || '').join('');
  let body = raw;
  try {
    const obj = JSON.parse(raw);
    body = typeof obj.result === 'string' ? obj.result : (obj.error || raw);
  } catch (e) { /* 非 JSON，按纯文本处理 */ }
  entityContextCache.set(key, { ts: Date.now(), body: String(body) });
  return String(body);
}

// ── 1.5b 全屋摘要（方案 A 补充）─────────────────────────────
//
// 无任何过滤条件的 GetLiveContext({}) 原本返回约 9KB / 84 个实体，
// 实测小智云端拿到后会生成不出回答（「报告全屋状态」无应答）。
// 这里按「控制类 + 关键传感器 + 每区域上限 + 去 attributes」压到约 2~3KB，
// 并附省略提示，引导模型按需二次查询指定房间/设备。
// 只影响 {} 查询；带 name/area/domain 的查询完全不受影响。
const CTRL_DOMAINS = new Set([
  'switch', 'light', 'climate', 'fan', 'media_player',
  'cover', 'lock', 'vacuum', 'humidifier'
]);

function stripAttributes(block) {
  return block.split('\n').filter(l =>
    !/^\s*attributes:/.test(l) &&
    !/^\s+(unit_of_measurement|device_class|friendly_name):/.test(l)
  ).join('\n').replace(/\n+$/, '');
}

// 人在/存在实体（回答「谁在家/家里有人吗」的核心依据）
function isPresenceBlock(b) {
  const dom = (b.match(/^\s*domain: (.*)$/m) || [])[1] || '';
  if (/^(person|device_tracker|zone)$/.test(dom)) return true;
  const name = (b.match(/^- names: (.*)$/m) || [])[1] || '';
  return /人在|occupancy|presence|有人|person|device_tracker/i.test(name);
}

// 门/窗接触（安防，也是人在的弱信号）
function isDoorBlock(b) {
  const name = (b.match(/^- names: (.*)$/m) || [])[1] || '';
  return /门|窗|Contact/i.test(name);
}

// 关键传感器（温度/功率/湿度/电量等）
function isKeySensorBlock(b) {
  const dom = (b.match(/^\s*domain: (.*)$/m) || [])[1] || '';
  if (dom !== 'sensor' && dom !== 'binary_sensor') return false;
  const cls = (b.match(/device_class: (.*)$/m) || [])[1] || '';
  const unit = (b.match(/unit_of_measurement: (.*)$/m) || [])[1] || '';
  const name = (b.match(/^- names: (.*)$/m) || [])[1] || '';
  return /temperature|power|humidity|occupancy|motion|battery|illuminance/i.test(cls) ||
         /W|%|lx|°C|°F/i.test(unit) ||
         /温度|功率|湿度|人在|电量|照度/.test(name);
}

// ── 1.5b 全屋摘要（方案 A 补充，v2.8.2 优化）─────────────────
// 无任何过滤条件的 GetLiveContext({}) 原本返回约 9KB / 84 个实体，小智拿到后生成不出回答；
// 早先版本按「控制类+关键传感器、每区域≤4」挑 33 个实体，是设备大杂烩，且人在类实体被
// 每区域上限卡掉（如客厅已有 4 条，把「客厅人在」挤掉），导致「家里有人吗」答非所问、乱报设备。
// 这里改为分层置顶：人在/存在 → 门禁 → 开启中的设备 → 关键传感器；人在/门禁豁免每区域上限，
// 始终置顶；并对同名实体去重。覆盖两类查询：① 完全无参数 GetLiveContext({}) → 全屋摘要；
// ② 只给 area、未给 name/domain（模型逐房间 fan-out 的常见形态）→ 该区域 presence 优先摘要。
// 带 name（实体解析）或带 domain（如灯/空调）的查询不受影响。
async function buildSummary(up, key, erCfg, cacheMs, rpcId, log, areaFilter = null) {
  const body = await fetchEntityCatalog(up, key, cacheMs, rpcId);
  const parts = body.split(/\n(?=- names:)/);
  const header = parts[0] || 'Live Context:';
  let allBlocks = parts.slice(1);
  if (allBlocks.length === 0) return null;

  // 仅给 area、未给 name/domain 时（模型逐房间 fan-out 的常见形态）：
  // 先把该区域实体按 area 过滤，再做 presence 优先摘要，避免带回整间房的设备大杂烩
  if (areaFilter) {
    const af = String(areaFilter).toLowerCase();
    allBlocks = allBlocks.filter(b => {
      const a = (b.match(/^\s*areas?: (.*)$/m) || [])[1] || '';
      return a.toLowerCase().includes(af);
    });
    if (allBlocks.length === 0) return null;
  }

  const perArea = Math.max(1, Number(erCfg.full_per_area) || 4);
  // 单区域摘要比全屋更短，更聚焦人在/门禁，避免模型再把设备罗列一遍
  const maxBytes = Math.max(800, Number(erCfg.full_max_bytes) || 2800) * (areaFilter ? 0.5 : 1);
  const areaOf = b => (b.match(/^\s*areas?: (.*)$/m) || [])[1] || '未分区';
  const stateOf = b => (b.match(/^\s*state: (.*)$/m) || [])[1] || '';
  const domOf = b => (b.match(/^\s*domain: (.*)$/m) || [])[1] || '';
  const nameOf = b => (b.match(/^- names: (.*)$/m) || [])[1] || '';
  const isOn = b => {
    const s = stateOf(b).replace(/'/g, '');
    return s === 'on' || s === 'playing' || s === 'open' ||
           s === 'heat' || s === 'cool' || s === 'auto' || s === 'fan_only';
  };

  // 去重：同一实体常在目录里被拆成多条 names 块（如「凉霸」「壁灯」），按首名+域去重
  const seen = new Set();
  const blocks = allBlocks.filter(b => {
    const k = nameOf(b).split(/[,，]/)[0].trim() + '|' + domOf(b);
    if (seen.has(k)) return false; seen.add(k); return true;
  });

  const tiers = [
    blocks.filter(isPresenceBlock),
    blocks.filter(b => isDoorBlock(b) && !isPresenceBlock(b)),
    blocks.filter(b => CTRL_DOMAINS.has(domOf(b)) && isOn(b) && !isPresenceBlock(b)),
    blocks.filter(b => isKeySensorBlock(b) && !isPresenceBlock(b)),
  ];

  const per = {};
  const picked = [];
  for (const tier of tiers) {
    for (const b of tier) {
      const a = areaOf(b);
      const exempt = isPresenceBlock(b) || isDoorBlock(b);
      per[a] = (per[a] || 0) + 1;
      if (exempt || per[a] <= perArea) picked.push(b);
    }
  }

  // 字节上限兜底：优先裁剪非置顶层（控制设备/传感器）
  let out = header + '\n' + picked.map(stripAttributes).join('\n');
  while (out.length > maxBytes && picked.length > tiers[0].length + tiers[1].length + 2) {
    let idx = -1;
    for (let i = picked.length - 1; i >= 0; i--) {
      if (!(isPresenceBlock(picked[i]) || isDoorBlock(picked[i]))) { idx = i; break; }
    }
    if (idx < 0) break;
    picked.splice(idx, 1);
    out = header + '\n' + picked.map(stripAttributes).join('\n');
  }

  const scope = areaFilter ? `（${areaFilter}）` : '全屋';
  out += `\n(已省略 ${blocks.length - picked.length} 个次要实体；指定设备可查完整详情)`;
  if (tiers[0].length) {
    out += `\n注：本列表为${scope}摘要；问"谁在家/家里有人吗"应聚焦 人在/occupancy 类实体，勿复述无关设备。`;
  }
  const scopeLabel = areaFilter ? `摘要(${areaFilter})` : '全屋摘要';
  log?.(`🔎 ${scopeLabel}: ${picked.length}/${blocks.length} 个实体, ${out.length}B (原始 ${body.length}B)`);
  return { content: [{ type: 'text', text: JSON.stringify({ result: out }) }], isError: false };
}

// ── 1.5c 控制类工具的名称失配候选回传（方案 A 补充）──────────
//
// 口语总称常不是 HA 暴露名：说「客厅灯」，HA 里实际叫「大灯/射灯/灯带/筒灯组」，
// 精确匹配必然报 No entities matched the name。模型随后常退化成
// {"area":"客厅","domain":["light"]} 把整间房的灯全关掉，违反「只操作主设备」的意图。
// 这里在工具返回名称错误时，用运行时实体目录把总称拆成「区域 + 设备词」，
// 把匹配到的真实实体名附在错误文本里回给模型，指导它用准确 name 重试。
// 只改错误文本；不自动重试、不主动调用任何控制工具，零误控风险。
async function buildNameCandidates(up, key, erCfg, badName, rpcId) {
  const cacheMs = Math.max(0, Number(erCfg.cache_ms) || 0);
  const body = await fetchEntityCatalog(up, key, cacheMs, `${rpcId}-cand`);
  const blocks = body.split(/\n(?=- names:)/).slice(1);
  const nameOf = b => (b.match(/^- names: (.*)$/m) || [])[1] || '';
  const areaOf = b => (b.match(/^\s*areas: (.*)$/m) || [])[1] || '';

  const areas = [...new Set(
    blocks.map(areaOf).filter(Boolean)
      .flatMap(a => a.split(/[,，]/)).map(a => a.trim()).filter(Boolean)
  )].sort((a, b) => b.length - a.length);

  let area = '', keyword = badName;
  for (const a of areas) {
    if (a && badName.startsWith(a) && badName.length > a.length) {
      area = a;
      keyword = badName.slice(a.length).trim();
      break;
    }
  }
  if (!keyword) keyword = badName;

  const kw = keyword.toLowerCase();
  const match = (b, useArea) =>
    (!useArea || !area || areaOf(b).includes(area)) &&
    nameOf(b).toLowerCase().includes(kw);
  let cand = blocks.filter(b => match(b, true));
  if (cand.length === 0 && area) cand = blocks.filter(b => match(b, false));
  const names = [...new Set(cand.map(nameOf))].filter(Boolean).slice(0, 8);
  return { names, area, keyword };
}

async function enhanceNameError(up, key, erCfg, result, rpcId, log) {
  if (!erCfg || erCfg.control_hints === false) return result;
  if (!result || result.isError !== true) return result;
  const item = (result.content || [])[0];
  if (!item || typeof item.text !== 'string') return result;
  const m = item.text.match(/No entities matched the name \(given name '([^']+)'\)/);
  if (!m) return result;
  const bad = m[1];
  try {
    const { names, area, keyword } = await buildNameCandidates(up, key, erCfg, bad, rpcId);
    if (!names.length) return result;
    const hint = `\n\n[可用实体] ${area ? area + ' 下' : ''}与「${keyword}」匹配的实体：${names.join('、')}。` +
      `\n可用准确的 name 操作单个设备；也可用 area+domain 操作该区域全部同类设备。` +
      `\n具体选哪种按用户指令判断（例如「关某区域的灯」通常指全部，「开灯」按用户习惯只开主灯）。`;
    log?.(`🔎 名称候选: "${bad}" → ${names.join(', ')}`);
    return {
      ...result,
      content: [{ ...item, text: item.text + hint }, ...result.content.slice(1)]
    };
  } catch (e) {
    return result;  // 目录不可用时不改变原错误
  }
}

async function expandEntityQuery(up, key, erCfg, callArgs, rpcId, log) {
  if (!erCfg || erCfg.enabled === false) return null;
  const rawName = callArgs.name;
  const kw = (typeof rawName === 'string' ? rawName
    : Array.isArray(rawName) ? String(rawName[0] || '') : '').trim();
  const cacheMs = Math.max(0, Number(erCfg.cache_ms) || 0);
  const maxResults = Math.max(1, Number(erCfg.max_results) || 12);
  const area = (typeof callArgs.area === 'string' ? callArgs.area : '').trim().toLowerCase();
  const domRaw = callArgs.domain;
  const domList = (Array.isArray(domRaw) ? domRaw : (domRaw ? [domRaw] : []))
    .map(d => String(d).trim()).filter(Boolean);

  // 无任何过滤条件 → 全屋摘要，避免 9KB 原文让小智生成不出回答
  if (!kw && !area && !domList.length && erCfg.summarize_full !== false) {
    return await buildSummary(up, key, erCfg, cacheMs, rpcId, log);
  }
  // 只给了 area、未给 name/domain → 该区域 presence 优先摘要
  // （解决逐房间 fan-out 时，每个 area 查询都带回整间房设备、最终答非所问的问题）
  if (!kw && area && !domList.length && erCfg.summarize_area !== false) {
    return await buildSummary(up, key, erCfg, cacheMs, rpcId, log, area);
  }
  if (!kw) return null;  // 只有 domain 时保持上游原语义

  const body = await fetchEntityCatalog(up, key, cacheMs, rpcId);
  const parts = body.split(/\n(?=- names:)/);
  const header = parts[0] || 'Live Context:';
  const blocks = parts.slice(1);
  if (blocks.length === 0) return null;

  const nameOf = b => (b.match(/^- names: (.*)$/m) || [])[1] || '';
  const areaOf = b => (b.match(/^\s*areas: (.*)$/m) || [])[1] || '';
  const domOf = b => (b.match(/^\s*domain: (.*)$/m) || [])[1] || '';
  const k = kw.toLowerCase();

  let selected = blocks.filter(b =>
    nameOf(b).toLowerCase().includes(k) &&
    (!area || areaOf(b).toLowerCase().includes(area)) &&
    (!domList.length || domList.includes(domOf(b)))
  );
  let note = '';
  if (selected.length === 0 && (area || domList.length)) {
    // 渐进放宽：小智常猜错 domain（如 water_heater），先丢 domain 再丢 area
    selected = blocks.filter(b => nameOf(b).toLowerCase().includes(k) &&
      (!area || areaOf(b).toLowerCase().includes(area)));
    if (selected.length) note = '忽略无效 domain';
    else {
      selected = blocks.filter(b => nameOf(b).toLowerCase().includes(k));
      if (selected.length) note = '忽略无效 domain/area';
    }
  }
  if (selected.length === 0) return null;  // 完全无匹配 → 回退上游原调用

  const truncated = selected.length > maxResults;
  const picked = selected.slice(0, maxResults);
  const out = header + '\n' + picked.join('\n');
  log?.(`🔎 实体解析: "${kw}" → ${picked.length}/${blocks.length} 个实体, ${out.length}B` +
        (note ? ` (${note})` : '') + (truncated ? ' [已截断]' : ''));
  return { content: [{ type: 'text', text: JSON.stringify({ result: out }) }], isError: false };
}

// ── 1. 初始化所有 upstream ────────────────────────────────
async function initUpstreams() {
  for (const [name, cfg] of Object.entries(config.upstreams || {})) {
    await initOneUpstream(name, cfg);
  }
}

async function initOneUpstream(name, cfg) {
  try {
    if (cfg.type === 'streamable-http') {
      const interpolatedUrl = interpolateEnv(cfg.url);
      const interpolatedHeaders = {};
      for (const [key, value] of Object.entries(cfg.headers || {})) {
        interpolatedHeaders[key] = interpolateEnv(value);
      }
      // 可选：初始化 tools/list 失败时短超时重试（默认 1 次，保持原有行为）。
      // HA 2026.11 内置 MCP 偶发首包挂起，配置 init_retries/init_timeout_ms 后
      // 可在启动期快速重试，避免每次重启都等 60s 的 upstream 自愈窗口。
      const initRetries = Math.max(1, Number(cfg.init_retries) || 1);
      const initTimeoutMs = Number(cfg.init_timeout_ms) || 15000;
      let listResp = null;
      let lastErr;
      for (let attempt = 1; attempt <= initRetries; attempt++) {
        try {
          listResp = await postHA(
            { url: interpolatedUrl, headers: interpolatedHeaders },
            'tools/list',
            {},
            `init-list-${attempt}`,
            initTimeoutMs
          );
          break;
        } catch (err) {
          lastErr = err;
          if (attempt < initRetries) {
            console.warn(`⚠️  [${name}] tools/list 第 ${attempt} 次失败，2s 后重试: ${maskUrl(err.message)}`);
            await new Promise(r => setTimeout(r, 2000));
          }
        }
      }
      if (!listResp) throw lastErr;
      const tools = listResp.result?.tools || [];
      upstreams.set(name, { type: 'streamable-http', url: interpolatedUrl, headers: interpolatedHeaders, tools });
      console.log(`✅ [${name}] streamable-http: ${tools.length} 个工具`);
      if (cfg.entity_resolve && cfg.entity_resolve.enabled !== false) {
        console.log(`🔎 [${name}] 实体解析已启用 (max_results=${cfg.entity_resolve.max_results || 12}, cache_ms=${cfg.entity_resolve.cache_ms || 0})`);
      }
    } else if (cfg.type === 'stdio') {
      const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
      const transportOpts = {
        command: cfg.command,
        args: cfg.args || [],
        env: { ...process.env, ...(cfg.env || {}) }
      };
      if (cfg.cwd) {
        transportOpts.cwd = cfg.cwd;
      }
      const transport = new StdioClientTransport(transportOpts);
      const client = new Client(
        { name: `mini-${name}`, version: VERSION },
        { capabilities: {} }
      );
      await client.connect(transport);
      const result = await client.listTools();
      const tools = result.tools || [];
      upstreams.set(name, { type: 'stdio', client, transport, tools });
      console.log(`✅ [${name}] stdio: ${tools.length} 个工具 (PID: ${transport._process?.pid})`);
    } else {
      console.warn(`⚠️  [${name}] 未知类型: ${maskUrl(cfg.type)}，跳过`);
    }
  } catch (err) {
    console.error(`❌ [${name}] 初始化失败: ${maskUrl(err)}`);
    upstreams.set(name, { type: cfg.type, tools: [], error: maskUrl(err.message) });
  }
}

// ── 1.5 upstream 失败自动重连 ────────────────────────────
// 每 60 秒检查一次，对初始化失败的 upstream 尝试重新连接
const UPSTREAM_RETRY_INTERVAL = 60000;
let upstreamRetryTimer = null;

function startUpstreamRetry() {
  if (upstreamRetryTimer) return;
  upstreamRetryTimer = setInterval(async () => {
    const failedNames = [];
    for (const [name, up] of upstreams) {
      if (up.error) {
        failedNames.push(name);
      }
    }
    if (failedNames.length === 0) return;

    console.log(`🔄 检查 ${failedNames.length} 个失败的 upstream...`);
    let anySuccess = false;
    for (const name of failedNames) {
      const cfg = config.upstreams?.[name];
      if (!cfg) continue;
      try {
        // 先关闭旧的可能残留的连接
        const oldUp = upstreams.get(name);
        if (oldUp?.type === 'stdio' && oldUp?.transport) {
          try { oldUp.transport.close(); } catch (_) { /* ignore */ }
        }
        upstreams.delete(name);
        await initOneUpstream(name, cfg);
        const newUp = upstreams.get(name);
        if (newUp && !newUp.error) {
          console.log(`🎉 [${name}] 重连成功！`);
          anySuccess = true;
        }
      } catch (err) {
        console.error(`❌ [${name}] 重连仍失败: ${maskUrl(err.message)}`);
        upstreams.set(name, { type: cfg.type, tools: [], error: maskUrl(err.message) });
      }
    }
    // 有 upstream 重连成功时，重新建立小智连接以刷新工具列表。
    // 小智服务端在连接保持期间不会响应 notifications/tools/list_changed
    // 重新拉取 tools/list，只在新连接建立时才拉取，因此需要软重连。
    // 消息队列会缓存断连期间的消息，不会丢失。
    if (anySuccess) {
      for (const conn of xiaozhiConnections) {
        try {
          console.log(`[${conn.name}] 🔄 upstream 工具列表变更，重新连接以刷新`);
          conn.connect();
        } catch (err) {
          console.error(`[${conn.name}] ❌ 重新连接失败:`, maskUrl(err.message));
        }
      }
    }
  }, UPSTREAM_RETRY_INTERVAL);
}

// ── 2. 聚合工具列表（加前缀）─────────────────────────────
//
// 工具名路由表：exposedName -> { upstream, realName }
// 不再靠 indexOf('_') 猜前缀，避免 upstream 名含 - 或 _ 时解析错误，
// 同时把工具名规范化为 [A-Za-z0-9_]，兼容只接受该字符集的小智后端
// （例如 upstream 叫 ha-climate 时，暴露为 ha_climate_xxx）
const toolRoutes = new Map();

function aggregateTools() {
  const all = [];
  const MAX_DESC_LEN = 300;
  const MAX_PAYLOAD_SIZE = config.xiaozhi?.max_payload_size || 40000;

  toolRoutes.clear();

  for (const [name, up] of upstreams) {
    if (up.error) continue;

    // 可选的工具过滤：include（白名单，按上游原始工具名）/ exclude（黑名单）
    // ha-mcp 新版本有 77+ 个工具（约 176KB），远超小智 40KB 上限，
    // 必须用 include 只保留语音控制需要的核心工具，否则截断会丢掉关键工具
    const cfg = config.upstreams?.[name] || {};
    const include = Array.isArray(cfg.include) && cfg.include.length > 0 ? new Set(cfg.include) : null;
    const exclude = Array.isArray(cfg.exclude) && cfg.exclude.length > 0 ? new Set(cfg.exclude) : null;
    let tools = up.tools;
    if (include) {
      tools = tools.filter(t => include.has(t.name));
      const matched = new Set(tools.map(t => t.name));
      const missed = [...include].filter(n => !matched.has(n));
      if (missed.length > 0) {
        console.warn(`⚠️  [${name}] include 中这些工具在上游不存在: ${missed.join(', ')}`);
      }
      if (tools.length === 0) {
        console.warn(`⚠️  [${name}] include 过滤后 0 个工具！回退为全部工具。可用工具: ${up.tools.map(t => t.name).join(', ')}`);
        tools = up.tools;
      }
    } else if (exclude) {
      tools = tools.filter(t => !exclude.has(t.name));
    }
    if (include || exclude) {
      console.log(`🔍 [${name}] 工具过滤: ${tools.length}/${up.tools.length} 个`);
    }

    // 每个 upstream 可单独设置描述截断长度，工具多/描述长时压缩以腾出 payload 空间
    const maxDescLen = cfg.max_desc_len || MAX_DESC_LEN;

    for (const t of tools) {
      let desc = t.description || '';

      if (name === 'memory') {
        const memoryDescMap = {
          'create_entities': '创建记忆：当用户说"记住"、"记录"、"帮我记着"时，把信息保存到长期记忆',
          'add_observations': '补充记忆：给已有记忆添加新细节',
          'read_graph': '读取全部记忆：当用户问"你知道我什么"、"我是谁"时使用',
          'search_nodes': '搜索记忆：用关键词查找已记录的信息',
          'open_nodes': '打开记忆详情：查看某个具体记忆的完整内容',
          'delete_entities': '删除记忆：用户要求"删掉"、"忘掉"某条记录',
          'delete_observations': '删除记忆中的某条细节',
          'delete_relations': '删除记忆中的关联关系'
        };
        desc = memoryDescMap[t.name] || desc;
      }

      if (desc.length > maxDescLen) {
        desc = desc.substring(0, maxDescLen);
      }

      // 暴露给小智的工具名：规范化为 [A-Za-z0-9_]，重名时追加序号
      const rawName = `${name}_${t.name}`;
      let exposedName = rawName.replace(/[^A-Za-z0-9_]/g, '_');
      if (exposedName !== rawName) {
        console.log(`🔤 工具名规范化: ${rawName} → ${exposedName}`);
      }
      let dup = 1;
      while (toolRoutes.has(exposedName)) {
        exposedName = `${rawName.replace(/[^A-Za-z0-9_]/g, '_')}_${dup++}`;
      }

      const tool = {
        name: exposedName,
        description: `[${name}] ${desc}`,
        inputSchema: simplifyAnyOfSchema(t.inputSchema)
      };

      const testPayload = { jsonrpc: '2.0', result: { tools: [...all, tool] }, id: 1 };
      const testSize = JSON.stringify(testPayload).length;
      if (testSize > MAX_PAYLOAD_SIZE) {
        const remaining = tools.slice(tools.indexOf(t)).map(x => `${name}_${x.name}`);
        const restUpstreams = [...upstreams.keys()].slice([...upstreams.keys()].indexOf(name) + 1);
        console.warn(`⚠️  payload 接近上限(${MAX_PAYLOAD_SIZE})，已截断到 ${all.length} 个工具`);
        console.warn(`⚠️  被丢弃的工具: ${[...remaining, ...restUpstreams.map(u => `${u}_*`)].join(', ')}`);
        console.warn(`💡 建议在 config.yaml 的 upstreams.${name} 下配置 include 列表，只保留需要的工具`);
        return all;
      }

      all.push(tool);
      toolRoutes.set(exposedName, { upstream: name, realName: t.name });
    }
  }
  return all;
}

// ── 3. XiaozhiConnection 类：每个 token 一个实例 ──────────
//
// 连接优化策略（参考 mcphub 及其他小智生态项目）：
// 1. WebSocket 协议层 Ping/Pong 心跳保活（默认开启）
// 2. 可选 listTools 应用层保活（兼容不支持 ping 的服务端）
// 3. 指数退避 + 随机抖动重连，避免惊群效应
// 4. 消息队列：断开期间缓存消息，重连后发送
// 5. 1006 空闲超时使用更短的延迟快速重连
//
// upstreams Map 全局共享：所有连接看到的工具列表一致；
// 工具调用时哪个连接收到请求，就用哪个连接回复。

class XiaozhiConnection {
  constructor(name, url, cfg) {
    this.name = name;       // 连接名称，如 "xiaozhi-1"
    this.url = url;         // 完整 wss URL（含 token）
    this.ws = null;
    this.isClosed = false;
    this.reconnectTimer = null;
    this.messageQueue = [];

    this.quickReconnectAttempts = 0;
    this.isInInfiniteReconnectMode = false;
    this.infiniteRetryCount = 0;
    this.isInSleepMode = false;

    // ── 连接健康检查状态 ──────────────────────────────────
    // lastMessageTime: 最后一次收到任何消息（含服务端 ping / pong）的时间
    // lastToolsRefreshTime: 最后一次主动重推工具列表的时间
    // healthCheckTimer: 定时器，周期性检测连接存活 + 重推工具
    this.lastMessageTime = Date.now();
    this.lastToolsRefreshTime = Date.now();
    this.healthCheckTimer = null;

    // 重连参数（从全局 config 读取，所有连接共享同一套策略）
    this.reconnectDelay = cfg.reconnect_delay || 2000;
    this.reconnectMaxDelay = cfg.reconnect_max_delay || 60000;
    this.messageQueueMaxSize = cfg.message_queue_max || 100;
    this.aggressiveReconnect = cfg.aggressive_reconnect !== false;
    this.maxQuickReconnect = cfg.max_quick_reconnect || 10;
    this.infiniteReconnectDelay = cfg.infinite_reconnect_delay || 1800000;
    this.maxInfiniteRetries = cfg.max_infinite_retries || 48;
    this.sleepThreshold = cfg.sleep_threshold || 12;
    this.sleepInterval = cfg.sleep_interval || 7200000;

    // 健康检查参数
    this.healthCheckInterval = cfg.health_check_interval || 60000;
    this.staleThreshold = cfg.stale_threshold || 300000;
    this.toolsRefreshInterval = cfg.tools_refresh_interval || 1800000;
  }

  log(...args) {
    console.log(`[${this.name}]`, ...args);
  }

  enqueueMessage(data) {
    if (this.messageQueue.length >= this.messageQueueMaxSize) {
      const dropped = this.messageQueue.shift();
      this.log(`⚠️  消息队列已满，丢弃最旧消息: ${dropped?.id || 'unknown'}`);
    }
    this.messageQueue.push(data);
  }

  flushMessageQueue() {
    if (this.messageQueue.length === 0) return;
    this.log(`📤 重连后发送 ${this.messageQueue.length} 条缓存消息`);
    while (this.messageQueue.length > 0) {
      const msg = this.messageQueue.shift();
      if (!this.wsSendImmediate(msg)) {
        this.messageQueue.unshift(msg);
        break;
      }
    }
  }

  wsSendImmediate(data) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return false;
    }
    try {
      this.ws.send(JSON.stringify(data));
      return true;
    } catch (err) {
      this.log('❌ WebSocket 发送失败:', maskUrl(err.message));
      return false;
    }
  }

  wsSend(data, { queue = true } = {}) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return this.wsSendImmediate(data);
    }
    if (queue) {
      this.enqueueMessage(data);
      this.log('📥 连接未就绪，消息已加入队列');
      return false;
    }
    this.log('⚠️  WebSocket 未连接，丢弃消息');
    return false;
  }

  cleanupWs() {
    if (this.ws) {
      try {
        this.ws.removeAllListeners();
        this.ws.close();
      } catch (_) { /* ignore */ }
      this.ws = null;
    }
  }

  jitter(base) {
    return Math.floor(base * (0.8 + Math.random() * 0.4));
  }

  scheduleReconnect() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.aggressiveReconnect) {
      const delay = this.jitter(this.reconnectDelay);
      this.log(`🔄 快速重连模式，${Math.round(delay / 1000)}s 后重连...`);
      this.reconnectTimer = setTimeout(() => {
        try {
          this.connect();
        } catch (error) {
          this.log('❌ 快速重连失败:', maskUrl(error.message));
          this.scheduleReconnect();
        }
      }, delay);
      return;
    }

    if (this.quickReconnectAttempts >= this.maxQuickReconnect) {
      if (!this.isInInfiniteReconnectMode) {
        this.isInInfiniteReconnectMode = true;
        this.log(`🔄 快速重连次数已达上限(${this.maxQuickReconnect})，进入无限重连模式`);
      }
      this.scheduleInfiniteReconnect();
      return;
    }

    const delay = Math.min(
      this.reconnectDelay * Math.pow(2, this.quickReconnectAttempts),
      this.reconnectMaxDelay
    );
    this.log(`🔄 第 ${this.quickReconnectAttempts + 1}/${this.maxQuickReconnect} 次快速重连，${Math.round(delay / 1000)}s 后重试...`);
    this.reconnectTimer = setTimeout(() => {
      this.quickReconnectAttempts++;
      try {
        this.connect();
      } catch (error) {
        this.log('❌ 快速重连失败:', maskUrl(error.message));
        this.scheduleReconnect();
      }
    }, this.jitter(delay));
  }

  scheduleInfiniteReconnect() {
    this.infiniteRetryCount++;

    if (this.maxInfiniteRetries > 0 && this.infiniteRetryCount > this.maxInfiniteRetries) {
      this.log(`⏹️  已达到最大无限重连次数(${this.maxInfiniteRetries})，停止重连`);
      return;
    }

    let delay;
    if (this.infiniteRetryCount >= this.sleepThreshold && !this.isInSleepMode) {
      this.isInSleepMode = true;
      this.log(`😴 连续失败 ${this.sleepThreshold} 次，进入休眠模式`);
    }

    if (this.isInSleepMode) {
      delay = this.sleepInterval;
      this.log(`😴 休眠模式，${Math.round(delay / 60000)} 分钟后重连（第 ${this.infiniteRetryCount} 次）`);
    } else {
      delay = this.infiniteReconnectDelay;
      this.log(`🔄 无限重连，${Math.round(delay / 60000)} 分钟后重试（第 ${this.infiniteRetryCount}/${this.maxInfiniteRetries || '∞'} 次）`);
    }

    this.reconnectTimer = setTimeout(() => {
      this.log(`🔄 进行无限重连尝试（第 ${this.infiniteRetryCount}/${this.maxInfiniteRetries || '∞'} 次）...`);
      try {
        this.connect();
      } catch (error) {
        this.log('❌ 无限重连失败:', maskUrl(error.message));
        this.scheduleInfiniteReconnect();
      }
    }, this.jitter(delay));
  }

  handleIncomingMessage(raw) {
    // 更新最后消息时间，用于连接健康检查（静默断连检测）
    this.lastMessageTime = Date.now();

    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return; }

    if (msg.method === 'initialize') {
      this.wsSend({
        jsonrpc: '2.0',
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'xiaozhi-mini', version: VERSION }
        },
        id: msg.id
      });
      return;
    }

    // MCP 协议初始化完成通知，静默处理
    if (msg.method === 'notifications/initialized') {
      return;
    }

    // 小智服务端 ping 保活探测，必须回复 pong（空 result）
    // 不回复会导致服务端在30秒后断开连接（1006）
    if (msg.method === 'ping') {
      this.wsSend({ jsonrpc: '2.0', result: {}, id: msg.id });
      return;
    }

    if (msg.method === 'tools/list') {
      const tools = aggregateTools();
      const payload = { jsonrpc: '2.0', result: { tools }, id: msg.id };
      const payloadSize = JSON.stringify(payload).length;
      const breakdown = {};
      for (const t of tools) {
        const p = toolRoutes.get(t.name)?.upstream || t.name;
        breakdown[p] = (breakdown[p] || 0) + 1;
      }
      const detail = Object.entries(breakdown).map(([k, v]) => `${k}:${v}`).join(' ');
      this.log(`📤 推送 ${tools.length} 个工具给小智 (payload: ${Math.round(payloadSize / 1024)}KB) [${detail}]`);
      if (payloadSize > 100000) {
        this.log(`⚠️  payload 过大 (${payloadSize} bytes)，可能导致连接断开`);
      }
      this.wsSend(payload);
      return;
    }

    if (msg.method === 'tools/call') {
      const toolName = msg.params?.name || '';
      this.log(`📨 收到工具调用请求: ${toolName}`, JSON.stringify(msg.params?.arguments || {}));

      // 优先查路由表（支持含 - / _ 的 upstream 名，以及规范化后的工具名）
      let prefix;
      let realName;
      const route = toolRoutes.get(toolName);
      if (route) {
        prefix = route.upstream;
        realName = route.realName;
      } else {
        // 兼容兜底：按第一个下划线拆前缀
        const underscoreIdx = toolName.indexOf('_');
        if (underscoreIdx <= 0) {
          this.wsSend({ jsonrpc: '2.0', error: { code: -32602, message: `invalid tool name: ${toolName}` }, id: msg.id });
          return;
        }
        prefix = toolName.substring(0, underscoreIdx);
        realName = toolName.substring(underscoreIdx + 1);
      }
      const up = upstreams.get(prefix);

      if (!up) {
        this.wsSend({ jsonrpc: '2.0', error: { code: -32601, message: `unknown upstream: ${prefix}` }, id: msg.id });
        return;
      }

      // 可选：为指定工具注入默认调用参数（仅在小智未传任何参数时生效）
      // 依据：HA 2026.11 内置 MCP 的 GetLiveContext 不带过滤参数会拉全屋状态并超时
      const userArgs = msg.params?.arguments || {};
      const callDefaults = config.upstreams?.[prefix]?.call_defaults?.[realName];
      const callArgs = (callDefaults && Object.keys(userArgs).length === 0)
        ? { ...callDefaults }
        : userArgs;

      // 还原小智云端把数组参数字符串化的形态（"[...]" → [...]）
      const finalArgs = restoreStringifiedArrays(callArgs, s => this.log(s));

      // 哪个连接收到请求，就用这个连接回复（捕获 this）
      (async () => {
        try {
          let result;
          if (up.type === 'streamable-http') {
            const erCfg = config.upstreams?.[prefix]?.entity_resolve;
            let expanded = null;
            if (erCfg && erCfg.enabled !== false &&
                realName === 'homeassistant__GetLiveContext') {
              try {
                expanded = await expandEntityQuery(
                  up, prefix, erCfg, finalArgs, msg.id, s => this.log(s)
                );
              } catch (err) {
                this.log(`⚠️  实体解析失败，回退原调用: ${maskUrl(err.message)}`);
              }
            }
            if (expanded) {
              result = expanded;
            } else {
              const response = await postHA(
                up, 'tools/call', { name: realName, arguments: finalArgs },
                msg.id, 60000
              );
              result = response.result;
              // 名称失配时把真实实体候选回给模型（只改错误文本，不自动控制）
              const erCfgHint = config.upstreams?.[prefix]?.entity_resolve;
              if (erCfgHint) {
                result = await enhanceNameError(
                  up, prefix, erCfgHint, result, msg.id, s => this.log(s)
                );
              }
            }
          } else if (up.type === 'stdio') {
            result = await up.client.callTool({ name: realName, arguments: finalArgs });
          }
          // 统一治理工具结果体量（错误结果保持原样）
          if (up.type === 'streamable-http' && !result?.isError) {
            result = compactToolResult(result, config, s => this.log(s));
          }
          const errText = toolErrorOf(result);
          this.wsSend({ jsonrpc: '2.0', result, id: msg.id });
          if (errText) this.log(`⚠️  ${toolName} → error: ${errText}`);
          else this.log(`🔧 ${toolName} → ok`);
        } catch (err) {
          this.log(`❌ ${toolName} 调用失败:`, maskUrl(err));
          this.wsSend({ jsonrpc: '2.0', error: { code: -32000, message: maskUrl(err.message) }, id: msg.id });
        }
      })();
      return;
    }

    if (msg.id && String(msg.id).startsWith('keepalive-')) {
      this.log('💓 保活响应正常');
      return;
    }

    this.log(`❓ 未知请求: method=${msg.method}`, JSON.stringify(msg).substring(0, 200));
  }

  // ── 连接健康检查 ──────────────────────────────────────
  // 解决"静默断连"问题：WebSocket TCP 连接在中间网络设备（NAT/代理/负载
  // 均衡）处被静默回收时，ws 库不触发 close 事件，客户端误以为连接存活，
  // 导致工具列表不再推送。
  //
  // 三重防护：
  // 1. 静默检测：超过 staleThreshold 未收到任何消息（含服务端 ping）→ 强制重连
  // 2. WebSocket ping：周期性发送底层 ping 帧，pong 响应更新 lastMessageTime
  // 3. 工具重推：每隔 toolsRefreshInterval 重新通知工具列表更新
  startHealthCheck() {
    if (this.healthCheckTimer) return;
    this.healthCheckTimer = setInterval(() => {
      if (this.isClosed || !this.ws) return;
      if (this.ws.readyState !== WebSocket.OPEN) return;

      const now = Date.now();
      const idleTime = now - this.lastMessageTime;

      // 1. 静默检测：连接长时间无任何消息 → 疑似半开/僵尸连接
      if (idleTime > this.staleThreshold) {
        this.log(
          `💀 连接静默 ${Math.round(idleTime / 1000)}s 超过阈值 ${Math.round(this.staleThreshold / 1000)}s，` +
          `疑似半开连接，强制重连`
        );
        this.forceReconnect();
        return;
      }

      // 2. WebSocket 底层 ping：探测 TCP 连接是否真正可用
      try {
        this.ws.ping();
      } catch (e) {
        this.log('⚠️  WebSocket ping 失败，强制重连:', maskUrl(e.message));
        this.forceReconnect();
        return;
      }

      // 3. 定期重推工具列表：防止小智后台丢失工具后无法恢复
      if (now - this.lastToolsRefreshTime >= this.toolsRefreshInterval) {
        this.lastToolsRefreshTime = now;
        try {
          const notification = {
            jsonrpc: '2.0',
            method: 'notifications/tools/list_changed',
          };
          this.ws.send(JSON.stringify(notification));
          this.log('📢 周期性重新通知工具列表更新');
        } catch (e) {
          this.log('⚠️  周期性工具通知失败:', maskUrl(e.message));
        }
      }
    }, this.healthCheckInterval);
  }

  // 强制重连：跳过 close 事件，直接清理并重新连接
  forceReconnect() {
    this.log('🔄 检测到连接异常，强制重连...');
    // connect() 内部会调用 cleanupWs() 清理旧连接，
    // removeAllListeners 确保 close 事件不会触发二次重连
    try {
      this.connect();
    } catch (err) {
      this.log('❌ 强制重连失败:', maskUrl(err.message));
      this.scheduleReconnect();
    }
  }

  connect() {
    this.isClosed = false;
    this.cleanupWs();
    this.log(`🔗 连接小智: ${maskUrl(this.url)}`);

    this.ws = new WebSocket(this.url, {
      perMessageDeflate: false,
      handshakeTimeout: 10000,
    });

    this.ws.on('open', () => {
      this.log('🟢 小智 WebSocket 已连接');
      this.quickReconnectAttempts = 0;
      this.isInInfiniteReconnectMode = false;
      this.infiniteRetryCount = 0;
      this.isInSleepMode = false;

      // 重置健康检查时间戳：连接刚建立，从零开始计时
      this.lastMessageTime = Date.now();
      this.lastToolsRefreshTime = Date.now();

      // 连接建立后必须通知工具列表更新，否则小智不会主动拉取
      try {
        const notification = {
          jsonrpc: '2.0',
          method: 'notifications/tools/list_changed',
        };
        this.ws.send(JSON.stringify(notification));
        this.log('📢 已通知小智工具列表更新');
      } catch (e) {
        this.log('⚠️  通知工具列表更新失败:', maskUrl(e.message));
      }

      this.flushMessageQueue();
    });

    this.ws.on('message', (raw) => this.handleIncomingMessage(raw));

    // WebSocket 底层 pong 响应：确认 TCP 连接双向可达
    this.ws.on('pong', () => {
      this.lastMessageTime = Date.now();
    });

    this.ws.on('close', () => {
      if (this.isClosed) return;
      this.isClosed = true;
      this.log('🔴 小智连接断开');
      this.scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      this.log('❌ WebSocket 错误:', maskUrl(err));
    });

    // 启动健康检查（幂等，已启动则跳过）
    this.startHealthCheck();
  }

  cleanup() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }
    this.isClosed = true;
    this.cleanupWs();
  }
}

// ── 组装小智连接列表 ─────────────────────────────────────
// 所有 URL 都是从小智官方后台复制的完整 wss 链接（含 token），
// 直接放在 .env 里，config.yaml 用 ${VAR} 引用，无需手动拆分 token。
//
// 优先级：
// 1. config.xiaozhi.urls（数组，多个完整 URL，每项支持 ${ENV} 插值）
// 2. config.xiaozhi.url（单个完整 URL，支持 ${ENV} 插值）
function buildXiaozhiConnections() {
  const connCfg = {
    reconnect_delay: RECONNECT_DELAY,
    reconnect_max_delay: RECONNECT_MAX_DELAY,
    message_queue_max: MESSAGE_QUEUE_MAX_SIZE,
    aggressive_reconnect: AGGRESSIVE_RECONNECT,
    max_quick_reconnect: MAX_QUICK_RECONNECT,
    infinite_reconnect_delay: INFINITE_RECONNECT_DELAY,
    max_infinite_retries: MAX_INFINITE_RETRIES,
    sleep_threshold: SLEEP_THRESHOLD,
    sleep_interval: SLEEP_INTERVAL,
    health_check_interval: HEALTH_CHECK_INTERVAL,
    stale_threshold: STALE_THRESHOLD,
    tools_refresh_interval: TOOLS_REFRESH_INTERVAL,
  };

  const connections = [];
  const isPlaceholder = (v) => !v || v.includes('REPLACE_WITH_YOUR_TOKEN') || v.includes('YOUR_TOKEN');

  // 1. 多 URL 模式（多个小智接入）
  const rawUrls = config.xiaozhi?.urls;
  if (Array.isArray(rawUrls) && rawUrls.length > 0) {
    rawUrls.forEach((rawUrl, idx) => {
      const url = interpolateEnv(rawUrl);
      if (isPlaceholder(url)) {
        console.warn(`⚠️  第 ${idx + 1} 个 url 为空或占位符，跳过`);
        return;
      }
      if (!url.startsWith('wss://') && !url.startsWith('ws://')) {
        console.warn(`⚠️  第 ${idx + 1} 个 url 不是有效的 ws/wss 链接，跳过`);
        return;
      }
      connections.push(new XiaozhiConnection(`xiaozhi-${idx + 1}`, url, connCfg));
    });
    if (connections.length > 0) return connections;
    console.error('❌ config.xiaozhi.urls 配置了但没有有效 url');
  }

  // 2. 单 URL 模式（从 .env 读取完整 wss URL）
  const cfgUrl = interpolateEnv(config.xiaozhi?.url);
  if (cfgUrl && !isPlaceholder(cfgUrl) &&
      (cfgUrl.startsWith('wss://') || cfgUrl.startsWith('ws://'))) {
    connections.push(new XiaozhiConnection('xiaozhi-1', cfgUrl, connCfg));
    return connections;
  }

  console.error('❌ 请先配置小智 URL：');
  console.error('   方式一（多接入）：在 config.yaml 中取消注释 xiaozhi.urls，并在 .env 中设置 XIAOZHI_URL_1、XIAOZHI_URL_2');
  console.error('   方式二（单接入）：在 .env 中设置 XIAOZHI_URL 为从小智后台复制的完整 wss 链接');
  process.exit(1);
}

// ── 4. 优雅退出 ────────────────────────────────────────────
let xiaozhiConnections = [];

function gracefulShutdown(signal) {
  console.log(`\n收到 ${signal}，正在退出...`);
  if (upstreamRetryTimer) {
    clearInterval(upstreamRetryTimer);
    upstreamRetryTimer = null;
  }
  for (const conn of xiaozhiConnections) {
    try {
      conn.cleanup();
    } catch (_) { /* ignore */ }
  }
  // 关闭所有 stdio upstream
  for (const [name, up] of upstreams) {
    if (up.type === 'stdio' && up.transport) {
      try {
        up.transport.close();
      } catch (_) { /* ignore */ }
    }
  }
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ── 启动 ───────────────────────────────────────────────────
console.log(`🚀 xiaozhi-mini v${VERSION} 启动中...`);
await initUpstreams();
const totalTools = [...upstreams.values()].reduce((sum, u) => sum + u.tools.length, 0);
const errCount = [...upstreams.values()].filter(u => u.error).length;
console.log(`📦 ${upstreams.size} 个 upstream，${totalTools} 个工具${errCount ? `，${errCount} 个失败` : ''}`);

xiaozhiConnections = buildXiaozhiConnections();
console.log(`🔗 准备启动 ${xiaozhiConnections.length} 个小智连接`);
for (const conn of xiaozhiConnections) {
  conn.connect();
}

// 启动 upstream 失败自动重连
startUpstreamRetry();
