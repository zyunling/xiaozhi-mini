/**
 * xiaozhi-mini v2.5
 * 轻量 MCP 聚合桥：小智 wss(MCP) ↔ N 个 upstream MCP Server
 * streamable-http: 自己 fetch + SSE 解析（避开 SDK 路径问题）
 * stdio: 仍用 SDK StdioClientTransport（路径稳定）
 */

import WebSocket from 'ws';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import yaml from 'yaml';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const VERSION = '2.5.0';

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
          (val.startsWith("'") && val.endsWith("'"))) {
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

// ── 组装小智 WebSocket URL ─────────────────────────────────
// 优先级：
// 1. config.xiaozhi.url 中有完整 URL（含 token）且不是占位符 → 直接用
// 2. 从环境变量 XIAOZHI_TOKEN 读取 token，拼接 base URL
let XIAOZHI_URL = config.xiaozhi?.url || '';
const XIAOZHI_TOKEN = process.env.XIAOZHI_TOKEN || '';

if (XIAOZHI_TOKEN && XIAOZHI_TOKEN !== 'REPLACE_WITH_YOUR_TOKEN') {
  const baseUrl = config.xiaozhi?.base_url || 'wss://api.xiaozhi.me/mcp/';
  const separator = baseUrl.includes('?') ? '&' : '?';
  XIAOZHI_URL = `${baseUrl}${separator}token=${XIAOZHI_TOKEN}`;
}

if (!XIAOZHI_URL || XIAOZHI_URL.includes('REPLACE_WITH_YOUR_TOKEN') || XIAOZHI_URL.includes('YOUR_TOKEN')) {
  console.error('❌ 请先配置小智 token：');
  console.error('   方式一：复制 .env.example 为 .env，填入 XIAOZHI_TOKEN');
  console.error('   方式二：在环境变量中设置 XIAOZHI_TOKEN');
  process.exit(1);
}

const upstreams = new Map();

// ── 工具：日志脱敏，避免 token/密钥泄露到日志 ─────────────
function maskUrl(s) {
  if (s instanceof Error) s = s.message;
  if (typeof s !== 'string') return String(s);
  return s
    .replace(/(token=)[^&\s"']+/gi, '$1***')
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
async function postHA(up, method, params, rpcId) {
  const res = await fetch(up.url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json, text/event-stream',
      ...(up.headers || {})
    },
    body: JSON.stringify({ jsonrpc: '2.0', method, params, id: rpcId })
  });
  return await parseSSEResponse(res);
}

// ── 1. 初始化所有 upstream ────────────────────────────────
async function initUpstreams() {
  for (const [name, cfg] of Object.entries(config.upstreams || {})) {
    try {
      if (cfg.type === 'streamable-http') {
        const interpolatedUrl = interpolateEnv(cfg.url);
        const interpolatedHeaders = {};
        for (const [key, value] of Object.entries(cfg.headers || {})) {
          interpolatedHeaders[key] = interpolateEnv(value);
        }
        const listResp = await postHA(
          { url: interpolatedUrl, headers: interpolatedHeaders },
          'tools/list',
          {},
          'init-list'
        );
        const tools = listResp.result?.tools || [];
        upstreams.set(name, { type: 'streamable-http', url: interpolatedUrl, headers: interpolatedHeaders, tools });
        console.log(`✅ [${name}] streamable-http: ${tools.length} 个工具`);
      } else if (cfg.type === 'stdio') {
        const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
        const transport = new StdioClientTransport({
          command: cfg.command, args: cfg.args || [], env: { ...process.env, ...(cfg.env || {}) }
        });
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
}

// ── 2. 聚合工具列表（加前缀）─────────────────────────────
function aggregateTools() {
  const all = [];
  const MAX_DESC_LEN = 300;
  const MAX_PAYLOAD_SIZE = 40000;

  for (const [name, up] of upstreams) {
    if (up.error) continue;
    for (const t of up.tools) {
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

      if (desc.length > MAX_DESC_LEN) {
        desc = desc.substring(0, MAX_DESC_LEN);
      }

      const tool = {
        name: `${name}_${t.name}`,
        description: `[${name}] ${desc}`,
        inputSchema: t.inputSchema
      };

      const testPayload = { jsonrpc: '2.0', result: { tools: [...all, tool] }, id: 1 };
      const testSize = JSON.stringify(testPayload).length;
      if (testSize > MAX_PAYLOAD_SIZE) {
        console.warn(`⚠️  payload 接近上限(${MAX_PAYLOAD_SIZE})，已截断到 ${all.length} 个工具`);
        return all;
      }

      all.push(tool);
    }
  }
  return all;
}

// ── 3. 连小智 WebSocket ───────────────────────────────────
//
// 连接优化策略（参考 mcphub 及其他小智生态项目）：
// 1. WebSocket 协议层 Ping/Pong 心跳保活（默认开启）
// 2. 可选 listTools 应用层保活（兼容不支持 ping 的服务端）
// 3. 指数退避 + 随机抖动重连，避免惊群效应
// 4. 消息队列：断开期间缓存消息，重连后发送
// 5. 1006 空闲超时使用更短的延迟快速重连

let ws = null;
let isClosed = false;
let reconnectTimer = null;
const messageQueue = [];

let quickReconnectAttempts = 0;
let isInInfiniteReconnectMode = false;
let infiniteRetryCount = 0;
let isInSleepMode = false;

function now() {
  return Date.now();
}

function enqueueMessage(data) {
  if (messageQueue.length >= MESSAGE_QUEUE_MAX_SIZE) {
    const dropped = messageQueue.shift();
    console.warn(`⚠️  消息队列已满，丢弃最旧消息: ${dropped?.id || 'unknown'}`);
  }
  messageQueue.push(data);
}

function flushMessageQueue() {
  if (messageQueue.length === 0) return;
  console.log(`📤 重连后发送 ${messageQueue.length} 条缓存消息`);
  while (messageQueue.length > 0) {
    const msg = messageQueue.shift();
    if (!wsSendImmediate(msg)) {
      messageQueue.unshift(msg);
      break;
    }
  }
}

function wsSendImmediate(data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return false;
  }
  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch (err) {
    console.error('❌ WebSocket 发送失败:', maskUrl(err.message));
    return false;
  }
}

function wsSend(data, { queue = true } = {}) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    return wsSendImmediate(data);
  }
  if (queue) {
    enqueueMessage(data);
    console.log('📥 连接未就绪，消息已加入队列');
    return false;
  }
  console.warn('⚠️  WebSocket 未连接，丢弃消息');
  return false;
}

function cleanupWs() {
  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch (_) { /* ignore */ }
    ws = null;
  }
}

function jitter(base) {
  return Math.floor(base * (0.8 + Math.random() * 0.4));
}

function scheduleReconnect() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  if (AGGRESSIVE_RECONNECT) {
    const delay = jitter(RECONNECT_DELAY);
    console.log(`🔄 快速重连模式，${Math.round(delay / 1000)}s 后重连...`);
    reconnectTimer = setTimeout(() => {
      try {
        connectXiaozhi();
      } catch (error) {
        console.error('❌ 快速重连失败:', maskUrl(error.message));
        scheduleReconnect();
      }
    }, delay);
    return;
  }

  if (quickReconnectAttempts >= MAX_QUICK_RECONNECT) {
    if (!isInInfiniteReconnectMode) {
      isInInfiniteReconnectMode = true;
      console.log(`🔄 快速重连次数已达上限(${MAX_QUICK_RECONNECT})，进入无限重连模式`);
    }
    scheduleInfiniteReconnect();
    return;
  }

  const delay = Math.min(
    RECONNECT_DELAY * Math.pow(2, quickReconnectAttempts),
    RECONNECT_MAX_DELAY
  );
  console.log(`🔄 第 ${quickReconnectAttempts + 1}/${MAX_QUICK_RECONNECT} 次快速重连，${Math.round(delay / 1000)}s 后重试...`);
  reconnectTimer = setTimeout(() => {
    quickReconnectAttempts++;
    try {
      connectXiaozhi();
    } catch (error) {
      console.error('❌ 快速重连失败:', maskUrl(error.message));
      scheduleReconnect();
    }
  }, jitter(delay));
}

function scheduleInfiniteReconnect() {
  infiniteRetryCount++;

  if (MAX_INFINITE_RETRIES > 0 && infiniteRetryCount > MAX_INFINITE_RETRIES) {
    console.log(`⏹️  已达到最大无限重连次数(${MAX_INFINITE_RETRIES})，停止重连`);
    return;
  }

  let delay;
  if (infiniteRetryCount >= SLEEP_THRESHOLD && !isInSleepMode) {
    isInSleepMode = true;
    console.log(`😴 连续失败 ${SLEEP_THRESHOLD} 次，进入休眠模式`);
  }

  if (isInSleepMode) {
    delay = SLEEP_INTERVAL;
    console.log(`😴 休眠模式，${Math.round(delay / 60000)} 分钟后重连（第 ${infiniteRetryCount} 次）`);
  } else {
    delay = INFINITE_RECONNECT_DELAY;
    console.log(`🔄 无限重连，${Math.round(delay / 60000)} 分钟后重试（第 ${infiniteRetryCount}/${MAX_INFINITE_RETRIES || '∞'} 次）`);
  }

  reconnectTimer = setTimeout(() => {
    console.log(`🔄 进行无限重连尝试（第 ${infiniteRetryCount}/${MAX_INFINITE_RETRIES || '∞'} 次）...`);
    try {
      connectXiaozhi();
    } catch (error) {
      console.error('❌ 无限重连失败:', maskUrl(error.message));
      scheduleInfiniteReconnect();
    }
  }, jitter(delay));
}

function handleIncomingMessage(raw) {
  let msg;
  try { msg = JSON.parse(raw.toString()); }
  catch { return; }

  if (msg.method === 'initialize') {
    wsSend({
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
    wsSend({ jsonrpc: '2.0', result: {}, id: msg.id });
    return;
  }

  if (msg.method === 'tools/list') {
    const tools = aggregateTools();
    const payload = { jsonrpc: '2.0', result: { tools }, id: msg.id };
    const payloadSize = JSON.stringify(payload).length;
    console.log(`📤 推送 ${tools.length} 个工具给小智 (payload: ${Math.round(payloadSize / 1024)}KB)`);
    if (payloadSize > 100000) {
      console.warn(`⚠️  payload 过大 (${payloadSize} bytes)，可能导致连接断开`);
    }
    wsSend(payload);
    return;
  }

  if (msg.method === 'tools/call') {
    const toolName = msg.params?.name || '';
    console.log(`📨 收到工具调用请求: ${toolName}`, JSON.stringify(msg.params?.arguments || {}));
    const underscoreIdx = toolName.indexOf('_');
    if (underscoreIdx <= 0) {
      wsSend({ jsonrpc: '2.0', error: { code: -32602, message: `invalid tool name: ${toolName}` }, id: msg.id });
      return;
    }
    const prefix = toolName.substring(0, underscoreIdx);
    const realName = toolName.substring(underscoreIdx + 1);
    const up = upstreams.get(prefix);

    if (!up) {
      wsSend({ jsonrpc: '2.0', error: { code: -32601, message: `unknown upstream: ${prefix}` }, id: msg.id });
      return;
    }

    (async () => {
      try {
        let result;
        if (up.type === 'streamable-http') {
          const response = await postHA(
            up,
            'tools/call',
            { name: realName, arguments: msg.params.arguments || {} },
            msg.id
          );
          result = response.result;
        } else if (up.type === 'stdio') {
          result = await up.client.callTool({ name: realName, arguments: msg.params.arguments || {} });
        }
        wsSend({ jsonrpc: '2.0', result, id: msg.id });
        console.log(`🔧 ${toolName} → ok`);
      } catch (err) {
        console.error(`❌ ${toolName} 调用失败:`, maskUrl(err));
        wsSend({ jsonrpc: '2.0', error: { code: -32000, message: maskUrl(err.message) }, id: msg.id });
      }
    })();
    return;
  }

  if (msg.id && String(msg.id).startsWith('keepalive-')) {
    console.log('💓 保活响应正常');
    return;
  }

  console.log(`❓ 未知请求: method=${msg.method}`, JSON.stringify(msg).substring(0, 200));
}

function connectXiaozhi() {
  isClosed = false;
  cleanupWs();
  console.log(`🔗 连接小智: ${maskUrl(XIAOZHI_URL)}`);

  ws = new WebSocket(XIAOZHI_URL, {
    perMessageDeflate: false,
    handshakeTimeout: 10000,
  });

  ws.on('open', () => {
    console.log('🟢 小智 WebSocket 已连接');
    quickReconnectAttempts = 0;
    isInInfiniteReconnectMode = false;
    infiniteRetryCount = 0;
    isInSleepMode = false;

    try {
      const notification = {
        jsonrpc: '2.0',
        method: 'notifications/tools/list_changed',
      };
      ws.send(JSON.stringify(notification));
      console.log('📢 已通知小智工具列表更新');
    } catch (e) {
      console.warn('⚠️  通知工具列表更新失败:', maskUrl(e.message));
    }

    flushMessageQueue();
  });

  ws.on('message', handleIncomingMessage);

  ws.on('close', () => {
    if (isClosed) return;
    isClosed = true;
    console.log('🔴 小智连接断开');
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket 错误:', maskUrl(err));
  });
}

// ── 4. 优雅退出 ────────────────────────────────────────────
function gracefulShutdown(signal) {
  console.log(`\n收到 ${signal}，正在退出...`);
  clearTimeout(reconnectTimer);
  cleanupWs();
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
connectXiaozhi();
