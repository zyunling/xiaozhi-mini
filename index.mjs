/**
 * xiaozhi-mini v2.4
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

const VERSION = '2.4.0';

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

const RECONNECT_DELAY = config.xiaozhi?.reconnect_delay || 3000;
const RECONNECT_MAX_DELAY = config.xiaozhi?.reconnect_max_delay || 60000;
const HEARTBEAT_INTERVAL = config.xiaozhi?.heartbeat_interval || 25000;
const HEARTBEAT_TIMEOUT = config.xiaozhi?.heartbeat_timeout || 10000;
const MESSAGE_QUEUE_MAX_SIZE = config.xiaozhi?.message_queue_max || 100;
const ENABLE_HEARTBEAT = config.xiaozhi?.enable_heartbeat !== false;
const ENABLE_LISTTOOLS_KEEPALIVE = config.xiaozhi?.enable_listtools_keepalive === true;

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

// ── 工具：解析 SSE 流，取出 JSON-RPC 响应 ─────────────────
// 兼容两种情况：1) 单个 JSON 响应  2) SSE 流式响应（取完整 JSON）
async function parseSSEResponse(response) {
  const text = await response.text();
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
    return JSON.parse(text);
  }
  const lastData = dataLines[dataLines.length - 1];
  try {
    return JSON.parse(lastData.trim());
  } catch {
    return JSON.parse(dataLines.join('').trim());
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
        const listResp = await postHA(
          { url: cfg.url, headers: cfg.headers || {} },
          'tools/list',
          {},
          'init-list'
        );
        const tools = listResp.result?.tools || [];
        upstreams.set(name, { type: 'streamable-http', url: cfg.url, headers: cfg.headers || {}, tools });
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
  for (const [name, up] of upstreams) {
    for (const t of up.tools) {
      let desc = t.description || '';

      // 给 memory/knowledge-graph 工具添加中文记忆语义，
      // 否则中文 LLM 听到"记住"不会和英文的 "create_entities" 关联起来
      if (name === 'memory') {
        const memoryDescMap = {
          'create_entities': '创建记忆：当用户说"记住"、"记录"、"帮我记着"时，把信息保存到长期记忆。例如"记住我叫张三"→用此工具',
          'add_observations': '补充记忆：给已有记忆添加新细节。例如用户之前提过名字，现在补充"他还喜欢喝咖啡"',
          'read_graph': '读取全部记忆：当用户问"你知道我什么"、"我是谁"时，查看已记录的所有信息',
          'search_nodes': '搜索记忆：当用户提到一个关键词，想不起来之前是否记过时，用此工具查找',
          'open_nodes': '打开记忆详情：查看某个具体记忆的完整内容',
          'delete_entities': '删除记忆：用户要求"删掉"、"忘掉"某条记录时使用',
          'delete_observations': '删除记忆中的某条细节',
          'delete_relations': '删除记忆中的关联关系'
        };
        desc = memoryDescMap[t.name] || desc;
      }

      all.push({ ...t, name: `${name}_${t.name}`, description: `[${name}] ${desc}` });
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
let reconnectDelay = RECONNECT_DELAY;
let isClosed = false;
let reconnectTimer = null;
let heartbeatTimer = null;
let heartbeatTimeoutTimer = null;
let pingSupported = null;
let lastMessageTime = 0;
const messageQueue = [];
let consecutive1006Count = 0;

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

function startHeartbeat() {
  if (!ENABLE_HEARTBEAT) return;
  stopHeartbeat();

  heartbeatTimer = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    if (pingSupported === null || pingSupported === true) {
      try {
        ws.ping();
        if (HEARTBEAT_TIMEOUT > 0) {
          heartbeatTimeoutTimer = setTimeout(() => {
            console.warn('⚠️  心跳超时，连接可能已断开');
            if (ws) {
              ws.terminate();
            }
          }, HEARTBEAT_TIMEOUT);
        }
      } catch (err) {
        console.warn('⚠️  发送 ping 失败:', maskUrl(err.message));
        pingSupported = false;
      }
    }

    if (ENABLE_LISTTOOLS_KEEPALIVE && (!ENABLE_HEARTBEAT || pingSupported === false)) {
      if (now() - lastMessageTime > HEARTBEAT_INTERVAL) {
        console.log('💓 listTools 保活探测');
        wsSendImmediate({ jsonrpc: '2.0', method: 'tools/list', id: `keepalive-${now()}` });
      }
    }
  }, HEARTBEAT_INTERVAL);
}

function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (heartbeatTimeoutTimer) {
    clearTimeout(heartbeatTimeoutTimer);
    heartbeatTimeoutTimer = null;
  }
}

function cleanupWs() {
  stopHeartbeat();
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

function scheduleReconnect(code) {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
  }

  let delay;
  if (code === 1006) {
    consecutive1006Count++;
    if (consecutive1006Count <= 3) {
      delay = RECONNECT_DELAY;
    } else {
      delay = Math.min(RECONNECT_DELAY * Math.pow(1.5, consecutive1006Count - 3), RECONNECT_MAX_DELAY);
    }
    console.log(`🔄 空闲超时(${code})，连续 ${consecutive1006Count} 次，${Math.round(delay / 1000)}s 后重连...`);
  } else {
    consecutive1006Count = 0;
    delay = reconnectDelay;
    console.log(`🔴 小智断开(${code})，${Math.round(delay / 1000)}s 后重连...`);
    reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_DELAY);
  }

  delay = jitter(delay);
  reconnectTimer = setTimeout(connectXiaozhi, delay);
}

function handleIncomingMessage(raw) {
  lastMessageTime = now();

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

  if (msg.method === 'tools/list') {
    const tools = aggregateTools();
    wsSend({ jsonrpc: '2.0', result: { tools }, id: msg.id });
    console.log(`📤 推送 ${tools.length} 个工具给小智`);
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
  if (ENABLE_HEARTBEAT) {
    console.log(`💓 心跳保活已启用，间隔 ${HEARTBEAT_INTERVAL / 1000}s，超时 ${HEARTBEAT_TIMEOUT / 1000}s`);
  }
  if (ENABLE_LISTTOOLS_KEEPALIVE) {
    console.log(`📋 listTools 保活已启用`);
  }

  ws = new WebSocket(XIAOZHI_URL, {
    perMessageDeflate: false,
    handshakeTimeout: 10000,
  });

  ws.on('open', () => {
    console.log('🟢 小智 WebSocket 已连接');
    lastMessageTime = now();
    reconnectDelay = RECONNECT_DELAY;
    consecutive1006Count = 0;
    pingSupported = null;
    startHeartbeat();
    flushMessageQueue();
  });

  ws.on('message', handleIncomingMessage);

  ws.on('pong', () => {
    lastMessageTime = now();
    if (heartbeatTimeoutTimer) {
      clearTimeout(heartbeatTimeoutTimer);
      heartbeatTimeoutTimer = null;
    }
    if (pingSupported === null) {
      pingSupported = true;
      console.log('✅ WebSocket ping/pong 心跳可用');
    }
  });

  ws.on('ping', () => {
    lastMessageTime = now();
  });

  ws.on('close', (code) => {
    if (isClosed) return;
    isClosed = true;
    stopHeartbeat();
    scheduleReconnect(code);
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
