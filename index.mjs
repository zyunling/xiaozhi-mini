/**
 * xiaozhi-mini v2.6
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

const VERSION = '2.6.0';

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
      this.log(`📤 推送 ${tools.length} 个工具给小智 (payload: ${Math.round(payloadSize / 1024)}KB)`);
      if (payloadSize > 100000) {
        this.log(`⚠️  payload 过大 (${payloadSize} bytes)，可能导致连接断开`);
      }
      this.wsSend(payload);
      return;
    }

    if (msg.method === 'tools/call') {
      const toolName = msg.params?.name || '';
      this.log(`📨 收到工具调用请求: ${toolName}`, JSON.stringify(msg.params?.arguments || {}));
      const underscoreIdx = toolName.indexOf('_');
      if (underscoreIdx <= 0) {
        this.wsSend({ jsonrpc: '2.0', error: { code: -32602, message: `invalid tool name: ${toolName}` }, id: msg.id });
        return;
      }
      const prefix = toolName.substring(0, underscoreIdx);
      const realName = toolName.substring(underscoreIdx + 1);
      const up = upstreams.get(prefix);

      if (!up) {
        this.wsSend({ jsonrpc: '2.0', error: { code: -32601, message: `unknown upstream: ${prefix}` }, id: msg.id });
        return;
      }

      // 哪个连接收到请求，就用这个连接回复（捕获 this）
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
          this.wsSend({ jsonrpc: '2.0', result, id: msg.id });
          this.log(`🔧 ${toolName} → ok`);
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

    this.ws.on('close', () => {
      if (this.isClosed) return;
      this.isClosed = true;
      this.log('🔴 小智连接断开');
      this.scheduleReconnect();
    });

    this.ws.on('error', (err) => {
      this.log('❌ WebSocket 错误:', maskUrl(err));
    });
  }

  cleanup() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
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
