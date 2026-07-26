/**
 * xiaozhi-mini v2.2
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
const config = yaml.parse(fs.readFileSync(path.join(__dirname, 'config.yaml'), 'utf8'));

const XIAOZHI_URL = config.xiaozhi.url;
const PING_INTERVAL = config.xiaozhi.ping_interval || 15000;
const PING_TIMEOUT = config.xiaozhi.ping_timeout || 8000;

if (!XIAOZHI_URL || XIAOZHI_URL.includes('YOUR_TOKEN')) {
  console.error('❌ 请先在 config.yaml 里填好 xiaozhi.url');
  process.exit(1);
}

const upstreams = new Map();

// ── 工具：日志脱敏，避免 token/密钥泄露到日志 ─────────────
// 覆盖小智 wss token、HA 路径 token、?token= 查询参数等常见形态
function maskUrl(s) {
  if (s instanceof Error) s = s.message;
  if (typeof s !== 'string') return String(s);
  return s
    .replace(/(token=)[^&\s"']+/gi, '$1***')        // ?token=xxx
    .replace(/(\/mcp\/)[^?\s"']+/gi, '$1***')        // /mcp/xxx (小智)
    .replace(/(\/private_\/)[^?\s"']+/gi, '$1***')   // /private_/xxx (HA)
    .replace(/(\/\d{3,5}\/)[A-Za-z0-9_\-]{8,}/gi, '$1***'); // /<port>/<token>
}

// ── 工具：解析 SSE 流，取出最后一个 JSON-RPC 响应 ─────────
// HA-MCP 的 streamable-http 返回格式：
//   event: message\n
//   data: {"jsonrpc":"2.0","result":{...},"id":X}\n
//   \n
// 我们只关心 data: 那行的 JSON
async function parseSSEResponse(response) {
  const text = await response.text();
  // 取最后一个 data: 行（GET / SSE 可能有多段，POST 一般一段）
  const lines = text.split('\n');
  let dataLine = null;
  for (const line of lines) {
    if (line.startsWith('data: ')) {
      dataLine = line.slice(6);  // 去掉 "data: " 前缀
    }
  }
  if (!dataLine) {
    // 有些实现直接返回纯 JSON（非 SSE 包装），也兼容
    return JSON.parse(text);
  }
  return JSON.parse(dataLine.trim());
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
    body: JSON.stringify({
      jsonrpc: '2.0',
      method,
      params,
      id: rpcId
    })
  });
  return await parseSSEResponse(res);
}

// ── 1. 初始化所有 upstream ────────────────────────────────
async function initUpstreams() {
  for (const [name, cfg] of Object.entries(config.upstreams || {})) {
    try {
      if (cfg.type === 'streamable-http') {

        // 先测一下连通 + 拉 tools
        const listResp = await postHA({ url: cfg.url, headers: cfg.headers || {} }, 'tools/list', {}, 'init-list');
        const tools = listResp.result?.tools || [];
        // 存一个轻量对象，tools/call 时走 postHA
        upstreams.set(name, {
          type: 'streamable-http',
          url: cfg.url,
          headers: cfg.headers || {},
          tools
        });
        console.log(`✅ [${name}] streamable-http: ${tools.length} 个工具`);

      } else if (cfg.type === 'stdio') {
        // stdio 仍用 SDK（路径稳定）
        const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
        const transport = new StdioClientTransport({
          command: cfg.command,
          args: cfg.args || [],
          env: { ...process.env, ...(cfg.env || {}) }
        });
        const client = new Client(
          { name: `mini-${name}`, version: '2.2.0' },
          { capabilities: {} }
        );
        await client.connect(transport);
        const result = await client.listTools();
        const tools = result.tools || [];
        upstreams.set(name, { type: 'stdio', client, tools });
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
      all.push({
        ...t,
        name: `${name}_${t.name}`,
        description: `[${name}] ${t.description || ''}`
      });
    }
  }
  return all;
}

// ── 3. 连小智 WebSocket ───────────────────────────────────
let ws = null;
let reconnectDelay = 3000;

function connectXiaozhi() {
  console.log(`🔗 连接小智: ${maskUrl(XIAOZHI_URL)}`);

  ws = new WebSocket(XIAOZHI_URL, {
    pingInterval: PING_INTERVAL,
    pingTimeout: PING_TIMEOUT,
  });

  ws.on('open', () => {
    console.log('🟢 小智 WebSocket 已连接');
    reconnectDelay = 3000;
  });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return; }

    if (msg.method === 'initialize') {
      ws.send(JSON.stringify({
        jsonrpc: '2.0',
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'xiaozhi-mini', version: '2.2.0' }
        },
        id: msg.id
      }));
      return;
    }

    if (msg.method === 'tools/list') {
      const tools = aggregateTools();
      ws.send(JSON.stringify({ jsonrpc: '2.0', result: { tools }, id: msg.id }));
      console.log(`📤 推送 ${tools.length} 个工具给小智`);
      return;
    }

    if (msg.method === 'tools/call') {
      const toolName = msg.params?.name || '';
      const underscoreIdx = toolName.indexOf('_');
      if (underscoreIdx <= 0) {
        ws.send(JSON.stringify({ jsonrpc: '2.0', error: { code: -32602, message: `invalid tool name: ${toolName}` }, id: msg.id }));
        return;
      }
      const prefix = toolName.substring(0, underscoreIdx);
      const realName = toolName.substring(underscoreIdx + 1);
      const up = upstreams.get(prefix);

      if (!up) {
        ws.send(JSON.stringify({ jsonrpc: '2.0', error: { code: -32601, message: `unknown upstream: ${prefix}` }, id: msg.id }));
        return;
      }

      try {
        let response;
        if (up.type === 'streamable-http') {
          // 自己 POST JSON-RPC，SSE 自己解析
          response = await postHA(up, 'tools/call', {
            name: realName,
            arguments: msg.params.arguments || {}
          }, msg.id);
          // 确保 response 带正确的 id（HA 那边返回的 id 是 'init-list' 之类，要换成小智的 msg.id）
          if (response.id !== undefined) response.id = msg.id;
        } else if (up.type === 'stdio') {
          const result = await up.client.callTool({
            name: realName,
            arguments: msg.params.arguments || {}
          });
          response = { jsonrpc: '2.0', result, id: msg.id };
        }
        ws.send(JSON.stringify(response));
        console.log(`🔧 ${toolName} → ok`);
      } catch (err) {
        console.error(`❌ ${toolName} 调用失败:`, maskUrl(err));
        ws.send(JSON.stringify({
          jsonrpc: '2.0',
          error: { code: -32000, message: maskUrl(err.message) },
          id: msg.id
        }));
      }
      return;
    }
  });

  ws.on('close', (code) => {
    console.log(`🔴 小智断开(${code})，${reconnectDelay/1000}s 后重连...`);
    setTimeout(connectXiaozhi, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 60000);
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket 错误:', maskUrl(err));
  });
}

// ── 4. 优雅退出 ────────────────────────────────────────────
process.on('SIGTERM', () => { ws?.close(); process.exit(0); });
process.on('SIGINT',  () => { ws?.close(); process.exit(0); });

// ── 启动 ───────────────────────────────────────────────────
console.log('🚀 xiaozhi-mini v2.2 启动中...');
await initUpstreams();
const totalTools = [...upstreams.values()].reduce((sum, u) => sum + u.tools.length, 0);
const errCount = [...upstreams.values()].filter(u => u.error).length;
console.log(`📦 ${upstreams.size} 个 upstream，${totalTools} 个工具${errCount ? `，${errCount} 个失败` : ''}`);
connectXiaozhi();