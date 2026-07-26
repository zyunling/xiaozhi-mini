/**
 * xiaozhi-mini v2.3
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
const RECONNECT_DELAY = config.xiaozhi.reconnect_delay || 3000;

if (!XIAOZHI_URL || XIAOZHI_URL.includes('YOUR_TOKEN')) {
  console.error('❌ 请先在 config.yaml 里填好 xiaozhi.url');
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

// ── 工具：解析 SSE 流，取出最后一个 JSON-RPC 响应 ─────────
async function parseSSEResponse(response) {
  const text = await response.text();
  const lines = text.split('\n');
  let dataLine = null;
  for (const line of lines) {
    if (line.startsWith('data: ')) {
      dataLine = line.slice(6);
    }
  }
  if (!dataLine) {
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
    body: JSON.stringify({ jsonrpc: '2.0', method, params, id: rpcId })
  });
  return await parseSSEResponse(res);
}

// ── 1. 初始化所有 upstream ────────────────────────────────
async function initUpstreams() {
  for (const [name, cfg] of Object.entries(config.upstreams || {})) {
    try {
      if (cfg.type === 'streamable-http') {
        const listResp = await postHA({ url: cfg.url, headers: cfg.headers || {} }, 'tools/list', {}, 'init-list');
        const tools = listResp.result?.tools || [];
        upstreams.set(name, { type: 'streamable-http', url: cfg.url, headers: cfg.headers || {}, tools });
        console.log(`✅ [${name}] streamable-http: ${tools.length} 个工具`);
      } else if (cfg.type === 'stdio') {
        const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
        const transport = new StdioClientTransport({
          command: cfg.command, args: cfg.args || [], env: { ...process.env, ...(cfg.env || {}) }
        });
        const client = new Client({ name: `mini-${name}`, version: '2.3.0' }, { capabilities: {} });
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
// 小智 MCP endpoint 会在空闲约 40-60s 后以 1006 断开连接。
// 该 endpoint 不支持客户端主动发送任何数据（协议层 Ping 帧和
// JSON 心跳消息都会被服务端关闭连接），因此无法从客户端侧保活。
//
// 策略：1006 走固定短延迟快速重连（对用户透明），
// 其他 close code 走指数退避避免频繁重试。

let ws = null;
let reconnectDelay = RECONNECT_DELAY;
let isClosed = false;
let reconnectTimer = null;

function connectXiaozhi() {
  isClosed = false;
  console.log(`🔗 连接小智: ${maskUrl(XIAOZHI_URL)}`);

  ws = new WebSocket(XIAOZHI_URL);

  ws.on('open', () => {
    console.log('🟢 小智 WebSocket 已连接');
    reconnectDelay = RECONNECT_DELAY;
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
          serverInfo: { name: 'xiaozhi-mini', version: '2.3.0' }
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
      console.log(`📨 收到工具调用请求: ${toolName}`, JSON.stringify(msg.params?.arguments || {}));
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
          response = await postHA(up, 'tools/call', { name: realName, arguments: msg.params.arguments || {} }, msg.id);
          if (response.id !== undefined) response.id = msg.id;
        } else if (up.type === 'stdio') {
          const result = await up.client.callTool({ name: realName, arguments: msg.params.arguments || {} });
          response = { jsonrpc: '2.0', result, id: msg.id };
        }
        ws.send(JSON.stringify(response));
        console.log(`🔧 ${toolName} → ok`);
      } catch (err) {
        console.error(`❌ ${toolName} 调用失败:`, maskUrl(err));
        ws.send(JSON.stringify({ jsonrpc: '2.0', error: { code: -32000, message: maskUrl(err.message) }, id: msg.id }));
      }
      return;
    }

    // 记录所有未识别的 method，帮助排查小智发了什么请求
    console.log(`❓ 未知请求: method=${msg.method}`, JSON.stringify(msg).substring(0, 200));
  });

  ws.on('close', (code) => {
    if (isClosed) return;
    isClosed = true;

    if (code === 1006) {
      // 1006 = 服务端空闲超时，正常行为，固定延迟快速重连
      console.log(`🔄 空闲超时(${code})，${RECONNECT_DELAY/1000}s 后重连...`);
      reconnectTimer = setTimeout(connectXiaozhi, RECONNECT_DELAY);
    } else {
      // 其他 close code：指数退避（可能是真实错误，避免频繁重试）
      console.log(`🔴 小智断开(${code})，${reconnectDelay/1000}s 后重连...`);
      reconnectTimer = setTimeout(connectXiaozhi, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 60000);
    }
  });

  ws.on('error', (err) => {
    console.error('❌ WebSocket 错误:', maskUrl(err));
  });
}

// ── 4. 优雅退出 ────────────────────────────────────────────
process.on('SIGTERM', () => { clearTimeout(reconnectTimer); ws?.close(); process.exit(0); });
process.on('SIGINT',  () => { clearTimeout(reconnectTimer); ws?.close(); process.exit(0); });

// ── 启动 ───────────────────────────────────────────────────
console.log('🚀 xiaozhi-mini v2.3 启动中...');
await initUpstreams();
const totalTools = [...upstreams.values()].reduce((sum, u) => sum + u.tools.length, 0);
const errCount = [...upstreams.values()].filter(u => u.error).length;
console.log(`📦 ${upstreams.size} 个 upstream，${totalTools} 个工具${errCount ? `，${errCount} 个失败` : ''}`);
connectXiaozhi();
