# xiaozhi-mini

> 轻量 MCP 聚合桥：小智 AI ↔ N 个 MCP Server，内存占用 ~50-70MB

替代 MCPHub（~800MB），专干一件事：把小智后台的 `wss://api.xiaozhi.me/mcp/` 和多个标准 MCP Server（streamable-http / stdio）桥接起来，工具自动聚合、前缀路由、连接稳定不掉线。

## ✨ 特性

- 🪶 **超轻量**：Node 单进程，内存 50-70MB（MCPHub 的 1/12）
- 🔌 **双协议**：streamable-http（自动解析 SSE） + stdio（子进程）
- 📦 **工具聚合**：多 upstream 工具自动加前缀合并，推给小智
- 💓 **稳定保活**：正确响应小智服务端 JSON-RPC `ping`，连接不再 1006
- 🔁 **三重重连**：快速重连 → 无限重连 → 休眠模式，断线无感知
- 📝 **YAML 配置**：加 MCP 工具只需改 config.yaml + restart
- 🔒 **密钥分离**：Token 独立在 `.env` 文件，避免误提交
- 🔒 **日志脱敏**：自动遮蔽 URL 中的 token，避免密钥泄露到日志

## 🚀 快速开始

```bash
git clone https://github.com/zyunling/xiaozhi-mini.git
cd xiaozhi-mini
```

1. 配置小智 Token

```bash
cp .env.example .env
# 编辑 .env，填入你的小智 token
# XIAOZHI_TOKEN=你的真实token
```

2. 编辑 `config.yaml`，配置 upstream MCP Server

```bash
vim config.yaml
```

3. 构建 & 启动

```bash
docker compose up -d --build
```

4. 看日志

```bash
docker logs -f xiaozhi-mini
```

正常启动日志：

```text
🚀 xiaozhi-mini v2.4.0 启动中...
✅ [ha] streamable-http: 14 个工具
✅ [memory] stdio: 9 个工具 (PID: 16)
📦 2 个 upstream，23 个工具
🔗 连接小智: wss://api.xiaozhi.me/mcp/?token=***
🟢 小智 WebSocket 已连接
📢 已通知小智工具列表更新
📤 推送 23 个工具给小智
```

## ⚙️ 配置说明

### `.env` — 密钥配置

```env
# 小智 MCP 连接 Token（从小智后台获取）
XIAOZHI_TOKEN=your_token_here
```

### `config.yaml` — 功能配置

```yaml
xiaozhi:
  base_url: "wss://api.xiaozhi.me/mcp/"
  # 快速重连延迟（ms），默认 2s
  # reconnect_delay: 2000
  # 是否启用快速重连模式（默认 true）
  # aggressive_reconnect: true

upstreams:
  ha:
    type: streamable-http
    url: "http://HA_IP:PORT/private_/mcp"
    headers:
      Accept: "application/json, text/event-stream"

  memory:
    type: stdio
    command: "npx"
    args: ["-y", "@modelcontextprotocol/server-memory"]
    env:
      MEMORY_FILE_PATH: /tmp/memory.jsonl
```

| 字段 | 说明 |
|------|------|
| `xiaozhi.base_url` | 小智 MCP endpoint 基础地址（token 从 `.env` 注入） |
| `xiaozhi.reconnect_delay` | 快速重连延迟（ms），默认 2000 |
| `xiaozhi.aggressive_reconnect` | 快速重连模式（默认 true），关闭后启用指数退避→无限重连→休眠 |
| `xiaozhi.max_quick_reconnect` | 快速重连最大次数（默认 10），超限后进入无限重连 |
| `xiaozhi.infinite_reconnect_delay` | 无限重连间隔（ms），默认 30 分钟 |
| `xiaozhi.max_infinite_retries` | 无限重连最大次数（默认 48，约 24 小时） |
| `xiaozhi.sleep_threshold` | 进入休眠模式前的失败次数（默认 12） |
| `xiaozhi.sleep_interval` | 休眠间隔（ms），默认 2 小时 |
| `xiaozhi.message_queue_max` | 断线期间缓存消息数上限（默认 100） |
| `upstreams.*.type` | `streamable-http` 或 `stdio` |
| `upstreams.*.url` | HTTP 类型填端点 URL |
| `upstreams.*.command/args` | stdio 类型填启动命令 |
| `upstreams.*.env` | 可选，注入到子进程的环境变量 |

> 💡 **关于连接保活**：小智服务端通过 JSON-RPC `ping` 方法做应用层保活探测，客户端必须回复 `pong`（空 result），否则服务端 30 秒后断开连接。本项目已正确实现该握手，连接可长期保持稳定。

## ➕ 添加 MCP Server

编辑 `config.yaml`，在 `upstreams:` 下加一段（参考文件内示例），然后：

```bash
docker compose restart
```

小智会自动拿到新工具（带前缀，如 `tavily_search`、`filesystem_read`）。

## 📊 对比 MCPHub

| 指标 | MCPHub | xiaozhi-mini |
|------|--------|---------------|
| 内存 | ~800MB | **~50-70MB** |
| 依赖 | Node + pg + pgvector + React | Node + ws + yaml |
| 工具列表 | pg 查询 | 内存缓存，零 IO |
| 保活策略 | JSON-RPC ping 响应 | JSON-RPC ping 响应 |
| 重连 | 快速重连 → 无限重连 → 休眠 | 快速重连 → 无限重连 → 休眠 |
| 密钥管理 | 写在配置文件 | 独立 `.env`，防误提交 |

## 🗺️ Roadmap

- [ ] 支持多个小智 endpoint 并发
- [ ] upstream 失败自动重连（目前需 restart）
- [ ] 可选 Web UI（默认关闭，省内存）

## 📄 License

MIT
