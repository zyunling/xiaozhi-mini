# xiaozhi-mini

> 轻量 MCP 聚合桥：小智 AI ↔ N 个 MCP Server，内存占用 ~50-70MB

替代 MCPHub（~800MB），专干一件事：把小智后台的 `wss://api.xiaozhi.me/mcp/` 和多个标准 MCP Server（streamable-http / stdio）桥接起来，工具自动聚合、前缀路由、空闲超时 3 秒自愈。

## ✨ 特性

- 🪶 **超轻量**：Node 单进程，内存 50-70MB（MCPHub 的 1/12）
- 🔌 **双协议**：streamable-http（自动解析 SSE） + stdio（子进程）
- 📦 **工具聚合**：多 upstream 工具自动加前缀合并，推给小智
- 🔁 **断线自愈**：1006 空闲超时固定 3s 重连，其他错误指数退避（3s→60s）
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
🚀 xiaozhi-mini v2.4 启动中...
✅ [ha] streamable-http: 84 个工具
✅ [memory] stdio: 9 个工具
📦 2 个 upstream，93 个工具
🔗 连接小智: wss://api.xiaozhi.me/mcp/?token=***
🟢 小智 WebSocket 已连接
📤 推送 93 个工具给小智
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
  # 小智 MCP 服务 base URL（一般不用改）
  base_url: "wss://api.xiaozhi.me/mcp/"
  # 1006 空闲超时后的重连延迟（ms），默认 3000
  # reconnect_delay: 3000

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
| `xiaozhi.reconnect_delay` | 空闲超时重连延迟（ms），默认 3000 |
| `upstreams.*.type` | `streamable-http` 或 `stdio` |
| `upstreams.*.url` | HTTP 类型填端点 URL |
| `upstreams.*.command/args` | stdio 类型填启动命令 |
| `upstreams.*.env` | 可选，注入到子进程的环境变量 |

> 💡 **关于心跳保活**：小智 MCP endpoint 不支持客户端主动发送 Ping 帧或心跳消息（发送会被服务端主动断开）。策略是：服务端空闲 40-60s 以 1006 断开后，客户端固定 3s 延迟快速重连，对用户透明。

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
| 保活策略 | 配置层 bug 不生效 | 3s 快速重连 + 指数退避 |
| 重连 | MCP 工具丢失 | 3s 自愈 + 自动重推 |
| 密钥管理 | 写在配置文件 | 独立 `.env`，防误提交 |

## 🗺️ Roadmap

- [ ] 支持多个小智 endpoint 并发
- [ ] upstream 失败自动重连（目前需 restart）
- [ ] 可选 Web UI（默认关闭，省内存）

## 📄 License

MIT
