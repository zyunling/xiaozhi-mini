# xiaozhi-mini

> 轻量 MCP 聚合桥：小智 AI ↔ N 个 MCP Server，内存占用 ~50-70MB

替代 MCPHub（~800MB），专干一件事：把小智后台的 `wss://api.xiaozhi.me/mcp/:token` 和多个标准 MCP Server（streamable-http / stdio）桥接起来，工具自动聚合、前缀路由、断线 3 秒自愈。

## ✨ 特性

- 🪶 **超轻量**：Node 单进程，内存 50-70MB（MCPHub 的 1/12）
- 🔌 **双协议**：streamable-http（自动解析 SSE） + stdio（子进程）
- 📦 **工具聚合**：多 upstream 工具自动加前缀合并，推给小智
- 💤 **15s 心跳**：原生 ws ping，杜绝 1006 长时间断连
- 🔁 **断线自愈**：指数退避重连（3s→60s），重连后工具自动重推
- 📝 **YAML 配置**：加 MCP 工具只需改 config.yaml + restart
- 🔒 **日志脱敏**：自动遮蔽 URL 中的 token，避免密钥泄露到日志

## 🚀 快速开始

```bash
git clone https://github.com/zyunling/xiaozhi-mini.git
cd xiaozhi-mini
```

1. 编辑 `config.yaml`，填你的小智 token 和 upstream 地址

```bash
vim config.yaml
```

2. 构建 & 启动

```bash
docker compose up -d --build
```

3. 看日志

```bash
docker logs -f xiaozhi-mini
```

正常启动日志：

```text
🚀 xiaozhi-mini v2.2 启动中...
✅ [ha] streamable-http: 84 个工具
✅ [memory] stdio: 9 个工具
📦 2 个 upstream，93 个工具
🔗 连接小智: wss://api.xiaozhi.me/mcp/***
🟢 小智 WebSocket 已连接
📤 推送 93 个工具给小智
```

## ⚙️ 配置说明

`config.yaml` 结构：

```yaml
xiaozhi:
  url: "wss://api.xiaozhi.me/mcp/YOUR_TOKEN"
  ping_interval: 15000   # 心跳间隔 ms
  ping_timeout: 8000

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
| `xiaozhi.url` | 小智后台 MCP endpoint（含 token） |
| `upstreams.*.type` | `streamable-http` 或 `stdio` |
| `upstreams.*.url` | HTTP 类型填端点 URL |
| `upstreams.*.command/args` | stdio 类型填启动命令 |
| `upstreams.*.env` | 可选，注入到子进程的环境变量 |

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
| 心跳 | 配置层 bug 不生效 | ws 原生 15s，100% 生效 |
| 重连 | MCP 工具丢失 | 3s 自愈 + 自动重推 |

## 🗺️ Roadmap

- [ ] 支持多个小智 endpoint 并发
- [ ] upstream 失败自动重连（目前需 restart）
- [ ] 可选 Web UI（默认关闭，省内存）

## 📄 License

MIT
