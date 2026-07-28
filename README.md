# xiaozhi-mini

> 轻量 MCP 聚合桥：小智 AI ↔ N 个 MCP Server，内存占用 ~110MB（默认配置）

替代 MCPHub（~800MB），专干一件事：把小智后台的 `wss://api.xiaozhi.me/mcp/` 和多个标准 MCP Server（streamable-http / stdio）桥接起来，工具自动聚合、前缀路由、连接稳定不掉线。

## ✨ 特性

- 🪶 **超轻量**：默认配置（HA + memory + tavily）约 110MB，仅 MCPHub 的 1/7
- 🔌 **双协议**：streamable-http（自动解析 SSE） + stdio（子进程）
- 📦 **工具聚合**：多 upstream 工具自动加前缀合并，推给小智
- 💓 **稳定保活**：正确响应小智服务端 JSON-RPC `ping`，连接不再 1006
- 🔁 **三重重连**：快速重连 → 无限重连 → 休眠模式，断线无感知
- 📦 **Payload 控制**：自动截断工具描述，payload 控制在 40KB 以内，避免 1009
- 🔑 **环境变量插值**：配置中支持 `${VAR}` 引用 `.env` 变量，密钥不硬编码
- 📝 **YAML 配置**：加 MCP 工具只需改 config.yaml + restart
- 🔒 **密钥分离**：Token 独立在 `.env` 文件，避免误提交
- 🔒 **日志脱敏**：自动遮蔽 URL 中的 token，避免密钥泄露到日志
- 🔗 **多接入支持**：一个实例同时连接多个小智 token，工具列表共享
- 🔄 **upstream 自愈**：upstream 初始化失败后每 60 秒自动重试，无需手动 restart

## 🚀 快速开始

```bash
git clone https://github.com/zyunling/xiaozhi-mini.git
cd xiaozhi-mini
```

1. 配置密钥

```bash
cp .env.example .env
# 编辑 .env，把从小智后台复制的完整 wss 链接、HA 完整地址、Tavily Key 粘贴进去
# XIAOZHI_URL=wss://api.xiaozhi.me/mcp/?token=你的token
# HA_MCP_URL=http://HA_IP:PORT/你的HA长期令牌
# TAVILY_API_KEY=你的tavily_key
```

2. （可选）编辑 `config.yaml` 调整 upstream，默认已启用 HA / bing-search / memory / tavily 四个

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
🚀 xiaozhi-mini v2.6.0 启动中...
✅ [ha] streamable-http: 14 个工具
✅ [memory] stdio: 9 个工具 (PID: 16)
✅ [tavily] streamable-http: 5 个工具
📦 3 个 upstream，28 个工具
🔗 准备启动 1 个小智连接
[xiaozhi-1] 🔗 连接小智: wss://api.xiaozhi.me/mcp/?token=***
[xiaozhi-1] 🟢 小智 WebSocket 已连接
[xiaozhi-1] 📢 已通知小智工具列表更新
[xiaozhi-1] 📤 推送 28 个工具给小智 (payload: 32KB)
```

## ⚙️ 配置说明

### `.env` — 密钥与地址配置

所有 token / URL 都放在 `.env`，避免不同用户地址不一样还要改 `config.yaml`。直接把官方后台复制到的完整链接粘进来即可，无需手动拆分 token。

```env
# ── 小智 MCP 连接 ──────────────────────────────────────────
# 从小智官方后台复制完整的 wss 链接，直接粘贴进来
# 单个接入：
XIAOZHI_URL=wss://api.xiaozhi.me/mcp/?token=你的token

# 多个接入（可选，取消注释后在 config.yaml 中启用 xiaozhi.urls）：
# XIAOZHI_URL_1=wss://api.xiaozhi.me/mcp/?token=your_first_token
# XIAOZHI_URL_2=wss://api.xiaozhi.me/mcp/?token=your_second_token

# ── Home Assistant MCP ─────────────────────────────────────
# 填入你的 HA 完整 MCP 地址（含长期访问令牌）
HA_MCP_URL=http://192.168.1.100:9583/你的HA长期令牌

# ── Tavily 搜索 ────────────────────────────────────────────
# 从 https://tavily.com 获取 API Key
TAVILY_API_KEY=你的tavily_key
```

### `config.yaml` — 功能配置

默认已启用 HA、memory、tavily 三个 upstream，所有 URL/Key 通过 `${VAR}` 引用 `.env`，此文件一般不用改。需要更多工具（如必应搜索、文件系统等）取消注释对应段落即可。

```yaml
xiaozhi:
  # 单个接入：从 .env 读取完整 wss URL
  url: "${XIAOZHI_URL}"

  # 多个接入（可选，取消注释即可同时连接多个小智）：
  # urls:
  #   - "${XIAOZHI_URL_1}"
  #   - "${XIAOZHI_URL_2}"

upstreams:
  ha:
    type: streamable-http
    url: "${HA_MCP_URL}"
    headers:
      Accept: "application/json, text/event-stream"

  memory:
    type: stdio
    command: "npx"
    args: ["-y", "@modelcontextprotocol/server-memory"]
    env:
      MEMORY_FILE_PATH: /workspaces/memory.jsonl

  tavily:
    type: streamable-http
    url: "https://mcp.tavily.com/mcp/?tavilyApiKey=${TAVILY_API_KEY}"
    headers:
      Accept: "application/json, text/event-stream"

  # 可选：必应中文搜索（stdio，省内存默认关闭，已有 Tavily 可不开）
  # bing-search:
  #   type: stdio
  #   command: "npx"
  #   args: ["-y", "bing-cn-mcp"]
```

| 字段 | 说明 |
|------|------|
| `xiaozhi.url` | 单个接入的完整 wss URL，从 `.env` 的 `XIAOZHI_URL` 注入 |
| `xiaozhi.urls` | 多接入数组，每项是完整 wss URL（`${XIAOZHI_URL_1}` 等），一个实例连多个小智 |
| `xiaozhi.reconnect_delay` | 快速重连延迟（ms），默认 2000 |
| `xiaozhi.aggressive_reconnect` | 快速重连模式（默认 true），关闭后启用指数退避→无限重连→休眠 |
| `xiaozhi.max_quick_reconnect` | 快速重连最大次数（默认 10），超限后进入无限重连 |
| `xiaozhi.infinite_reconnect_delay` | 无限重连间隔（ms），默认 30 分钟 |
| `xiaozhi.max_infinite_retries` | 无限重连最大次数（默认 48，约 24 小时） |
| `xiaozhi.sleep_threshold` | 进入休眠模式前的失败次数（默认 12） |
| `xiaozhi.sleep_interval` | 休眠间隔（ms），默认 2 小时 |
| `xiaozhi.message_queue_max` | 断线期间缓存消息数上限（默认 100） |
| `upstreams.*.type` | `streamable-http` 或 `stdio` |
| `upstreams.*.url` | HTTP 类型填端点 URL，支持 `${VAR}` 环境变量插值 |
| `upstreams.*.headers` | HTTP 请求头，支持 `${VAR}` 环境变量插值 |
| `upstreams.*.command/args` | stdio 类型填启动命令 |
| `upstreams.*.env` | 可选，注入到子进程的环境变量 |

> 💡 **关于连接保活**：小智服务端通过 JSON-RPC `ping` 方法做应用层保活探测，客户端必须回复 `pong`（空 result），否则服务端 30 秒后断开连接（1006）。本项目已正确实现该握手，连接可长期保持稳定。

> ⚠️ **关于 Payload 大小**：小智云端 endpoint 对 WebSocket 消息大小有限制（约 40KB），超过会触发 1009 断连。本项目会自动截断工具描述并在接近上限时减少推送的工具数量。

## ➕ 添加 MCP Server

编辑 `config.yaml`，在 `upstreams:` 下加一段（参考文件内示例），然后：

```bash
docker compose restart
```

小智会自动拿到新工具（带前缀，如 `tavily_search`、`filesystem_read`）。

如果工具数量较多导致 payload 超限，日志会提示：
```
⚠️  payload 接近上限(40000)，已截断到 X 个工具
```

## 📊 对比 MCPHub

| 指标 | MCPHub | xiaozhi-mini |
|------|--------|---------------|
| 内存 | ~800MB | **~110MB（默认 3 个 upstream）** |
| 依赖 | Node + pg + pgvector + React | Node + ws + yaml |
| 工具列表 | pg 查询 | 内存缓存，零 IO |
| 保活策略 | JSON-RPC ping 响应 | JSON-RPC ping 响应 |
| 重连 | 快速重连 → 无限重连 → 休眠 | 快速重连 → 无限重连 → 休眠 |
| Payload 控制 | 无 | 自动截断，40KB 上限 |
| 多接入 | 不支持 | 一个实例连接多个小智（`xiaozhi.urls`） |
| upstream 自愈 | 无 | 失败后每 60 秒自动重试 |
| 密钥管理 | 写在配置文件 | 独立 `.env` + `${VAR}` 插值，完整 URL 直接粘贴 |

## 🗺️ Roadmap

- [x] 支持多个小智 endpoint 并发（v2.5+，`xiaozhi.urls`）
- [x] upstream 失败自动重连（v2.5+，每 60 秒自愈）
- [x] 密钥与配置分离，完整 URL 放 `.env`（v2.6）
- [ ] 可选 Web UI（默认关闭，省内存）

## 📄 License

MIT
