# xiaozhi-mini

> 轻量 MCP 聚合桥：小智 AI ↔ N 个 MCP Server，内存占用 ~50-70MB

替代 MCPHub（~800MB），专干一件事：把小智后台的 `wss://api.xiaozhi.me/mcp/` 和多个标准 MCP Server（streamable-http / stdio）桥接起来，工具自动聚合、前缀路由、连接稳定不掉线。

## ✨ 特性

- 🪶 **超轻量**：Node 单进程，内存 50-70MB（MCPHub 的 1/12）
- 🔌 **双协议**：streamable-http（自动解析 SSE） + stdio（子进程）
- 📦 **工具聚合**：多 upstream 工具自动加前缀合并，推给小智
- 💓 **稳定保活**：正确响应小智服务端 JSON-RPC `ping`，连接不再 1006
- 🔁 **三重重连**：快速重连 → 无限重连 → 休眠模式，断线无感知
- 📦 **Payload 控制**：自动截断工具描述，payload 控制在 40KB 以内，避免 1009
- 🔑 **环境变量插值**：配置中支持 `${VAR}` 引用 `.env` 变量，密钥不硬编码
- 📝 **YAML 配置**：加 MCP 工具只需改 config.yaml + restart
- 🔒 **密钥分离**：Token 独立在 `.env` 文件，避免误提交
- 🔒 **日志脱敏**：自动遮蔽 URL 中的 token，避免密钥泄露到日志
- ⏰ **内置闹钟**：支持倒计时/定时/重复闹钟，语音设置，数据持久化

## 🚀 快速开始

```bash
git clone https://github.com/zyunling/xiaozhi-mini.git
cd xiaozhi-mini
```

1. 配置密钥

```bash
cp .env.example .env
# 编辑 .env，填入你的 token 和 API Key
# XIAOZHI_TOKEN=你的小智token
# TAVILY_API_KEY=你的tavily_key（可选）
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
🚀 xiaozhi-mini v2.5.0 启动中...
✅ [ha] streamable-http: 14 个工具
✅ [memory] stdio: 9 个工具 (PID: 16)
✅ [tavily] streamable-http: 5 个工具
📦 3 个 upstream，28 个工具
🔗 连接小智: wss://api.xiaozhi.me/mcp/?token=***
🟢 小智 WebSocket 已连接
📢 已通知小智工具列表更新
📤 推送 28 个工具给小智 (payload: 37KB)
```

## ⚙️ 配置说明

### `.env` — 密钥配置

```env
# 小智 MCP 连接 Token（从小智后台获取）
XIAOZHI_TOKEN=your_token_here

# 可选：其他 MCP Server 的 API Key
# TAVILY_API_KEY=your_tavily_key
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

  # 支持环境变量插值：${VAR} 会自动替换为 .env 中的值
  tavily:
    type: streamable-http
    url: "https://api.tavily.com/mcp"
    headers:
      Authorization: "Bearer ${TAVILY_API_KEY}"
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

## ⏰ 内置闹钟功能

xiaozhi-mini 内置了一个云端闹钟 MCP 服务，支持通过语音设置闹钟。

### 启用方法

在 `config.yaml` 的 `upstreams` 中取消注释：

```yaml
alarm:
  type: stdio
  command: "node"
  args: ["alarm-server.mjs"]
  cwd: /app
```

然后重启：

```bash
docker compose restart
```

### 支持的功能

| 功能 | 说明 | 示例语音 |
|------|------|----------|
| 倒计时闹钟 | 设置多少秒/分钟后提醒 | "5分钟后提醒我开会" |
| 定时闹钟 | 设置指定时间提醒 | "明天早上8点叫我起床" |
| 重复闹钟 | 每天/每周重复 | "每天早上7点半提醒我吃早餐" |
| 间隔提醒 | 每隔一段时间提醒 | "每小时提醒我喝水" |
| 查询闹钟 | 查看所有设置中的闹钟 | "现在有什么闹钟" |
| 删除闹钟 | 删除指定闹钟 | "取消1号闹钟" |

### 工具说明

- `alarm_create` - 创建闹钟
- `alarm_list` - 查询闹钟列表
- `alarm_delete` - 删除指定 ID 的闹钟
- `alarm_clear_all` - 清除所有闹钟

### 数据持久化

闹钟数据保存在 `./alarm-data/alarms.json`，容器重启后不会丢失。

> ⚠️ **注意**：这是**云端闹钟**，运行在 xiaozhi-mini 服务器上。提醒方式为：当你下次和小智对话时，如果有已到时间的闹钟，会自动告知你。如需主动响铃提醒，需要设备端固件支持（可考虑刷入带闹钟功能的第三方固件）。

## 📊 对比 MCPHub

| 指标 | MCPHub | xiaozhi-mini |
|------|--------|---------------|
| 内存 | ~800MB | **~50-70MB** |
| 依赖 | Node + pg + pgvector + React | Node + ws + yaml |
| 工具列表 | pg 查询 | 内存缓存，零 IO |
| 保活策略 | JSON-RPC ping 响应 | JSON-RPC ping 响应 |
| 重连 | 快速重连 → 无限重连 → 休眠 | 快速重连 → 无限重连 → 休眠 |
| Payload 控制 | 无 | 自动截断，40KB 上限 |
| 密钥管理 | 写在配置文件 | 独立 `.env` + `${VAR}` 插值 |

## 🗺️ Roadmap

- [ ] 支持多个小智 endpoint 并发
- [ ] upstream 失败自动重连（目前需 restart）
- [ ] 可选 Web UI（默认关闭，省内存）

## 📄 License

MIT
