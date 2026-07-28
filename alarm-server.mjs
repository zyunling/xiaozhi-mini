import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const fs = require('fs');
const path = require('path');

const VERSION = '1.0.0';
const DATA_DIR = process.env.ALARM_DATA_DIR || path.join(process.cwd(), 'data');
const ALARM_FILE = path.join(DATA_DIR, 'alarms.json');

let alarms = [];
let nextId = 1;

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function loadAlarms() {
  try {
    ensureDataDir();
    if (fs.existsSync(ALARM_FILE)) {
      const data = JSON.parse(fs.readFileSync(ALARM_FILE, 'utf-8'));
      alarms = data.alarms || [];
      nextId = data.nextId || 1;
    }
  } catch (e) {
    console.error('加载闹钟失败:', e.message);
  }
}

function saveAlarms() {
  try {
    ensureDataDir();
    fs.writeFileSync(ALARM_FILE, JSON.stringify({ alarms, nextId }, null, 2));
  } catch (e) {
    console.error('保存闹钟失败:', e.message);
  }
}

function sendResponse(id, result) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', result, id }) + '\n');
}

function sendError(id, code, message) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', error: { code, message }, id }) + '\n');
}

function calculateNextTime(hour, minute, weekDays) {
  const now = new Date();
  const next = new Date(now);
  next.setHours(hour, minute, 0, 0);

  if (next.getTime() <= now.getTime()) {
    next.setDate(next.getDate() + 1);
  }

  if (weekDays && weekDays.length > 0) {
    for (let i = 0; i < 8; i++) {
      if (weekDays.includes(next.getDay())) {
        return next.getTime();
      }
      next.setDate(next.getDate() + 1);
    }
  }

  return next.getTime();
}

function formatTime(timestamp) {
  const d = new Date(timestamp);
  const pad = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function getWeekDayName(day) {
  const names = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
  return names[day] || '';
}

function formatAlarmInfo(alarm) {
  const lines = [];
  lines.push(`ID: ${alarm.id}`);
  lines.push(`名称: ${alarm.name}`);
  lines.push(`类型: ${alarm.type === 'countdown' ? '倒计时' : '定时'}`);
  lines.push(`触发时间: ${formatTime(alarm.triggerTime)}`);
  if (alarm.weekDays && alarm.weekDays.length > 0) {
    lines.push(`重复: 每周 ${alarm.weekDays.map(d => getWeekDayName(d)).join('、')}`);
  } else if (alarm.repeat > 0 && alarm.interval > 0) {
    const hours = Math.floor(alarm.interval / 3600);
    const mins = Math.floor((alarm.interval % 3600) / 60);
    const intervalStr = hours > 0 ? `${hours}小时` : `${mins}分钟`;
    lines.push(`重复: 还剩 ${alarm.repeat} 次，每${intervalStr}一次`);
  }
  return lines.join('\n');
}

function checkExpiredAlarms() {
  const now = Date.now();
  const expired = [];
  const stillActive = [];

  for (const alarm of alarms) {
    if (!alarm.active) continue;

    if (alarm.triggerTime <= now) {
      expired.push(alarm);

      if (alarm.repeat > 0 && alarm.interval > 0) {
        alarm.triggerTime = now + alarm.interval * 1000;
        alarm.repeat--;
        stillActive.push(alarm);
      } else if (alarm.weekDays && alarm.weekDays.length > 0 && alarm.hour !== undefined) {
        alarm.triggerTime = calculateNextTime(alarm.hour, alarm.minute || 0, alarm.weekDays);
        stillActive.push(alarm);
      } else {
        alarm.active = false;
      }
    } else {
      stillActive.push(alarm);
    }
  }

  if (expired.length > 0) {
    alarms = alarms.filter(a => a.active).concat(alarms.filter(a => !a.active));
    saveAlarms();
  }

  return expired;
}

function createAlarm(params) {
  const { name, delay, hour, minute, repeat, interval, week_days } = params;
  let triggerTime;
  let type = 'countdown';

  if (hour !== undefined || minute !== undefined) {
    type = 'scheduled';
    const h = hour !== undefined ? hour : 0;
    const m = minute !== undefined ? minute : 0;
    const wd = week_days || [];
    triggerTime = calculateNextTime(h, m, wd);
  } else if (delay !== undefined) {
    type = 'countdown';
    triggerTime = Date.now() + delay * 1000;
  } else {
    throw new Error('请指定 delay（倒计时秒数）或 hour/minute（定时时间）。例如："5分钟后提醒我"或"明天早上8点叫我起床"');
  }

  const alarm = {
    id: nextId++,
    name: name || '闹钟',
    type,
    triggerTime,
    repeat: repeat || 0,
    interval: interval || 0,
    weekDays: week_days || [],
    hour: hour,
    minute: minute,
    createdAt: Date.now(),
    active: true
  };

  alarms.push(alarm);
  saveAlarms();

  return {
    success: true,
    message: `闹钟已设置：${alarm.name}，将于 ${formatTime(alarm.triggerTime)} 提醒`,
    alarm: {
      id: alarm.id,
      name: alarm.name,
      trigger_time: formatTime(alarm.triggerTime),
      type: alarm.type
    }
  };
}

function listAlarms() {
  checkExpiredAlarms();
  const activeAlarms = alarms.filter(a => a.active);
  activeAlarms.sort((a, b) => a.triggerTime - b.triggerTime);

  if (activeAlarms.length === 0) {
    return {
      total: 0,
      message: '当前没有设置中的闹钟',
      alarms: []
    };
  }

  return {
    total: activeAlarms.length,
    message: `当前有 ${activeAlarms.length} 个闹钟`,
    alarms: activeAlarms.map(a => ({
      id: a.id,
      name: a.name,
      type: a.type,
      trigger_time: formatTime(a.triggerTime),
      info: formatAlarmInfo(a)
    }))
  };
}

function deleteAlarm(params) {
  const { id } = params;
  const index = alarms.findIndex(a => a.id === id);
  if (index === -1) {
    throw new Error(`未找到 ID 为 ${id} 的闹钟。请先调用 alarm_list 查看所有闹钟的 ID。`);
  }
  const deleted = alarms.splice(index, 1)[0];
  saveAlarms();
  return {
    success: true,
    message: `已删除闹钟：${deleted.name}（ID: ${deleted.id}）`,
    deleted_id: deleted.id,
    deleted_name: deleted.name
  };
}

function clearAllAlarms() {
  const count = alarms.filter(a => a.active).length;
  alarms = alarms.filter(a => !a.active);
  saveAlarms();
  return {
    success: true,
    message: count > 0 ? `已清除全部 ${count} 个闹钟` : '当前没有活动闹钟',
    cleared_count: count
  };
}

function getTriggeredAlarms() {
  const expired = checkExpiredAlarms();
  if (expired.length === 0) {
    return { has_triggered: false, alarms: [] };
  }
  return {
    has_triggered: true,
    message: `有 ${expired.length} 个闹钟已到时间！`,
    alarms: expired.map(a => ({
      id: a.id,
      name: a.name,
      trigger_time: formatTime(a.triggerTime)
    }))
  };
}

function handleRequest(method, params, id) {
  switch (method) {
    case 'initialize':
      sendResponse(id, {
        protocolVersion: '2024-11-05',
        capabilities: {
          tools: {}
        },
        serverInfo: { name: 'xiaozhi-alarm-server', version: VERSION }
      });
      break;

    case 'tools/list':
      sendResponse(id, {
        tools: [
          {
            name: 'alarm_create',
            description: `创建闹钟提醒。支持两种方式：
1. 倒计时：设置 delay 秒数，例如 delay=300 表示5分钟后提醒
2. 定时：设置 hour 和 minute，例如 hour=8, minute=0 表示早上8点

使用场景：
- "5分钟后提醒我开会" → delay=300, name="开会提醒"
- "明天早上8点叫我起床" → hour=8, minute=0, name="起床闹钟"
- "每天早上7点半提醒我吃早餐" → hour=7, minute=30, week_days=[0,1,2,3,4,5,6], name="早餐提醒"
- "每小时提醒我喝水" → delay=3600, repeat=23, interval=3600, name="喝水提醒"

注意：week_days 中 0=周日，1=周一，2=周二，3=周三，4=周四，5=周五，6=周六`,
            inputSchema: {
              type: 'object',
              properties: {
                name: { type: 'string', description: '闹钟名称，描述提醒的内容，例如"起床闹钟"、"开会提醒"' },
                delay: { type: 'integer', description: '倒计时秒数，从现在开始算起，例如 300 表示5分钟后' },
                hour: { type: 'integer', minimum: 0, maximum: 23, description: '定时的小时（24小时制），0-23，例如 8 表示早上8点' },
                minute: { type: 'integer', minimum: 0, maximum: 59, description: '定时的分钟，0-59，例如 30 表示30分' },
                repeat: { type: 'integer', minimum: 0, maximum: 1000, description: '重复次数，0表示不重复（默认），例如 1 表示再重复1次（总共响2次）' },
                interval: { type: 'integer', minimum: 60, description: '重复间隔秒数，repeat>0 时生效，例如 86400 表示每天，3600 表示每小时' },
                week_days: {
                  type: 'array',
                  items: { type: 'integer', minimum: 0, maximum: 6 },
                  description: '每周几重复，0=周日，1=周一，2=周二，3=周三，4=周四，5=周五，6=周六。例如 [1,2,3,4,5] 表示工作日'
                }
              },
              required: []
            }
          },
          {
            name: 'alarm_list',
            description: '查询所有未触发的闹钟列表。当用户问"现在有什么闹钟"、"我设了几个闹钟"、"几点的闹钟"时使用。会显示每个闹钟的名称、时间和重复设置。',
            inputSchema: {
              type: 'object',
              properties: {}
            }
          },
          {
            name: 'alarm_delete',
            description: '删除指定 ID 的闹钟。当用户说"删除闹钟"、"取消闹钟"、"删掉那个提醒"时使用。必须先调用 alarm_list 获取闹钟 ID，然后根据 ID 删除。',
            inputSchema: {
              type: 'object',
              properties: {
                id: { type: 'integer', description: '要删除的闹钟 ID，必须先调用 alarm_list 获取' }
              },
              required: ['id']
            }
          },
          {
            name: 'alarm_clear_all',
            description: '清除所有活动闹钟。当用户说"清除所有闹钟"、"删掉全部提醒"时使用。注意：此操作不可恢复！',
            inputSchema: {
              type: 'object',
              properties: {}
            }
          }
        ]
      });
      break;

    case 'tools/call':
      const toolName = params.name;
      const toolParams = params.arguments || {};
      try {
        let result;
        switch (toolName) {
          case 'alarm_create':
            result = createAlarm(toolParams);
            break;
          case 'alarm_list':
            result = listAlarms();
            break;
          case 'alarm_delete':
            result = deleteAlarm(toolParams);
            break;
          case 'alarm_clear_all':
            result = clearAllAlarms();
            break;
          default:
            throw new Error(`未知工具: ${toolName}`);
        }
        sendResponse(id, {
          content: [
            {
              type: 'text',
              text: JSON.stringify(result, null, 2)
            }
          ],
          isError: false
        });
      } catch (e) {
        sendResponse(id, {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: e.message }, null, 2)
            }
          ],
          isError: true
        });
      }
      break;

    case 'ping':
      sendResponse(id, {});
      break;

    default:
      if (id) {
        sendError(id, -32601, `未知方法: ${method}`);
      }
  }
}

function handleNotification(method, params) {
  if (method === 'notifications/initialized') {
  }
}

loadAlarms();

let buffer = '';
process.stdin.on('data', (chunk) => {
  buffer += chunk.toString();
  const lines = buffer.split('\n');
  buffer = lines.pop() || '';

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const msg = JSON.parse(trimmed);
      if (msg.id !== undefined) {
        handleRequest(msg.method, msg.params || {}, msg.id);
      } else {
        handleNotification(msg.method, msg.params || {});
      }
    } catch (e) {
      console.error('解析消息失败:', e.message, trimmed.substring(0, 100));
    }
  }
});

console.error(`⏰ 闹钟 MCP 服务已启动 v${VERSION}`);
console.error(`📁 数据目录: ${DATA_DIR}`);
