export {};
/**
 * QQ 机器人单聊推送服务
 *
 * 负责：
 *  - access_token 获取与缓存（提前刷新）
 *  - WebSocket 网关连接，捕获用户单聊 openid（intents 1<<25）
 *  - 重要日志聚合、Markdown 组装、频控（每好友 20/qpm、1000/天）
 *  - 事件图标（游戏真实物品图）与好友头像图片的素材上传与 LRU 缓存
 *
 * 所有网络调用只依赖 core/package.json 已有的 axios 与 ws。
 */

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const axios = require('axios');
const { WebSocket } = require('ws');
const { getResourcePath, getDataDir } = require('../config/runtime-paths');

const API_BASE = 'https://api.bot.qq.com';
const TOKEN_URL = `${API_BASE}/app/getAppAccessToken`;
const GATEWAY_URL = `${API_BASE}/gateway`;
const C2C_INTENTS = (1 << 25) | (1 << 26); // 1<<25: C2C_MESSAGE_CREATE; 1<<26: INTERACTION_CREATE（按钮回调）
const MAX_MEDIA_IMAGES = 90;  // 平台单机器人上限 100，留 10 张余量
const MEDIA_DIR_NAME = 'qq-bot-media';
const WX_TARGET_APP_ID = 'wx5306c5978fdb76e4';

// 七类重要事件 -> 展示名（同时兼容中文 event 与英文 code）
const EVENT_LABELS: Record<string, string> = {
    // 出售
    sell_success: '出售成功',
    '出售成功': '出售成功',
    sell_done: '仓库出售',
    // 照顾好友
    '照顾好友': '照顾好友',
    friend_care: '照顾好友',
    // 农场任务
    '领取任务': '农场任务',
    '扫描任务': '农场任务',
    farm_task: '农场任务',
    // 牧场任务
    ranch_task_claim: '牧场任务',
    ranch_task_report: '牧场任务',
    '牧场任务': '牧场任务',
    // 偷取好友牧场
    ranch_steal: '偷取牧场',
    '偷取牧场': '偷取牧场',
    // 收获作物
    '收获作物': '收获作物',
    harvest_crop: '收获作物',
    '施肥后收获作物': '收获作物',
    // 神秘商人
    '神秘商人自动购买': '神秘商人',
    mystery_shop_purchase: '神秘商人',
};

const SUPPORTED_EVENTS = Object.keys(EVENT_LABELS);

interface QqBotUser {
  openid: string;
  nickname: string;
  avatar: string;
  lastSeenAt: number;
}

interface QqBotPushConfig {
    enabled: boolean;
    appId: string;
    appSecret: string;
    openid: string;
    eventKeys: string[];
    batchWindowSec: number;
    dailyLimit: number;
    minuteLimit: number;
    includeIcons: boolean;
    includeAvatars: boolean;
    showAccount: boolean;
    offlineNotify: boolean;   // 账号下线时推送
    reloginEnabled: boolean;  // 下线消息是否带重新登录按钮
    accountRoutes: Record<string, string[]>;  // 账号ID -> 接收用户 openid 列表
}

interface RuntimeStatus {
    connected: boolean;
    connecting: boolean;
    openid: string;
    lastError: string;
    lastCaptchaAt: number;
    tokenReady: boolean;
    sentToday: number;
    sentThisMinute: number;
    queued: number;
    mediaCount: number;
    users: QqBotUser[];
    relogin: { active: boolean; accountId: string; stage: string; startedAt: number } | null;
}

// 活动实例引用：供 admin 路由访问（无需改动 provider/context 链路）
let activeService: any = null;

const DEFAULT_APP_ID = '';
const DEFAULT_EVENT_KEYS = ['sell_success', '照顾好友', '领取任务', 'ranch_task_claim', 'ranch_steal', '收获作物', '神秘商人自动购买'];

function createQqBotPushService(options: any = {}) {
    const store = options.store;
    const log: (tag: string, msg: string, extra?: any) => void = typeof options.log === 'function'
        ? options.log
        : () => {};
    // 重新登录依赖：由 runtime-engine 注入
    const deps = {
        getAccount: typeof options.getAccount === 'function' ? options.getAccount : (id: string) => null,
        reloginAccount: typeof options.reloginAccount === 'function' ? options.reloginAccount : null,
    };

    let config: QqBotPushConfig = normalizeConfig(store && store.getQqBotPush ? store.getQqBotPush() : {});
    let timer: any = null;
    let ws: any = null;
    let stopped = true;
    let reconnectDelayMs = 3000;
    let heartbeatTimer: any = null;
    let lastSeq: number | null = null;
    let sessionId = '';
    let tokenCache: { token: string; expiresAt: number } | null = null;
    let useLegacyTokenFormat = false; // op=9 时切换为 Bot {appid}.{token} 格式

    // 已发现用户（给机器人发过消息的人），持久化到 store
    const rawConfig: any = store && store.getQqBotPush ? store.getQqBotPush() : {};
    let discoveredUsers: QqBotUser[] = Array.isArray(rawConfig.discoveredUsers) ? rawConfig.discoveredUsers : [];
    // 当前进行中的扫码重新登录会话
    let reloginSession: { accountId: string; accountName: string; stage: string; startedAt: number; session: any } | null = null;

    const queue: any[] = [];
    const minuteBuckets: number[] = []; // 最近 60 秒发送时间戳
    let sentToday = 0;
    let sentDayKey = '';

    const mediaCache: Map<string, string> = new Map(); // key -> qqbot-media:// url
    const mediaMeta: Map<string, { imageId: string; file: string; lastUsed: number }> = new Map();
    let mediaLoaded = false;

    const status: RuntimeStatus = {
        connected: false,
        connecting: false,
        openid: config.openid || '',
        lastError: '',
        lastCaptchaAt: 0,
        tokenReady: false,
        sentToday: 0,
        sentThisMinute: 0,
        queued: 0,
        mediaCount: 0,
        users: [],
        relogin: null,
    };

    function normalizeConfig(input: any): QqBotPushConfig {
        const src = input && typeof input === 'object' ? input : {};
        const windowSec = Number.parseInt(src.batchWindowSec, 10);
        const daily = Number.parseInt(src.dailyLimit, 10);
        const minute = Number.parseInt(src.minuteLimit, 10);
        const eventKeys = Array.isArray(src.eventKeys)
            ? src.eventKeys.map((k: any) => String(k)).filter((k: string) => SUPPORTED_EVENTS.includes(k))
            : DEFAULT_EVENT_KEYS.slice();
        return {
            enabled: typeof src.enabled === 'boolean' ? src.enabled : false,
            appId: String(src.appId ?? DEFAULT_APP_ID).trim(),
            appSecret: String(src.appSecret ?? '').trim(),
            openid: String(src.openid ?? '').trim(),
            eventKeys: eventKeys.length ? Array.from(new Set(eventKeys)) : DEFAULT_EVENT_KEYS.slice(),
            batchWindowSec: Number.isFinite(windowSec) && windowSec >= 5 && windowSec <= 600 ? windowSec : 60,
            dailyLimit: Number.isFinite(daily) && daily >= 1 && daily <= 1000 ? daily : 800,
            minuteLimit: Number.isFinite(minute) && minute >= 1 && minute <= 20 ? minute : 8,
            includeIcons: typeof src.includeIcons === 'boolean' ? src.includeIcons : true,
            includeAvatars: typeof src.includeAvatars === 'boolean' ? src.includeAvatars : true,
            showAccount: typeof src.showAccount === 'boolean' ? src.showAccount : true,
            offlineNotify: typeof src.offlineNotify === 'boolean' ? src.offlineNotify : true,
            reloginEnabled: typeof src.reloginEnabled === 'boolean' ? src.reloginEnabled : true,
            accountRoutes: (src.accountRoutes && typeof src.accountRoutes === 'object' && !Array.isArray(src.accountRoutes))
                ? Object.fromEntries(Object.entries(src.accountRoutes).map(([k, v]) => [String(k), Array.isArray(v) ? v.map((x: any) => String(x)) : []]))
                : {},
        };
    }

    function persistUsers(): void {
        if (!store || !store.setQqBotPush) return;
        try {
            store.setQqBotPush({ ...config, discoveredUsers } as any);
        } catch {}
    }

    function getConfig(): QqBotPushConfig {
        return { ...config, eventKeys: [...config.eventKeys] };
    }

    // 账号的接收用户列表：优先用 accountRoutes 配置，否则用默认 openid
    function getRecipientsForAccount(accountId: string): string[] {
        const key = String(accountId || '');
        const routed = config.accountRoutes[key];
        if (Array.isArray(routed) && routed.length) return routed.filter(o => !!o);
        return config.openid ? [config.openid] : [];
    }

    function setConfig(input: any): QqBotPushConfig {
        const next = normalizeConfig({ ...config, ...(input || {}) });
        const secretChanged = next.appSecret !== config.appSecret || next.appId !== config.appId;
        config = next;
        status.openid = config.openid || '';
        if (store && store.setQqBotPush) {
            try { store.setQqBotPush({ ...config, discoveredUsers }); } catch (e: any) { log('QQ机器人', `保存配置失败: ${e.message}`); }
        }
        if (secretChanged) {
            tokenCache = null;
            status.tokenReady = false;
        }
        if (config.enabled) start(); else stop();
        return getConfig();
    }

    function getDayKey(): string {
        const d = new Date();
        return `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`;
    }

    function checkDayReset(): void {
        const key = getDayKey();
        if (key !== sentDayKey) {
            sentDayKey = key;
            sentToday = 0;
        }
        status.sentToday = sentToday;
        const now = Date.now();
        while (minuteBuckets.length && now - minuteBuckets[0] > 60000) minuteBuckets.shift();
        status.sentThisMinute = minuteBuckets.length;
    }

    async function getToken(force: boolean = false): Promise<string> {
        if (!config.appId || !config.appSecret) throw new Error('请先填写 AppID 和 AppSecret');
        const now = Date.now();
        if (!force && tokenCache && tokenCache.expiresAt - now > 60 * 1000) return tokenCache.token;
        const resp = await axios.post(TOKEN_URL, {
            appId: config.appId,
            clientSecret: config.appSecret,
        }, { timeout: 15000, proxy: false });
        const token = resp?.data?.access_token;
        if (!token) throw new Error(`获取 access_token 失败: ${JSON.stringify(resp?.data || {})}`);
        const expiresIn = Number(resp?.data?.expires_in || 7200);
        tokenCache = { token, expiresAt: now + expiresIn * 1000 };
        status.tokenReady = true;
        return token;
    }

    function authHeaders(token: string): Record<string, string> {
        return {
            'Authorization': `QQBot ${token}`,
            'Content-Type': 'application/json; charset=utf-8',
        };
    }

    // ---------- 用户识别 ----------
    function recordUser(user: QqBotUser): void {
        if (!user.openid) return;
        const idx = discoveredUsers.findIndex(u => u.openid === user.openid);
        if (idx >= 0) {
            discoveredUsers[idx] = { ...discoveredUsers[idx], ...user };
        } else {
            discoveredUsers.push(user);
        }
        // 只保留最近 50 个用户
        if (discoveredUsers.length > 50) discoveredUsers = discoveredUsers.slice(-50);
        status.users = getUsers();
        persistUsers();
    }

    function getUsers(): QqBotUser[] {
        return discoveredUsers.slice().sort((a, b) => b.lastSeenAt - a.lastSeenAt);
    }

    // 删除接收用户：同时清理账号路由与默认用户引用
    function deleteUser(openid: string): { defaultChanged: boolean; newDefault: string } {
        const target = String(openid || '');
        if (!target) throw new Error('openid 不能为空');
        const before = discoveredUsers.length;
        discoveredUsers = discoveredUsers.filter(u => u.openid !== target);
        if (discoveredUsers.length === before) throw new Error('未找到该用户');

        // 清理账号路由里对该用户的引用
        const routes: Record<string, string[]> = {};
        for (const [accountId, list] of Object.entries(config.accountRoutes || {})) {
            routes[accountId] = (list || []).filter(o => o !== target);
        }
        config = { ...config, accountRoutes: routes };

        // 若删除的是默认用户，改用剩余最近的一个用户
        let defaultChanged = false;
        let newDefault = config.openid;
        if (config.openid === target) {
            const next = getUsers()[0];
            newDefault = next ? next.openid : '';
            config = { ...config, openid: newDefault };
            status.openid = newDefault;
            defaultChanged = true;
        }
        status.users = getUsers();
        persistUsers();
        return { defaultChanged, newDefault };
    }

    function openidFromInteraction(d: any): string {
        return String(d?.user_openid || '').trim();
    }

    // ---------- 互动回调（重新登录按钮） ----------
    async function acknowledgeInteraction(interactionId: string, code: number = 0): Promise<void> {
        if (!interactionId) return;
        try {
            const token = await getToken();
            await axios.put(`${API_BASE}/interactions/${encodeURIComponent(interactionId)}`,
                { code }, { headers: authHeaders(token), timeout: 10000 });
        } catch (e: any) {
            log('QQ机器人', `互动回应失败: ${e?.message || '未知错误'}`);
        }
    }

    async function handleInteraction(d: any, fromOpenid: string): Promise<void> {
        const interactionId = String(d?.id || '').replace(/^INTERACTION_CREATE:/, '');
        const resolved = d?.data?.resolved || {};
        const buttonData = String(resolved.button_data || '');
        const type = Number(d?.type || 0);
        if (type !== 11) return; // 仅处理消息按钮
        // 仅指定接收用户可操作
        if (config.openid && fromOpenid && fromOpenid !== config.openid) {
            await acknowledgeInteraction(interactionId, 4); // 无权限
            return;
        }
        await acknowledgeInteraction(interactionId, 0);

        let payload: any = {};
        try {
            const decoded = buttonData.startsWith('{') ? JSON.parse(buttonData) : JSON.parse(Buffer.from(buttonData, 'base64').toString('utf8'));
            payload = decoded || {};
        } catch {
            payload = { action: buttonData };
        }
        const action = String(payload.action || '');
        if (action === 'relogin' || action === 'refreshQr') {
            const accountId = String(payload.accountId || '');
            if (!accountId) return;
            if (!config.reloginEnabled) {
                await sendText('当前未开启「重新登录按钮」功能，请在推送设置中开启。');
                return;
            }
            // refreshQr 是超时后主动重新获取二维码，强制刷新
            await startQrRelogin(accountId, action === 'refreshQr').catch(async (e: any) => {
                log('QQ机器人', `重新登录失败: ${e?.message || '未知错误'}`);
                await sendText(`重新登录启动失败：${e?.message || '未知错误'}`).catch(() => {});
            });
        }
    }

    // ---------- 二维码重新登录 ----------
    async function startQrRelogin(accountId: string, force: boolean = false): Promise<void> {
        if (!deps.reloginAccount) {
            await sendText('当前运行环境不支持从机器人重新登录，请在面板手动操作。');
            return;
        }
        const account = deps.getAccount(accountId) || {};
        const accountName = String(account.name || accountId);
        if (!force && reloginSession && reloginSession.accountId === accountId && Date.now() - reloginSession.startedAt < 100000) {
            await sendText(`账号「${accountName}」的登录二维码仍在等待扫码，请勿重复点击。`, buildRefreshQrKeyboard(accountId));
            return;
        }
        // 强制刷新时销毁旧会话
        if (reloginSession && reloginSession.session) {
            try { const OldWx = require('../services/wx-login/service'); new OldWx.WxLoginService().destroy(reloginSession.session); } catch {}
            reloginSession = null;
        }
        const { WxLoginService } = require('../services/wx-login/service');
        const wx = new WxLoginService();
        const { session, qr } = await wx.createQrSession();
        reloginSession = { accountId, accountName, stage: 'waiting', startedAt: Date.now(), session };
        status.relogin = { active: true, accountId, stage: 'waiting', startedAt: Date.now() };

        // 上传二维码图片到素材库
        const qrUrl = await uploadImageBuffer(qr, 'qr');
        const qrMarkdown = [
            `## 🔄 重新登录：${accountName}`,
            '> 请用微信扫描下方二维码完成登录',
            '',
            qrUrl ? `![扫码登录 #240px #240px](${qrUrl})` : '> 二维码图片生成失败，请在面板手动登录',
            '',
            '扫码并在手机上确认后，账号将自动重新上线。二维码过期可点击下方按钮刷新。',
        ].join('\n');
        await sendMarkdown(qrUrl ? qrMarkdown : qrMarkdown.replace(/!\[[^\n]*\]\([^\n]*\)/g, ''), buildRefreshQrKeyboard(accountId));
        log('QQ机器人', `已推送账号「${accountName}」的重登二维码，等待扫码`);

        // 后台轮询扫码状态
        void pollQrRelogin(accountId, accountName, wx, session).catch(async (e: any) => {
            status.relogin = null;
            reloginSession = null;
            log('QQ机器人', `扫码登录流程失败: ${e?.message || '未知错误'}`);
            await sendText(`账号「${accountName}」扫码登录失败：${e?.message || '未知错误'}。点击下方按钮重试：`, buildRefreshQrKeyboard(accountId)).catch(() => {});
        });
    }

    async function pollQrRelogin(accountId: string, accountName: string, wx: any, session: any): Promise<void> {
        const deadline = Date.now() + 110000;
        let scanStatus = 'waiting';
        while (Date.now() < deadline) {
            // 已被取消或替换为新会话（按 session 引用判断，避免强制刷新后旧轮询继续跑）
            if (!reloginSession || reloginSession.accountId !== accountId || reloginSession.session !== session) return;
            // poll 是长轮询（最长 35s），这里不再额外 sleep
            scanStatus = await wx.poll(session);
            if (reloginSession) reloginSession.stage = scanStatus;
            status.relogin = reloginSession ? { active: true, accountId, stage: scanStatus, startedAt: reloginSession.startedAt } : null;
            if (scanStatus === 'authorized') break;
            if (scanStatus === 'cancelled' || scanStatus === 'expired') {
                reloginSession = null;
                status.relogin = null;
                const reason = scanStatus === 'cancelled' ? '扫码已取消' : '二维码已过期';
                await sendText(`账号「${accountName}」${reason}。点击下方按钮获取最新二维码：`, buildRefreshQrKeyboard(accountId)).catch(() => {});
                return;
            }
        }
        if (scanStatus !== 'authorized') {
            reloginSession = null;
            status.relogin = null;
            await sendText(`账号「${accountName}」扫码超时。点击下方按钮获取最新二维码：`, buildRefreshQrKeyboard(accountId)).catch(() => {});
            return;
        }
        // 确认并换取 code
        if (reloginSession) reloginSession.stage = 'confirming';
        await wx.confirm(session);
        const code = await wx.issueCode(session, WX_TARGET_APP_ID);
        // 用新 code 更新账号并重新上线
        const account = deps.getAccount(accountId) || {};
        const success = await deps.reloginAccount(accountId, {
            code,
            platform: 'wx',
            loginType: 'manual',
            name: account.name || accountName,
        });
        wx.destroy(session);
        reloginSession = null;
        status.relogin = null;
        if (success) {
            await sendText(`✅ 账号「${accountName}」扫码登录成功，已重新上线。`);
            log('QQ机器人', `账号「${accountName}」扫码登录成功并重新上线`);
        } else {
            await sendText(`账号「${accountName}」登录信息已更新，但自动上线失败，请在面板启动账号。`);
        }
    }

    // ---------- WebSocket 网关：捕获用户 openid ----------
    async function connectGateway(): Promise<void> {
        if (!config.enabled || !config.appId || !config.appSecret) return;
        if (stopped || ws || status.connecting) return;
        status.connecting = true;
        try {
            const token = await getToken();
            if (stopped) { status.connecting = false; return; }
            const gwResp = await axios.get(GATEWAY_URL, { headers: authHeaders(token), timeout: 15000 });
            const url = gwResp?.data?.url || 'wss://api.bot.qq.com/websocket/';
            log('QQ机器人', '正在连接网关以捕获单聊 openid...');
            const socket = new WebSocket(url);
            ws = socket;
            let helloReceived = false;

            const clearHeartbeat = () => {
                if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
            };

            socket.on('open', () => { /* wait for Hello */ });

            socket.on('message', (raw: any) => {
                let pkt: any;
                try { pkt = JSON.parse(raw.toString()); } catch { return; }
                const op = pkt.op;
                const d = pkt.d || {};
                if (op === 10) {
                    if (helloReceived) return;
                    helloReceived = true;
                    status.connected = true;
                    status.connecting = false;
                    status.lastError = '';
                    reconnectDelayMs = 3000;
                    const tokenValue = useLegacyTokenFormat
                        ? `Bot ${config.appId}.${token}`
                        : `QQBot ${token}`;
                    if (sessionId && lastSeq !== null) {
                        socket.send(JSON.stringify({ op: 6, d: { token: tokenValue, session_id: sessionId, seq: lastSeq } }));
                    } else {
                        socket.send(JSON.stringify({
                            op: 2,
                            d: {
                                token: tokenValue,
                                intents: C2C_INTENTS,
                                shard: [0, 1],
                                properties: { $os: 'windows', $browser: 'qq-farm-bot', $device: 'qq-farm-bot' },
                            },
                        }));
                    }
                    const interval = Number(d.heartbeat_interval || 45000);
                    clearHeartbeat();
                    heartbeatTimer = setInterval(() => {
                        if (socket.readyState === socket.OPEN) {
                            socket.send(JSON.stringify({ op: 1, d: lastSeq }));
                        }
                    }, Math.max(5000, interval - 5000));
                    log('QQ机器人', '网关已连接');
                    return;
                }
                if (typeof pkt.s === 'number') lastSeq = pkt.s;
                if (op === 0) {
                    if (pkt.t === 'READY') {
                        sessionId = String(d.session_id || '');
                    } else if (pkt.t === 'C2C_MESSAGE_CREATE' || pkt.t === 'C2C_MSG_RECEIVE') {
                        const author = d.author || {};
                        const openid = String(author.user_openid || author.id || '').trim();
                        if (openid) {
                            recordUser({
                                openid,
                                nickname: String(author.username || '').trim(),
                                avatar: String(author.avatar || '').trim(),
                                lastSeenAt: Date.now(),
                            });
                            // 仅在尚未指定接收用户时，自动绑定第一个发消息的人
                            if (!config.openid) {
                                config = { ...config, openid };
                                status.openid = openid;
                                persistUsers();
                                log('QQ机器人', `已捕获单聊 openid: ${openid.slice(0, 8)}...`);
                            }
                        }
                    } else if (pkt.t === 'C2C_MSG_REJECT') {
                        status.lastCaptchaAt = Date.now();
                        log('QQ机器人', '用户关闭了主动消息推送');
                    } else if (pkt.t === 'INTERACTION_CREATE') {
                        handleInteraction(d, openidFromInteraction(d)).catch(() => {});
                    }
                } else if (op === 1) {
                    socket.send(JSON.stringify({ op: 11 }));
                } else if (op === 7) {
                    clearHeartbeat();
                    try { socket.close(); } catch {}
                } else if (op === 9) {
                    // 鉴权失败：切换 token 格式后重连（仅切换一次，避免死循环）
                    if (!useLegacyTokenFormat) {
                        useLegacyTokenFormat = true;
                        log('QQ机器人', '网关鉴权格式不兼容，已切换为兼容格式重试');
                    }
                    sessionId = '';
                    lastSeq = null;
                    clearHeartbeat();
                    helloReceived = false;
                    try { socket.close(); } catch {}
                }
            });

            socket.on('error', (err: any) => {
                status.lastError = String(err?.message || '网关连接错误');
                log('QQ机器人', `网关错误: ${status.lastError}`);
            });

            socket.on('close', () => {
                clearHeartbeat();
                if (ws === socket) ws = null;
                status.connected = false;
                status.connecting = false;
                if (!stopped && config.enabled) {
                    const delay = reconnectDelayMs;
                    reconnectDelayMs = Math.min(reconnectDelayMs * 2, 120000);
                    setTimeout(() => { connectGateway().catch(() => {}); }, delay);
                }
            });
        } catch (e: any) {
            status.connecting = false;
            status.connected = false;
            status.lastError = String(e?.message || '网关连接失败');
            log('QQ机器人', `网关连接失败: ${status.lastError}`);
            if (!stopped && config.enabled) {
                setTimeout(() => { connectGateway().catch(() => {}); }, reconnectDelayMs);
                reconnectDelayMs = Math.min(reconnectDelayMs * 2, 120000);
            }
        }
    }

    function disconnectGateway(): void {
        status.connected = false;
        status.connecting = false;
        if (ws) {
            try { ws.close(); } catch {}
            ws = null;
        }
        if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
    }

    // ---------- 素材管理：上传图片换 qqbot-media:// ----------
    function mediaFilePath(key: string): string {
        return path.join(getDataDir(), MEDIA_DIR_NAME, `${key}.json`);
    }

    function loadMediaCache(): void {
        if (mediaLoaded) return;
        mediaLoaded = true;
        try {
            const dir = path.join(getDataDir(), MEDIA_DIR_NAME);
            if (!fs.existsSync(dir)) return;
            for (const name of fs.readdirSync(dir)) {
                if (!name.endsWith('.json')) continue;
                try {
                    const meta = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'));
                    if (!meta || !meta.key || !meta.url || !meta.imageId) continue;
                    mediaCache.set(meta.key, meta.url);
                    mediaMeta.set(meta.key, { imageId: meta.imageId, file: path.join(dir, name), lastUsed: Number(meta.lastUsed || Date.now()) });
                } catch {}
            }
            status.mediaCount = mediaCache.size;
        } catch (e: any) {
            log('QQ机器人', `读取图片缓存失败: ${e.message}`);
        }
    }

    function persistMediaMeta(key: string, url: string, imageId: string): void {
        try {
            const dir = path.join(getDataDir(), MEDIA_DIR_NAME);
            if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
            const file = mediaFilePath(key);
            const meta = { key, url, imageId, lastUsed: Date.now() };
            fs.writeFileSync(file, JSON.stringify(meta), 'utf8');
            mediaMeta.set(key, { imageId, file, lastUsed: meta.lastUsed });
        } catch (e: any) {
            log('QQ机器人', `保存图片缓存失败: ${e.message}`);
        }
    }

    function evictMediaIfNeeded(): void {
        if (mediaCache.size < MAX_MEDIA_IMAGES) return;
        const entries = Array.from(mediaMeta.entries()).sort((a, b) => a[1].lastUsed - b[1].lastUsed);
        const toRemove = mediaCache.size - MAX_MEDIA_IMAGES + 1;
        for (let i = 0; i < toRemove && i < entries.length; i++) {
            const [key, meta] = entries[i];
            mediaCache.delete(key);
            mediaMeta.delete(key);
            try { if (fs.existsSync(meta.file)) fs.unlinkSync(meta.file); } catch {}
            // 同步释放平台配额
            getToken().then(token => axios.delete(`${API_BASE}/v2/media-images/${encodeURIComponent(meta.imageId)}/delete`, {
                headers: authHeaders(token), timeout: 10000,
            })).catch(() => {});
        }
    }

    function resolveImageFile(localPath: string): string {
        if (!localPath) return '';
        // 注意：Windows 下 path.isAbsolute('/game-config/...') 会返回 true，
        // 必须先处理项目内的虚拟路径前缀，再判断真实绝对路径。
        if (localPath.startsWith('/game-config/')) {
            return getResourcePath('gameConfig', localPath.slice('/game-config/'.length));
        }
        if (path.isAbsolute(localPath)) return localPath;
        return getResourcePath(localPath.replace(/^\/+/, ''));
    }

    async function uploadBuffer(buf: Buffer, key: string, ext: string, contentType: string, label: string): Promise<string> {
        loadMediaCache();
        const cached = mediaCache.get(key);
        if (cached) {
            const meta = mediaMeta.get(key);
            if (meta) { meta.lastUsed = Date.now(); }
            return cached;
        }
        if (!buf.length || buf.length > 10 * 1024 * 1024) return '';
        try {
            const token = await getToken();
            const md5 = crypto.createHash('md5').update(buf).digest('hex');
            const sha1 = crypto.createHash('sha1').update(buf).digest('hex');
            const fileName = `${key}${ext}`;
            const prep = await axios.post(`${API_BASE}/v2/media-images/prepare`, {
                file_name: fileName,
                file_size: buf.length,
                content_type: contentType,
                md5,
                sha1,
            }, { headers: authHeaders(token), timeout: 15000, proxy: false });
            const uploadId = prep?.data?.upload_id;
            const presignedUrl = prep?.data?.presigned_url;
            if (!uploadId || !presignedUrl) return '';
            await axios.put(presignedUrl, buf, {
                headers: { 'Content-Type': contentType },
                timeout: 30000,
                maxBodyLength: Infinity,
                proxy: false,
            });
            const done = await axios.post(`${API_BASE}/v2/media-images/complete`, {
                upload_id: uploadId,
                md5,
            }, { headers: authHeaders(token), timeout: 15000, proxy: false });
            const url = done?.data?.url;
            const imageId = done?.data?.image_id;
            if (!url || !imageId) return '';
            evictMediaIfNeeded();
            mediaCache.set(key, url);
            persistMediaMeta(key, url, imageId);
            status.mediaCount = mediaCache.size;
            return url;
        } catch (e: any) {
            log('QQ机器人', `图片上传失败(${label}): ${e?.message || '未知错误'}`);
            return '';
        }
    }

    async function uploadImage(localPath: string): Promise<string> {
        if (!localPath) return '';
        const key = crypto.createHash('sha1').update(String(localPath)).digest('hex');
        const file = resolveImageFile(localPath);
        if (!file || !fs.existsSync(file)) return '';
        let buf: Buffer;
        try { buf = fs.readFileSync(file); } catch { return ''; }
        const ext = path.extname(file).toLowerCase();
        const contentTypeMap: Record<string, string> = { '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.webp': 'image/webp' };
        const contentType = contentTypeMap[ext];
        if (!contentType) return '';
        return uploadBuffer(buf, key, ext, contentType, path.basename(file));
    }

    async function uploadImageBuffer(buf: Buffer, prefix: string): Promise<string> {
        if (!buf || !buf.length) return '';
        const key = `${prefix}_${crypto.createHash('sha1').update(buf).digest('hex').slice(0, 24)}`;
        // 微信二维码是 JPEG
        const contentType = buf[0] === 0x89 ? 'image/png' : 'image/jpeg';
        const ext = contentType === 'image/png' ? '.png' : '.jpg';
        return uploadBuffer(buf, key, ext, contentType, `${prefix} image`);
    }

    // 主进程侧：名称 -> 游戏物品图路径（懒加载查表），不依赖 worker 传递 iconPath
    let mainNameIconMap: Map<string, string> | null = null;
    function getMainNameIconMap(): Map<string, string> {
        if (mainNameIconMap) return mainNameIconMap;
        const map = new Map<string, string>();
        try {
            const g = require('../config/gameConfig');
            for (const p of (g.getAllPlants() as any[])) {
                const seedId = Number(p?.seed_id || 0) || 0;
                const img = seedId > 0 ? g.getSeedImageBySeedId(seedId) : '';
                const name = String(p?.name || '').trim();
                if (name && img && !map.has(name)) map.set(name, img);
            }
            for (const it of (g.getAllItems() as any[])) {
                const img = g.getItemImageById(Number(it?.id || 0) || 0);
                const name = String(it?.name || '').trim();
                if (name && img && !map.has(name)) map.set(name, img);
            }
        } catch {}
        mainNameIconMap = map;
        return map;
    }

    // 在消息文本里的物品名后插入对应物品图标，如 "出售 兰花x332" -> "出售 兰花![图](url)x332"
    async function decorateMsgWithIcons(msg: string): Promise<string> {
        if (!config.includeIcons || !msg) return msg;
        const map = getMainNameIconMap();
        if (!map.size) return msg;
        // 货币/单位等非物品词，不给它们加图标
        const NON_ITEM_WORDS = new Set(['金币', '经验', '钻石', '点券', '金币豆', '牧草', '金币卡']);
        // 找出消息中出现的所有物品名（按长度降序，避免短名先匹配导致长名被拆）
        const names = Array.from(map.keys())
            .filter(name => name && name.length >= 2 && !NON_ITEM_WORDS.has(name) && msg.includes(name))
            .sort((a, b) => b.length - a.length);
        if (!names.length) return msg;
        // 逐个把 "名字" 替换为 "名字![图标](url)"，用占位符避免重复替换
        // 同时避免同一个名字插入多次图标（只替换首次出现）
        let result = msg;
        const placeholders: Array<{ token: string; name: string }> = [];
        for (const name of names) {
            const idx = result.indexOf(name);
            if (idx < 0) continue;
            // 跳过已经插入过图标占位符附近的位置
            const token = `__QQBOT_ICON_${placeholders.length}__`;
            result = result.slice(0, idx + name.length) + token + result.slice(idx + name.length);
            placeholders.push({ token, name });
        }
        if (!placeholders.length) return result;
        const decorated = await Promise.all(placeholders.map(async ({ token, name }) => {
            const local = map.get(name) || '';
            const url = local ? await uploadImage(local) : '';
            return { token, text: url ? `![${name} #20px #20px](${url})` : '' };
        }));
        for (const d of decorated) {
            result = result.replace(d.token, d.text);
        }
        return result;
    }

    // 从消息文本里提取产物名（"出售 郁金香x37"、"获得 火鸡蛋 ×1"）
    function extractItemNameFromMsg(msg: string): string {
        const text = String(msg || '');
        let m = /出售\s+([^\s,x×，,]+)/.exec(text);
        if (m && m[1]) return m[1];
        m = /获得\s+([^\s,x×，,]+)/.exec(text);
        if (m && m[1]) return m[1];
        return '';
    }

    // 同步查表得到 iconPath（优先 itemId，其次 meta 里的名字，最后从 msg 提取）
    function resolveIconPathSync(meta: any, msg: string): string {
        const g = (() => { try { return require('../config/gameConfig'); } catch { return null; } })();
        if (!g) return '';
        const itemId = Number(meta.itemId || 0) || 0;
        if (itemId) {
            const p = g.getItemImageById(itemId);
            if (p) return p;
        }
        const map = getMainNameIconMap();
        for (const name of [meta.itemName, meta.plantName, meta.name, extractItemNameFromMsg(msg)]) {
            const n = String(name || '').trim();
            if (n && map.has(n)) return map.get(n)!;
        }
        return '';
    }

    // ---------- 日志聚合 ----------
    function enqueueLog(entry: any, accountName?: string): void {
        if (!config.enabled) return;
        if (!entry || !entry.meta) return;
        const originalMeta = entry.meta || {};
        const event = String(originalMeta.event || '');
        const label = EVENT_LABELS[event];
        if (!label) return;
        // 只推成功/有效的日志；错误与非 ok 结果跳过，减少噪音
        if (originalMeta.result && !['ok', 'success', 'successful'].includes(String(originalMeta.result))) return;
        if (!config.eventKeys.some((k: string) => EVENT_LABELS[k] === label)) return;
        const msg = String(entry.msg || '').replace(/\s+/g, ' ').trim();
        // 主进程补 iconPath：确保出售/收获/牧场偷取等带真实物品图
        const meta = { ...originalMeta };
        if (config.includeIcons && !meta.iconPath) {
            const iconPath = resolveIconPathSync(meta, msg);
            if (iconPath) meta.iconPath = iconPath;
        }
        queue.push({
            time: entry.time || '',
            tag: entry.tag || '',
            msg,
            label,
            meta,
            accountName: accountName || entry.accountName || '',
            accountId: entry.accountId || '',
        });
        status.queued = queue.length;
    }

    async function resolveIconUrl(meta: any): Promise<string> {
        if (!config.includeIcons) return Promise.resolve('');
        // 优先用 worker 侧补充的 iconPath（已按 itemId 或名字查表得到真实物品图）
        const iconPath = String(meta.iconPath || '').trim();
        let local = iconPath;
        if (!local) {
            // 回退：主进程按 itemId 反查
            const itemId = Number(meta.itemId || 0) || 0;
            if (!itemId) return Promise.resolve('');
            try {
                const gameConfig = require('../config/gameConfig');
                local = gameConfig.getItemImageById(itemId);
            } catch {}
        }
        if (!local) return Promise.resolve('');
        const url = await uploadImage(local);
        if (!url) log('QQ机器人', `图标上传未成功: ${local}`);
        return url;
    }

    async function resolveAvatarUrl(meta: any): Promise<string> {
        if (!config.includeAvatars) return '';
        const avatar = String(meta.avatarUrl || meta.friendAvatar || '').trim();
        if (!avatar) return '';
        // 仅接受 http(s) 公网图片；平台会自动转存
        if (!/^https?:\/\//i.test(avatar)) return '';
        return avatar;
    }

    async function buildMarkdown(items: any[]): Promise<string> {
        // 聚合同 label + 同 msg
        const groups = new Map<string, { count: number; item: any }>();
        for (const item of items) {
            const key = `${item.label}|${item.msg}`;
            const found = groups.get(key);
            if (found) found.count += 1;
            else groups.set(key, { count: 1, item });
        }

        const d = new Date();
        const hh = String(d.getHours()).padStart(2, '0');
        const mm = String(d.getMinutes()).padStart(2, '0');
        const accountText = config.showAccount && items[0]?.accountName ? ` · ${items[0].accountName}` : '';
        const totalText = items.length > 1 ? `（${items.length} 条）` : '';
        const lines: string[] = [`## 农场动态${accountText}`, `> ${hh}:${mm} · ${items.length} 项事件${totalText}`, ''];

        const prepared: any[] = [];
        for (const [, g] of groups) {
            const item = g.item;
            const [iconUrl, avatarUrl, decoratedMsg] = await Promise.all([
                resolveIconUrl(item.meta),
                resolveAvatarUrl(item.meta),
                decorateMsgWithIcons(item.msg),
            ]);
            prepared.push({ ...item, count: g.count, iconUrl, avatarUrl, decoratedMsg });
        }

        for (const item of prepared) {
            const countText = item.count > 1 ? ` ×${item.count}` : '';
            const friendName = String(item.meta.friendName || item.meta.nickname || '').trim();
            // 事件图标放到事件名文字后面（行内）；好友头像缩小放在昵称前
            const iconInline = item.iconUrl ? ` ![图标 #20px #20px](${item.iconUrl})` : '';
            const avatarInline = (item.avatarUrl && friendName) ? `![头像 #28px #28px](${item.avatarUrl})` : '';
            const friendText = friendName ? ` · ${avatarInline}${friendName}` : '';
            lines.push(`- **${item.label}**${iconInline}${countText}${friendText}`);
            const body = item.decoratedMsg || item.msg;
            if (body && body !== item.label) {
                lines.push(`  ${body}`);
            }
            lines.push('');
        }
        return lines.join('\n').trim();
    }

    async function sendMarkdown(content: string, keyboard?: any, openid?: string): Promise<any> {
        const token = await getToken();
        const body: any = {
            msg_type: 2,
            markdown: { content },
        };
        if (keyboard) body.keyboard = keyboard;
        const target = String(openid || config.openid || '');
        const resp = await axios.post(`${API_BASE}/v2/users/${encodeURIComponent(target)}/messages`,
            body, { headers: authHeaders(token), timeout: 15000, proxy: false });
        return resp?.data || {};
    }

    async function sendText(content: string, keyboard?: any, openid?: string): Promise<any> {
        const token = await getToken();
        const body: any = { msg_type: 0, content };
        if (keyboard) body.keyboard = keyboard;
        const target = String(openid || config.openid || '');
        const resp = await axios.post(`${API_BASE}/v2/users/${encodeURIComponent(target)}/messages`,
            body, { headers: authHeaders(token), timeout: 15000, proxy: false });
        return resp?.data || {};
    }

    // 通用操作按钮键盘
    function buildActionKeyboard(buttonId: string, label: string, visitedLabel: string, action: string, accountId: string): any {
        const buttonData = JSON.stringify({ action, accountId });
        return {
            content: {
                rows: [
                    {
                        buttons: [
                            {
                                id: buttonId,
                                render_data: { label, visited_label: visitedLabel, style: 1 },
                                action: {
                                    type: 1,
                                    permission: { type: 2 }, // 所有人可操作（回调里按 openid 过滤）
                                    data: buttonData,
                                    unsupport_tips: '当前客户端版本不支持按钮，请在面板手动登录',
                                },
                            },
                        ],
                    },
                ],
            },
        };
    }

    function buildReloginKeyboard(accountId: string): any {
        return buildActionKeyboard('relogin', '🔄 重新登录', '✅ 已触发', 'relogin', accountId);
    }

    function buildRefreshQrKeyboard(accountId: string): any {
        return buildActionKeyboard('refresh_qr', '🔄 重新获取二维码', '⏳ 已刷新', 'refreshQr', accountId);
    }

    // 账号下线推送（含下线原因与重新登录按钮）
    async function sendOfflineNotification(payload: { accountId: string; accountName: string; reason: string; detail?: string }): Promise<void> {
        if (!config.enabled || !config.offlineNotify) return;
        const recipients = getRecipientsForAccount(payload.accountId);
        if (!recipients.length) return;
        const d = new Date();
        const hh = String(d.getHours()).padStart(2, '0');
        const mm = String(d.getMinutes()).padStart(2, '0');
        const reasonText = describeOfflineReason(payload.reason);
        const lines: string[] = [
            '## ⚠️ 账号下线提醒',
            `> ${hh}:${mm} · ${payload.accountName}`,
            '',
            `- **账号**：${payload.accountName}`,
            `- **原因**：${reasonText}`,
        ];
        if (payload.detail) lines.push(`- **详情**：${payload.detail}`);
        lines.push('', '点击下方按钮重新登录：');
        const keyboard = config.reloginEnabled ? buildReloginKeyboard(payload.accountId) : undefined;
        try {
            for (const openid of recipients) {
                await sendMarkdown(lines.join('\n'), keyboard, openid);
            }
            log('QQ机器人', `已推送账号「${payload.accountName}」下线提醒（${reasonText}）`);
        } catch (e: any) {
            log('QQ机器人', `下线提醒推送失败: ${e?.message || '未知错误'}`);
        }
    }

    function describeOfflineReason(reason: string): string {
        const r = String(reason || '');
        if (r === 'offline_timeout') return '持续离线超时';
        if (r.startsWith('kickout:')) return `被踢下线（${r.slice(8)}）`;
        if (r.startsWith('disconnect:')) return `连接断开（${r.split(':').slice(1).join(':')}）`;
        if (r === 'ws_400') return '登录失效，需要更新 Code';
        return r || '未知原因';
    }

    async function flush(): Promise<void> {
        if (!queue.length) { status.queued = 0; return; }
        if (!config.enabled) {
            queue.length = 0;
            status.queued = 0;
            return;
        }
        checkDayReset();
        if (sentToday >= config.dailyLimit) {
            log('QQ机器人', `今日推送已达上限 ${config.dailyLimit} 条，已跳过`);
            queue.length = 0;
            status.queued = 0;
            return;
        }
        if (minuteBuckets.length >= config.minuteLimit) {
            status.queued = queue.length;
            return; // 等下一个窗口
        }
        const items = queue.splice(0, queue.length);
        status.queued = 0;

        // 按接收用户分组：每个 openid 收集属于它的日志（一个账号可推给多个用户）
        const byRecipient = new Map<string, any[]>();
        let anyRecipient = false;
        for (const item of items) {
            const recipients = getRecipientsForAccount(item.accountId);
            if (!recipients.length) continue;
            anyRecipient = true;
            for (const openid of recipients) {
                if (!byRecipient.has(openid)) byRecipient.set(openid, []);
                byRecipient.get(openid)!.push(item);
            }
        }
        if (!anyRecipient) {
            if (!config.openid && !Object.keys(config.accountRoutes).length) {
                log('QQ机器人', '尚未指定接收用户，请先用 QQ 给机器人发消息或在推送设置中选择');
            }
            return;
        }

        try {
            // 对每个接收用户：组装一条消息并发送（频控按整批计）
            for (const [openid, userItems] of byRecipient.entries()) {
                if (sentToday >= config.dailyLimit || minuteBuckets.length >= config.minuteLimit) break;
                const content = await buildMarkdown(userItems);
                await sendMarkdown(content, undefined, openid);
                sentToday += 1;
                minuteBuckets.push(Date.now());
                checkDayReset();
            }
        } catch (e: any) {
            status.lastError = String(e?.message || '发送失败');
            log('QQ机器人', `推送失败: ${status.lastError}`);
        }
    }

    function start(): void {
        if (!config.enabled) return;
        if (!stopped) return;
        stopped = false;
        sentDayKey = getDayKey();
        sentToday = 0;
        checkDayReset();
        const intervalMs = Math.max(5, config.batchWindowSec) * 1000;
        timer = setInterval(() => { flush().catch(() => {}); }, intervalMs);
        connectGateway().catch(() => {});
        log('QQ机器人', `推送服务已启动（聚合窗口 ${config.batchWindowSec}s）`);
    }

    function stop(): void {
        if (stopped) return;
        stopped = true;
        if (timer) { clearInterval(timer); timer = null; }
        disconnectGateway();
        log('QQ机器人', '推送服务已停止');
    }

    async function testSend(): Promise<any> {
        if (!config.enabled) throw new Error('请先开启 QQ 机器人推送');
        if (!config.appId || !config.appSecret) throw new Error('请先填写 AppID 和 AppSecret');
        if (!config.openid) throw new Error('尚未捕获 openid，请先用 QQ 给机器人发一条消息（或在设置中手动填写）');
        const content = [
            '## 🎮 农场动态（测试）',
            '> 这是一条来自 qq-farm-bot 的测试消息',
            '',
            '- **出售成功** ×3',
            '  出售 苹果x3，获得 120 金币',
            '',
            '- **收获作物** ×5',
            '  收获完成 5 块土地',
        ].join('\n');
        const result = await sendMarkdown(content);
        sentToday += 1;
        minuteBuckets.push(Date.now());
        checkDayReset();
        return result;
    }

    function getStatus(): RuntimeStatus {
        checkDayReset();
        return { ...status, queued: queue.length, mediaCount: mediaCache.size, users: getUsers() };
    }

    function selectUser(openid: string): QqBotPushConfig {
        const user = discoveredUsers.find(u => u.openid === openid);
        if (!user) throw new Error('未找到该用户，请先让对方给机器人发一条消息');
        config = { ...config, openid };
        status.openid = openid;
        persistUsers();
        return getConfig();
    }

    function startCapture(): void {
        if (!config.enabled) throw new Error('请先开启 QQ 机器人推送并填写 AppID/AppSecret');
        if (!ws || !status.connected) {
            sessionId = '';
            lastSeq = null;
            connectGateway().catch(() => {});
        }
    }

    const service = {
        start,
        stop,
        getConfig,
        setConfig,
        getStatus,
        enqueueLog,
        testSend,
        startCapture,
        flush,
        selectUser,
        sendOfflineNotification,
        getUsers,
        deleteUser,
    };
    activeService = service;
    return service;
}

function getActiveQqBotPush() {
    return activeService;
}

module.exports = { createQqBotPushService, getActiveQqBotPush };
