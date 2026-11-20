import { defineStore } from 'pinia'
import { ref } from 'vue'
import api from '../api'

export interface QqBotPushUser {
  openid: string
  nickname: string
  avatar: string
  lastSeenAt: number
}

export interface QqBotPushConfig {
  enabled: boolean
  appId: string
  appSecret: string
  openid: string
  eventKeys: string[]
  batchWindowSec: number
  dailyLimit: number
  minuteLimit: number
  includeIcons: boolean
  includeAvatars: boolean
  showAccount: boolean
  offlineNotify: boolean
  reloginEnabled: boolean
  /** 账号ID -> 接收用户 openid 列表 */
  accountRoutes: Record<string, string[]>
}

export interface QqBotPushStatus {
  connected: boolean
  connecting: boolean
  openid: string
  lastError: string
  tokenReady: boolean
  sentToday: number
  sentThisMinute: number
  queued: number
  mediaCount: number
  users: QqBotPushUser[]
  relogin: { active: boolean, accountId: string, stage: string, startedAt: number } | null
}

export interface QqBotEventOption {
  key: string
  label: string
  desc: string
}

export const QQ_BOT_EVENT_OPTIONS: QqBotEventOption[] = [
  { key: 'sell_success', label: '出售成功', desc: '仓库果实出售完成' },
  { key: '照顾好友', label: '照顾好友', desc: '给好友放草放虫等' },
  { key: '领取任务', label: '农场任务', desc: '农场每日任务领取' },
  { key: 'ranch_task_claim', label: '牧场任务', desc: '牧场每日任务领取' },
  { key: 'ranch_steal', label: '偷取好友牧场', desc: '从好友牧场偷取' },
  { key: '收获作物', label: '收获作物', desc: '农场作物收获完成' },
  { key: '神秘商人自动购买', label: '神秘商人', desc: '神秘商人自动购买成功' },
]

function createDefaultConfig(): QqBotPushConfig {
  return {
    enabled: false,
    appId: '',
    appSecret: '',
    openid: '',
    eventKeys: QQ_BOT_EVENT_OPTIONS.map(o => o.key),
    batchWindowSec: 60,
    dailyLimit: 800,
    minuteLimit: 8,
    includeIcons: true,
    includeAvatars: true,
    showAccount: true,
    offlineNotify: true,
    reloginEnabled: true,
    accountRoutes: {},
  }
}

export const useQqBotPushStore = defineStore('qqBotPush', () => {
  const config = ref<QqBotPushConfig>(createDefaultConfig())
  const status = ref<QqBotPushStatus>({
    connected: false,
    connecting: false,
    openid: '',
    lastError: '',
    tokenReady: false,
    sentToday: 0,
    sentThisMinute: 0,
    queued: 0,
    mediaCount: 0,
    users: [],
    relogin: null,
  })
  const loading = ref(false)
  let pollTimer: ReturnType<typeof setInterval> | null = null

  function applyData(data: any) {
    if (!data) return
    if (data.config) config.value = { ...config.value, ...data.config }
    if (data.status) status.value = { ...status.value, ...data.status }
  }

  async function fetch() {
    loading.value = true
    try {
      const { data } = await api.get('/api/settings/qq-bot-push')
      if (data?.ok) applyData(data.data)
      return data
    }
    finally {
      loading.value = false
    }
  }

  async function save(next: QqBotPushConfig) {
    const { data } = await api.post('/api/settings/qq-bot-push', next)
    if (data?.ok) {
      config.value = { ...config.value, ...data.data?.config }
      status.value = { ...status.value, ...data.data?.status }
    }
    return data
  }

  async function testSend() {
    const { data } = await api.post('/api/settings/qq-bot-push/test')
    return data
  }

  async function capture() {
    const { data } = await api.post('/api/settings/qq-bot-push/capture')
    return data
  }

  async function selectUser(openid: string) {
    const { data } = await api.post('/api/settings/qq-bot-push/select-user', { openid })
    if (data?.ok) config.value = { ...config.value, ...data.data?.config }
    return data
  }

  function startPolling() {
    if (pollTimer) return
    pollTimer = setInterval(() => { fetch().catch(() => {}) }, 5000)
  }

  function stopPolling() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null }
  }

  return {
    config,
    status,
    loading,
    fetch,
    save,
    testSend,
    capture,
    selectUser,
    startPolling,
    stopPolling,
  }
})
