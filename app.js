/* =========================================
 * 1) CONFIG / HELPERS BÁSICOS
 * ======================================= */
const BACKEND = () => (window.__BACKEND_URL__ || "").replace(/\/+$/, "")
const $ = (s) => document.querySelector(s)
const show = (sel) => { const el = $(sel); if (el) el.classList.remove("hidden") }
const hide = (sel) => { const el = $(sel); if (el) el.classList.add("hidden") }

// Idle callback
const rIC = (cb) => (window.requestIdleCallback ? window.requestIdleCallback(cb, { timeout: 200 }) : setTimeout(cb, 0))

// Limitador de concorrência
async function runLimited(tasks, limit = 8) {
  const results = []
  let i = 0
  const workers = new Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
    while (i < tasks.length) {
      const cur = i++
      try {
        results[cur] = await tasks[cur]()
      } catch {
        results[cur] = undefined
      }
    }
  })
  await Promise.all(workers)
  return results
}

function isMobile() {
  return window.matchMedia("(max-width:1023px)").matches
}
function setMobileMode(mode) {
  document.body.classList.remove("is-mobile-list", "is-mobile-chat")
  if (!isMobile()) return
  if (mode === "list") document.body.classList.add("is-mobile-list")
  if (mode === "chat") document.body.classList.add("is-mobile-chat")
}

function jwt() {
  return localStorage.getItem("luna_jwt") || ""
}

// --- decodifica payload do JWT com padding (corrigido) ---
function jwtPayload() {
  const t = jwt()
  if (!t || t.indexOf(".") < 0) return {}
  try {
    let b64 = t.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")
    b64 += "=".repeat((4 - (b64.length % 4)) % 4)
    const json = atob(b64)
    return JSON.parse(json)
  } catch {
    return {}
  }
}

function authHeaders() {
  const headers = { Authorization: "Bearer " + jwt() }
  const p = jwtPayload()
  const iid = p.instance_id || p.phone_number_id || p.pnid || p.sub || ""
  if (iid) headers["x-instance-id"] = String(iid)
  return headers
}

async function api(path, opts = {}) {
  const res = await fetch(BACKEND() + path, {
    headers: { "Content-Type": "application/json", ...authHeaders(), ...(opts.headers || {}) },
    ...opts,
  })
  if (!res.ok) {
    const t = await res.text().catch(() => "")
    throw new Error(`HTTP ${res.status}: ${t}`)
  }
  return res.json().catch(() => ({}))
}

function escapeHtml(s) {
  return String(s || "").replace(
    /[&<>"']/g,
    (m) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[m],
  )
}
function truncatePreview(s, max = 90) {
  const t = String(s || "")
    .replace(/\s+/g, " ")
    .trim()
  if (!t) return ""
  return t.length > max ? t.slice(0, max - 1).trimEnd() + "…" : t
}

/*
 * ============================================================
 *  Helper functions para integração com a Uazapi (instância)
 * ============================================================
 * Estas funções encapsulam as chamadas aos novos endpoints
 * implementados no backend (/api/uaz).  Elas permitem criar
 * uma instância na Uazapi (via token administrativo), iniciar
 * o processo de conexão (gerando um QR Code ou código de
 * pareamento) e realizar o polling do QR Code/status.  Além
 * disso, controlam a exibição de um modal simples de QR.
 */

// Cria uma instância na Uazapi e retorna { instance, status }
async function createUazInstance(name = "Minha Instância") {
  // utiliza JWT da conta (não o da instância)
  const jwtAcct = acctJwt()
  if (!jwtAcct) throw new Error("Usuário não autenticado")
  const res = await fetch(BACKEND() + "/api/uaz/instance/init", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer " + jwtAcct,
    },
    body: JSON.stringify({ name }),
  })
  if (!res.ok) throw new Error(await res.text().catch(() => ""))
  return res.json().catch(() => ({}))
}

// Aciona a conexão de uma instância existente (gera QR ou código de pareamento)
async function connectUazInstance(instance) {
  const jwtAcct = acctJwt()
  if (!jwtAcct) throw new Error("Usuário não autenticado")
  const res = await fetch(BACKEND() + "/api/uaz/instance/connect", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer " + jwtAcct,
    },
    body: JSON.stringify({ instance }),
  })
  if (!res.ok) throw new Error(await res.text().catch(() => ""))
  return res.json().catch(() => ({}))
}

// Consulta QR/status periodicamente até que a instância esteja conectada ou timeout expire
async function pollUazQR(instance, { interval = 3000, timeout = 180000, onUpdate } = {}) {
  const jwtAcct = acctJwt()
  if (!jwtAcct) throw new Error("Usuário não autenticado")
  const t0 = Date.now()
  while (Date.now() - t0 < timeout) {
    try {
      const url = new URL(BACKEND() + "/api/uaz/instance/qr")
      url.searchParams.set("instance", instance)
      const res = await fetch(url.toString(), {
        headers: { Authorization: "Bearer " + jwtAcct },
      })
      if (!res.ok) throw new Error(await res.text().catch(() => ""))
      const data = await res.json().catch(() => ({}))
      onUpdate && onUpdate(data)
      if (data.status === "connected") return data
    } catch (e) {
      console.error("pollUazQR", e)
    }
    await new Promise((r) => setTimeout(r, interval))
  }
  throw new Error("Tempo esgotado para conexão da instância")
}

// Exibe o modal de QR Code
function showQrModal() {
  const modal = document.getElementById("qr-modal")
  if (modal) modal.classList.remove("hidden")
}
function hideQrModal() {
  const modal = document.getElementById("qr-modal")
  if (modal) modal.classList.add("hidden")
}
// Atualiza conteúdo do modal de QR (status, imagem)
function updateQrModal({ status, qrcode, paircode }) {
  const statusEl = document.getElementById("qr-status")
  const imgEl = document.getElementById("qr-img")
  if (statusEl) {
    statusEl.textContent = status === "connected" ? "Conectado!" : status || "Aguardando..."
  }
  if (imgEl) {
    if (qrcode) {
      imgEl.src = qrcode
    } else if (paircode) {
      // gera código QR a partir do texto do paircode via serviço público
      imgEl.src = `https://api.qrserver.com/v1/create-qr-code/?size=240x240&data=${encodeURIComponent(paircode)}`
    }
  }
}

// Orquestra o fluxo completo de criação e pareamento da instância
async function startPairingFlow() {
  try {
    showQrModal()
    // Define um nome padrão baseado na data/hora para a instância
    const instName = `Instância ${new Date().toLocaleString()}`
    const { instance, token } = await createUazInstance(instName)
    // armazena o token da instância (caso precise em outro lugar)
    try {
      localStorage.setItem("luna_uaz_token", token)
    } catch {}
    // inicia o processo de conexão (gera QR)
    await connectUazInstance(instance)
    // realiza polling para obter QR e verificar se conectou
    await pollUazQR(instance, {
      interval: 3000,
      timeout: 300000,
      onUpdate: updateQrModal,
    })
    // Ao conectar, faz login no backend para gerar JWT de instância
    try {
      const res = await fetch(BACKEND() + "/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token }),
      })
      if (res.ok) {
        const d = await res.json()
        if (d?.jwt) {
          localStorage.setItem("luna_jwt", d.jwt)
        }
      }
    } catch (e) {
      console.error("Falha ao gerar JWT da instância", e)
    }
    // Instância conectada: fecha modal após breve delay
    setTimeout(() => hideQrModal(), 1500)
  } catch (e) {
    console.error(e)
    updateQrModal({ status: "Erro: " + (e.message || e), qrcode: null })
  }
}

/* ----- CACHE LOCAL (TTL) + DE-DUPE ----- */
const TTL = {
  NAME_IMAGE_HIT: 24 * 60 * 60 * 1000,
  NAME_IMAGE_MISS: 5 * 60 * 1000,
  PREVIEW: 10 * 60 * 1000,
}
const LStore = {
  get(key) {
    try {
      const raw = localStorage.getItem(key)
      if (!raw) return null
      const { v, exp } = JSON.parse(raw)
      if (exp && Date.now() > exp) {
        localStorage.removeItem(key)
        return null
      }
      return v
    } catch {
      return null
    }
  },
  set(key, val, ttlMs) {
    try {
      localStorage.setItem(key, JSON.stringify({ v: val, exp: Date.now() + (ttlMs || 0) }))
    } catch {}
  },
}
const inflight = new Map()
function once(key, fn) {
  if (inflight.has(key)) return inflight.get(key)
  const p = Promise.resolve()
    .then(fn)
    .finally(() => inflight.delete(key))
  inflight.set(key, p)
  return p
}
function prettyId(id = "") {
  const s = String(id)
  if (/@s\.whatsapp\.net$/i.test(s)) return s.replace(/@s\.whatsapp\.net$/i, "")
  return s
}

/* ========= NDJSON STREAM ========= */
async function* readNDJSONStream(resp) {
  const reader = resp.body.getReader()
  const decoder = new TextDecoder("utf-8")
  let buf = ""
  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buf += decoder.decode(value, { stream: true })
    let idx
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).trim()
      buf = buf.slice(idx + 1)
      if (!line) continue
      try {
        yield JSON.parse(line)
      } catch {}
    }
  }
  if (buf.trim()) {
    try {
      yield JSON.parse(buf.trim())
    } catch {}
  }
}

// ==== Conta (auth de e-mail/senha) ====
const ACCT_JWT_KEY = "luna_acct_jwt"

function acctJwt() {
  return localStorage.getItem(ACCT_JWT_KEY) || ""
}

function acctHeaders() {
  const h = { "Content-Type": "application/json" }
  const t = acctJwt()
  if (t) h.Authorization = "Bearer " + t
  return h
}

async function acctApi(path, opts = {}) {
  const res = await fetch(BACKEND() + path, { headers: { ...acctHeaders(), ...(opts.headers || {}) }, ...opts })
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text().catch(() => "")}`)
  return res.json().catch(() => ({}))
}

// ==== Billing System ====
let billingStatus = null

// Registra o trial para o usuário logado via e‑mail/senha.  Usa o token de
// conta (ACCT_JWT_KEY) como Authorization.  Mantém idempotência: se já
// existir registro, simplesmente retorna sucesso.
async function registerTrialUser() {
  try {
    // Dispara o trial no backend usando o JWT do usuário (conta).
    await acctApi("/api/billing/register-trial", { method: "POST" })
    console.log("[v1] Trial user registered successfully")
  } catch (e) {
    console.error("[v1] Failed to register user trial:", e)
  }
}

async function registerTrial() {
  try {
    // Determine se há um JWT de conta (usuário) para registrar o trial.  Se
    // existir, usamos o fluxo de conta; caso contrário, recorremos ao JWT da
    // instância (comportamento legado).
    if (acctJwt()) {
      await acctApi("/api/billing/register-trial", { method: "POST" })
      console.log("[v1] Trial registered successfully (user)")
    } else {
      await api("/api/billing/register-trial", { method: "POST" })
      console.log("[v0] Trial registered successfully (instance)")
    }
  } catch (e) {
    console.error("[v1] Failed to register trial:", e)
  }
}

async function checkBillingStatus() {
  try {
    // Busca o status de billing.  Se existir um JWT de conta (e‑mail/senha),
    // priorizamos esse fluxo, pois o billing passa a ser por usuário.  Caso
    // contrário, utilizamos o token da instância (legado).
    let res
    if (acctJwt()) {
      res = await acctApi("/api/billing/status")
    } else {
      res = await api("/api/billing/status")
    }
    console.log("[v1] Billing status response:", res)
    // A API retorna o objeto completo {ok, billing_key, status}.  Extraímos a
    // propriedade 'status', mas se não existir assumimos o próprio objeto.
    const st = res?.status ?? res
    billingStatus = st

    if (billingStatus?.require_payment === true) {
      showBillingModal()
      return false
    }

    updateBillingView()
    return true
  } catch (e) {
    console.error("[v1] Failed to check billing status:", e)
    return true // Permite o acesso em caso de erro
  }
}

function showBillingModal() {
  const modal = $("#billing-modal")
  if (modal) {
    modal.classList.remove("hidden")
  }
}

function hideBillingModal() {
  const modal = $("#billing-modal")
  if (modal) {
    modal.classList.add("hidden")
  }
}

function updateBillingView() {
  if (!billingStatus) return

  const currentPlan = $("#current-plan")
  const daysRemaining = $("#days-remaining")
  const trialUntil = $("#trial-until")
  const paidUntil = $("#paid-until")

  if (currentPlan) {
    currentPlan.textContent = billingStatus.plan || "Trial"
  }

  if (daysRemaining) {
    daysRemaining.textContent = String(billingStatus.days_left ?? "0")
  }

  if (trialUntil) {
    trialUntil.textContent = billingStatus.trial_ends_at ? new Date(billingStatus.trial_ends_at).toLocaleString() : "N/A"
  }

  if (paidUntil) {
    paidUntil.textContent = billingStatus.paid_until ? new Date(billingStatus.paid_until).toLocaleString() : "N/A"
  }
}

async function createCheckoutLink() {
  try {
    const btnEl = $("#btn-pay-getnet")
    if (btnEl) {
      btnEl.disabled = true
      btnEl.innerHTML = "<span>Processando...</span>"
    }

    // Checkout com JWT da INSTÂNCIA
    const response = await api("/api/billing/checkout-link", { method: "POST" })

    if (response?.url) {
      window.location.href = response.url
    } else {
      throw new Error("URL de pagamento não recebida")
    }
  } catch (e) {
    console.error("[v0] Failed to create checkout link:", e)
    alert("Erro ao processar pagamento. Tente novamente.")
  } finally {
    const btnEl = $("#btn-pay-getnet")
    if (btnEl) {
      btnEl.disabled = false
      btnEl.innerHTML = `
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="1" y="4" width="22" height="16" rx="2" ry="2"/>
          <line x1="1" y1="10" x2="23" y2="10"/>
        </svg>
        Pagar com GetNet
      `
    }
  }
}

function showConversasView() {
  hide("#billing-view")
  show(".chatbar")
  show("#messages")

  // Update menu active state
  document.querySelectorAll(".menu-item").forEach((item) => item.classList.remove("active"))
  $("#btn-conversas")?.classList.add("active")
}

function showBillingView() {
  hide(".chatbar")
  hide("#messages")
  show("#billing-view")

  // Update menu active state
  document.querySelectorAll(".menu-item").forEach((item) => item.classList.remove("active"))
  $("#btn-pagamentos")?.classList.add("active")

  // Load billing status when showing billing view
  checkBillingStatus()
}

/* =========================================
 * 2) STATE GLOBAL + ORDENAÇÃO POR RECÊNCIA
 * ======================================= */
const state = {
  chats: [],
  current: null,
  lastMsg: new Map(),
  lastMsgFromMe: new Map(),
  nameCache: new Map(),
  unread: new Map(),
  loadingChats: false,
  stages: new Map(),
  splash: { shown: false, timer: null, forceTimer: null },
  activeTab: "geral",
  listReqId: 0,
  lastTs: new Map(),
  orderDirty: false,
}

function toMs(x) {
  const n = Number(x || 0)
  if (String(x).length === 10) return n * 1000
  return isNaN(n) ? 0 : n
}
function updateLastActivity(chatid, ts) {
  if (!chatid) return
  const cur = state.lastTs.get(chatid) || 0
  const val = toMs(ts)
  if (val > cur) {
    state.lastTs.set(chatid, val)
    state.orderDirty = true
    scheduleReorder()
  }
}
let reorderTimer = null
function scheduleReorder() {
  if (reorderTimer) return
  reorderTimer = setTimeout(() => {
    reorderTimer = null
    if (!state.orderDirty) return
    state.orderDirty = false
    reorderChatList()
  }, 60)
}
function reorderChatList() {
  const list = document.getElementById("chat-list")
  if (!list) return
  const cards = Array.from(list.querySelectorAll(".chat-item"))
  if (!cards.length) return
  cards.sort((a, b) => {
    const ta = state.lastTs.get(a.dataset.chatid) || 0
    const tb = state.lastTs.get(b.dataset.chatid) || 0
    return tb - ta
  })
  cards.forEach((el) => list.appendChild(el))
}

/* =========================================
 * 3) FILA GLOBAL DE BACKGROUND
 * ======================================= */
const bgQueue = []
let bgRunning = false
function pushBg(task) {
  bgQueue.push(task)
  if (!bgRunning) runBg()
}
async function runBg() {
  bgRunning = true
  while (bgQueue.length) {
    const batch = bgQueue.splice(0, 16)
    await runLimited(batch, 8)
    await new Promise((r) => rIC(r))
  }
  bgRunning = false
}

/* =========================================
 * 4) PIPE DE STAGES + BULK + RESERVA
 * ======================================= */
const STAGES = ["contatos", "lead", "lead_quente"]
const STAGE_LABEL = { contatos: "Contatos", lead: "Lead", lead_quente: "Lead Quente" }

function normalizeStage(s) {
  const k = String(s || "")
    .toLowerCase()
    .trim()
  if (k.startsWith("contato")) return "contatos"
  if (k.includes("lead_quente") || k.includes("quente")) return "lead_quente"
  if (k === "lead") return "lead"
  return "contatos"
}
function getStage(chatid) {
  return state.stages.get(chatid) || null
}
function setStage(chatid, nextStage) {
  const stage = normalizeStage(nextStage)
  const rec = { stage, at: Date.now() }
  state.stages.set(chatid, rec)
  return rec
}

// ------- chamadas compat de endpoints -------
async function callLeadStatusEndpointsBulk(ids) {
  const attempts = [
    { path: "/api/lead-status/bulk", method: "POST", body: { chatids: ids } },
    { path: "/api/lead_status/bulk", method: "POST", body: { chatids: ids } },
    { path: "/api/lead-status/bulk", method: "POST", body: { ids } },
    { path: "/api/lead_status/bulk", method: "POST", body: { ids } },
  ]
  for (const a of attempts) {
    try {
      const res = await api(a.path, { method: a.method, body: JSON.stringify(a.body) })
      if (res && (Array.isArray(res.items) || Array.isArray(res.data))) {
        const arr = Array.isArray(res.items) ? res.items : res.data
        return arr.map((it) => ({
          chatid: it.chatid || it.id || it.number || it.chatId || "",
          stage: it.stage || it.status || it._stage || "",
        }))
      }
    } catch {}
  }
  return null
}
async function callLeadStatusSingle(chatid) {
  const attempts = [
    { path: "/api/lead-status", method: "POST", body: { chatid } },
    { path: "/api/lead_status", method: "POST", body: { chatid } },
    { path: `/api/lead-status?chatid=${encodeURIComponent(chatid)}`, method: "GET" },
    { path: `/api/lead_status?chatid=${encodeURIComponent(chatid)}`, method: "GET" },
  ]
  for (const a of attempts) {
    try {
      const res = await api(a.path, a.method === "GET" ? {} : { method: "POST", body: JSON.stringify(a.body) })
      const st = normalizeStage(res?.stage || res?.status || res?._stage || "")
      if (st) return { chatid, stage: st }
    } catch {}
  }
  return null
}

// ------- Bulk seed de estágios do banco -------
const _stageBuffer = new Set()
let _stageTimer = null

async function fetchStageNow(chatid) {
  if (!chatid) return
  try {
    const bulkOne = await callLeadStatusEndpointsBulk([chatid])
    let rec = bulkOne?.find((x) => (x.chatid || "") === chatid) || null
    if (!rec) rec = await callLeadStatusSingle(chatid)
    const st = normalizeStage(rec?.stage || "")
    if (st) {
      setStage(chatid, st)
      rIC(refreshStageCounters)
      const cur = state.current
      if (cur && (cur.wa_chatid || cur.chatid) === chatid) upsertStagePill(st)
    }
  } catch (e) {
    console.warn("fetchStageNow falhou:", e)
  }
}

function queueStageLookup(chatid) {
  if (!chatid || state.stages.has(chatid)) return
  _stageBuffer.add(chatid)
  if (_stageBuffer.size >= 12) {
    flushStageLookup()
  } else {
    if (_stageTimer) clearTimeout(_stageTimer)
    _stageTimer = setTimeout(flushStageLookup, 250)
  }
}

async function flushStageLookup() {
  const ids = Array.from(_stageBuffer)
  _stageBuffer.clear()
  if (_stageTimer) {
    clearTimeout(_stageTimer)
    _stageTimer = null
  }
  if (!ids.length) return

  try {
    const arr = await callLeadStatusEndpointsBulk(ids)
    const seen = new Set()
    if (Array.isArray(arr)) {
      for (const rec of arr) {
        const cid = rec?.chatid || ""
        const st = normalizeStage(rec?.stage || "")
        if (!cid || !st) continue
        setStage(cid, st)
        seen.add(cid)
      }
    }
    await runLimited(
      ids
        .filter((id) => !seen.has(id))
        .map((id) => async () => {
          await fetchStageNow(id)
        }),
      6,
    )

    rIC(refreshStageCounters)
    if (state.activeTab !== "geral") {
      const tab = state.activeTab
      rIC(() => loadStageTab(tab))
    }
  } catch (e) {
    console.error("lead-status bulk compat falhou:", e)
    await runLimited(
      ids.map((id) => async () => {
        await fetchStageNow(id)
      }),
      6,
    )
  }
}

/* =========================================
 * 5) CRM
 * ======================================= */
const CRM_STAGES = ["novo", "sem_resposta", "interessado", "em_negociacao", "fechou", "descartado"]
async function apiCRMViews() {
  return api("/api/crm/views")
}
async function apiCRMList(stage, limit = 100, offset = 0) {
  const qs = new URLSearchParams({ stage, limit, offset }).toString()
  return api("/api/crm/list?" + qs)
}
async function apiCRMSetStatus(chatid, stage, notes = "") {
  return api("/api/crm/status", { method: "POST", body: JSON.stringify({ chatid, stage, notes }) })
}
function ensureCRMBar() {}
async function refreshCRMCounters() {
  try {
    const data = await apiCRMViews()
    const counts = data?.counts || {}
    const el = document.querySelector(".crm-counters")
    if (el) {
      const parts = CRM_STAGES.map((s) => `${s.replace("_", " ")}: ${counts[s] || 0}`)
      el.textContent = parts.join(" • ")
    }
  } catch {}
}
async function loadCRMStage(stage) {
  const list = $("#chat-list")
  list.innerHTML = "<div class='hint'>Carregando visão CRM...</div>"
  try {
    const data = await apiCRMList(stage, 100, 0)
    const items = []
    for (const it of data?.items || []) {
      const ch = it.chat || {}
      if (!ch.wa_chatid && it.crm?.chatid) ch.wa_chatid = it.crm.chatid
      items.push(ch)
    }
    await progressiveRenderChats(items)
    await prefetchCards(items)
  } catch (e) {
    list.innerHTML = `<div class='error'>Falha ao carregar CRM: ${escapeHtml(e.message || "")}</div>`
  } finally {
    refreshCRMCounters()
  }
}
function attachCRMControlsToCard(el, ch) {}

// Etapas de login
function showStepAccount() {
  hide("#step-instance")
  hide("#step-register")
  show("#step-account")
}
function showStepInstance() {
  hide("#step-account")
  hide("#step-register")
  show("#step-instance")
}

// Mostra a etapa de registro de conta e esconde as demais (conta/instância).
function showStepRegister() {
  hide("#step-account")
  hide("#step-instance")
  show("#step-register")
}

// Login por e-mail/senha
async function acctLogin() {
  const email = $("#acct-email")?.value?.trim()
  const pass = $("#acct-pass")?.value?.trim()
  const msgEl = $("#acct-msg")
  const btnEl = $("#btn-acct-login")

  if (!email || !pass) {
    if (msgEl) msgEl.textContent = "Informe e-mail e senha."
    return
  }

  try {
    if (btnEl) {
      btnEl.disabled = true
      btnEl.textContent = "Entrando..."
    }
    if (msgEl) msgEl.textContent = ""

    // Backend novo: /api/users/login
    const r = await fetch(BACKEND() + "/api/users/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password: pass }),
    })
    if (!r.ok) throw new Error(await r.text())

    const data = await r.json()
    if (!data?.jwt) throw new Error("Resposta inválida do servidor.")

    localStorage.setItem(ACCT_JWT_KEY, data.jwt)

    // Registra o trial para o usuário logado (caso ainda não exista) e
    // obtém o status de billing.  Isso é feito antes de avançar para a
    // instância para que o modal de cobrança possa aparecer imediatamente
    // se o trial já estiver expirado.
    try {
      await registerTrialUser()
      await checkBillingStatus()
    } catch (e) {
      console.error(e)
    }

    // Avança para o passo do token da instância
    showStepInstance()
    $("#token")?.focus()
  } catch (e) {
    if (msgEl) msgEl.textContent = e?.message || "Falha no login."
  } finally {
    if (btnEl) {
      btnEl.disabled = false
      btnEl.textContent = "Entrar"
    }
  }
}

// Registro de conta (e‑mail/senha).  Esta função envia os dados para o
// endpoint /api/users/register.  Ao registrar a conta, um token JWT de
// usuário é retornado.  Em seguida, iniciamos o trial para esse usuário
// (caso ainda não exista) e verificamos o status de billing.  Por fim,
// avançamos para a etapa de instância.
async function acctRegister() {
  const email = $("#reg-email")?.value?.trim()
  const pass = $("#reg-pass")?.value?.trim()
  const msgEl = $("#reg-msg")
  const btnEl = $("#btn-acct-register")

  if (!email || !pass) {
    if (msgEl) msgEl.textContent = "Informe e-mail e senha."
    return
  }

  try {
    if (btnEl) {
      btnEl.disabled = true
      btnEl.textContent = "Criando..."
    }
    if (msgEl) msgEl.textContent = ""

    const r = await fetch(BACKEND() + "/api/users/register", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password: pass }),
    })
    if (!r.ok) throw new Error(await r.text())
    const data = await r.json()
    if (!data?.jwt) throw new Error("Resposta inválida do servidor.")
    // Armazena o JWT da conta
    localStorage.setItem(ACCT_JWT_KEY, data.jwt)
    // Inicia trial e lê status
    try {
      await registerTrialUser()
      await checkBillingStatus()
    } catch (e) {
      console.error(e)
    }
    // Em vez de solicitar o token da instância manualmente, criamos uma nova
    // instância automaticamente, iniciamos o pareamento com QR Code e, ao
    // final, realizamos o login da instância.  Em seguida, vamos direto
    // para a aplicação se o billing estiver em dia.
    try {
      await startPairingFlow()
    } catch (e) {
      console.error(e)
    }
    // Após o pareamento, verifica novamente se o usuário pode acessar e
    // carrega a aplicação.  A função switchToApp cuida do restante.
    try {
      const canAccess = await checkBillingStatus()
      if (canAccess) {
        switchToApp()
      }
    } catch (e) {
      console.error(e)
    }
  } catch (e) {
    if (msgEl) msgEl.textContent = e?.message || "Falha no registro."
  } finally {
    if (btnEl) {
      btnEl.disabled = false
      btnEl.textContent = "Criar Conta"
    }
  }
}

/* =========================================
 * 6) SPLASH / LOGIN / ROUTER
 * ======================================= */
function createSplash() {
  if (state.splash.shown) return
  const el = document.createElement("div")
  el.id = "luna-splash"
  el.className = "splash-screen"

  const logoContainer = document.createElement("div")
  logoContainer.className = "splash-logos-container"

  const lunaLogoDiv = document.createElement("div")
  lunaLogoDiv.className = "splash-logo-luna active"
  const lunaLogo = document.createElement("img")
  lunaLogo.src = "lunapngcinza.png"
  lunaLogo.alt = "Luna Logo"
  lunaLogo.className = "splash-logo"
  lunaLogoDiv.appendChild(lunaLogo)

  const helseniaLogoDiv = document.createElement("div")
  helseniaLogoDiv.className = "splash-logo-helsenia"
  const helseniaLogo = document.createElement("img")
  helseniaLogo.src = "logohelsenia.png"
  helseniaLogo.alt = "Helsenia Logo"
  helseniaLogo.className = "splash-logo"
  helseniaLogoDiv.appendChild(helseniaLogo)

  const progressContainer = document.createElement("div")
  progressContainer.className = "splash-progress-container"

  const progressBar = document.createElement("div")
  progressBar.className = "splash-progress-bar"
  progressContainer.appendChild(progressBar)

  logoContainer.appendChild(lunaLogoDiv)
  logoContainer.appendChild(helseniaLogoDiv)
  el.appendChild(logoContainer)
  el.appendChild(progressContainer)
  document.body.appendChild(el)

  setTimeout(() => {
    progressBar.classList.add("animate")
  }, 100)
  setTimeout(() => {
    lunaLogoDiv.classList.remove("active")
    setTimeout(() => {
      helseniaLogoDiv.classList.add("active")
      progressBar.classList.add("helsenia")
    }, 500)
  }, 4000)

  state.splash.shown = true
  state.splash.forceTimer = setTimeout(hideSplash, 8000)
}
function hideSplash() {
  const el = document.getElementById("luna-splash")
  if (el) {
    el.classList.add("fade-out")
    setTimeout(() => {
      el.remove()
    }, 800)
  }
  state.splash.shown = false
  if (state.splash.timer) {
    clearTimeout(state.splash.timer)
    state.splash.timer = null
  }
  if (state.splash.forceTimer) {
    clearTimeout(state.splash.forceTimer)
    state.splash.forceTimer = null
  }
}

async function doLogin() {
  // Precisa estar logado na conta:
  if (!acctJwt()) {
    showStepAccount()
    return
  }

  const token = $("#token")?.value?.trim()
  const msgEl = $("#msg")
  const btnEl = $("#btn-login")
  if (!token) {
    if (msgEl) msgEl.textContent = "Por favor, cole o token da instância"
    return
  }
  if (msgEl) msgEl.textContent = ""
  if (btnEl) {
    btnEl.disabled = true
    btnEl.innerHTML = "<span>Conectando...</span>"
  }
  try {
    const r = await fetch(BACKEND() + "/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token }),
    })
    if (!r.ok) throw new Error(await r.text())
    const data = await r.json()
    localStorage.setItem("luna_jwt", data.jwt)

    // Garante trial com JWT da instância
    try { await registerTrial() } catch {}

    const canAccess = await checkBillingStatus()
    if (canAccess) {
      switchToApp()
    }
  } catch (e) {
    console.error(e)
    if (msgEl) msgEl.textContent = "Token inválido. Verifique e tente novamente."
  } finally {
    if (btnEl) {
      btnEl.disabled = false
      btnEl.innerHTML =
        '<span>Conectar instância</span><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg>'
    }
  }
}
function ensureTopbar() {
  if (!$(".topbar")) {
    const tb = document.createElement("div")
    tb.className = "topbar"
    tb.style.display = "flex"
    tb.style.alignItems = "center"
    tb.style.gap = "8px"
    tb.style.padding = "8px 12px"
    const host = $("#app-view") || document.body
    host.prepend(tb)
  }
}
function switchToApp() {
  hide("#login-view")
  show("#app-view")
  setMobileMode("list")
  ensureTopbar()
  ensureCRMBar()
  ensureStageTabs()
  createSplash()

  showConversasView()

  loadChats().finally(() => {})
}
function ensureRoute() {
  const hasAcct = !!acctJwt()
  const hasInst = !!jwt()
  if (!hasAcct) {
    show("#login-view")
    hide("#app-view")
    showStepAccount()
    return
  }
  if (!hasInst) {
    show("#login-view")
    hide("#app-view")
    showStepInstance()
    return
  }
  switchToApp()
  try { if (typeof handleRoute === 'function') handleRoute() } catch(e) {}
}

/* =========================================
 * 7) AVATAR / NOME
 * ======================================= */
async function fetchNameImage(chatid, preview = true) {
  const key = `ni:${chatid}:${preview ? 1 : 0}`
  const hit = LStore.get(key)
  if (hit) return hit

  return once(key, async () => {
    try {
      const resp = await api("/api/name-image", {
        method: "POST",
        body: JSON.stringify({ number: chatid, preview }),
      })
      const hasData = !!(resp?.name || resp?.image || resp?.imagePreview)
      LStore.set(key, resp, hasData ? TTL.NAME_IMAGE_HIT : TTL.NAME_IMAGE_MISS)
      return resp
    } catch {
      const empty = { name: null, image: null, imagePreview: null }
      LStore.set(key, empty, TTL.NAME_IMAGE_MISS)
      return empty
    }
  })
}
function initialsOf(str) {
  const s = (str || "").trim()
  if (!s) return "??"
  const parts = s.split(/\s+/).slice(0, 2)
  return parts.map((p) => p[0]?.toUpperCase() || "").join("") || "??"
}

/* =========================================
 * 8) ABAS DE STAGE (UI)
 * ======================================= */
function ensureStageTabs() {
  const host = document.querySelector(".topbar")
  if (!host || host.querySelector(".stage-tabs")) return

  const bar = document.createElement("div")
  bar.className = "stage-tabs"
  bar.style.display = "flex"
  bar.style.gap = "8px"

  const addBtn = (key, label, onclick) => {
    const b = document.createElement("button")
    b.className = "btn"
    b.dataset.stage = key
    b.textContent = label
    b.onclick = () => {
      state.activeTab = key
      onclick()
      host.querySelectorAll(".stage-tabs .btn").forEach((x) => x.classList.remove("active"))
      b.classList.add("active")
      const mobileSelect = document.getElementById("mobile-stage-select")
      if (mobileSelect) mobileSelect.value = key
    }
    return b
  }

  const btnGeral = addBtn("geral", "Geral", () => loadChats())
  const btnCont = addBtn("contatos", "Contatos", () => loadStageTab("contatos"))
  const btnLead = addBtn("lead", "Lead", () => loadStageTab("lead"))
  const btnLQ = addBtn("lead_quente", "Lead Quente", () => loadStageTab("lead_quente"))

  bar.appendChild(btnGeral)
  bar.appendChild(btnCont)
  bar.appendChild(btnLead)
  bar.appendChild(btnLQ)

  const counters = document.createElement("div")
  counters.className = "stage-counters"
  counters.style.marginLeft = "8px"
  counters.style.color = "var(--sub2)"
  counters.style.fontSize = "12px"

  host.appendChild(bar)
  host.appendChild(counters)

  const mobileSelect = document.getElementById("mobile-stage-select")
  if (mobileSelect) {
    mobileSelect.onchange = (e) => {
      const key = e.target.value
      state.activeTab = key
      switch (key) {
        case "geral":
          loadChats()
          break
        case "contatos":
          loadStageTab("contatos")
          break
        case "lead":
          loadStageTab("lead")
          break
        case "lead_quente":
          loadStageTab("lead_quente")
          break
      }
      const btn = host.querySelector(`.stage-tabs .btn[data-stage="${key}"]`)
      if (btn) {
        host.querySelectorAll(".stage-tabs .btn").forEach((x) => x.classList.remove("active"))
        btn.classList.add("active")
      }
    }
  }

  setTimeout(() => {
    const btn = host.querySelector(`.stage-tabs .btn[data-stage="${state.activeTab}"]`) || btnGeral
    btn.click()
  }, 0)
}

function refreshStageCounters() {
  const counts = { contatos: 0, lead: 0, lead_quente: 0 }
  state.chats.forEach((ch) => {
    const chatid = ch.wa_chatid || ch.chatid || ch.wa_fastid || ch.wa_id || ""
    const st = getStage(chatid)?.stage || "contatos"
    if (counts[st] !== undefined) counts[st]++
  })

  const el = document.querySelector(".stage-counters")
  if (el) el.textContent = `contatos: ${counts.contatos} • lead: ${counts.lead} • lead quente: ${counts.lead_quente}`

  const mobileContatos = document.getElementById("mobile-counter-contatos")
  const mobileLead = document.getElementById("mobile-counter-lead")
  const mobileLeadQuente = document.getElementById("mobile-counter-lead_quente")
  if (mobileContatos) mobileContatos.textContent = counts.contatos
  if (mobileLead) mobileLead.textContent = counts.lead
  if (mobileLeadQuente) mobileLeadQuente.textContent = counts.lead_quente

  if (el && !document.getElementById("verification-progress")) {
    const progressEl = document.createElement("div")
    progressEl.id = "verification-progress"
    progressEl.className = "verification-progress hidden"
    progressEl.innerHTML = `
      <div class="verification-content">
        <div class="verification-text">
          <span class="verification-label">Verificando classificações...</span>
          <span class="verification-counter">0/0 contatos</span>
        </div>
        <div class="verification-bar"><div class="verification-fill"></div></div>
      </div>`
    el.parentNode.appendChild(progressEl)
  }
}

async function loadStageTab(stageKey) {
  const reqId = ++state.listReqId
  const list = $("#chat-list")
  list.innerHTML = "<div class='hint'>Carregando…</div>"

  const filtered = state.chats.filter((ch) => {
    const chatid = ch.wa_chatid || ch.chatid || ch.wa_fastid || ch.wa_id || ""
    const st = getStage(chatid)?.stage || "contatos"
    return st === stageKey
  })

  await progressiveRenderChats(filtered, reqId)
  await prefetchCards(filtered)
}

/* =========================================
 * 9) CHATS (stream + prefetch + ordenação)
 * ======================================= */
async function loadChats() {
  if (state.loadingChats) return
  state.loadingChats = true

  const reqId = ++state.listReqId
  const startTab = state.activeTab

  const list = $("#chat-list")
  if (list) list.innerHTML = "<div class='hint'>Carregando conversas...</div>"

  try {
    const res = await fetch(BACKEND() + "/api/chats/stream", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify({ operator: "AND", sort: "-wa_lastMsgTimestamp" }),
    })
    if (!res.ok || !res.body) throw new Error("Falha no stream de conversas")

    if (reqId !== state.listReqId) return
    if (list) list.innerHTML = ""
    state.chats = []

    for await (const item of readNDJSONStream(res)) {
      if (item?.error) continue

      state.chats.push(item)

      const baseTs = item.wa_lastMsgTimestamp || item.messageTimestamp || item.updatedAt || 0
      const id = item.wa_chatid || item.chatid || item.wa_fastid || item.wa_id || ""
      updateLastActivity(id, baseTs)

      const stageFromStream = normalizeStage(item?._stage || item?.stage || item?.status || "")
      if (id && stageFromStream) {
        setStage(id, stageFromStream)
        rIC(refreshStageCounters)
        if (state.activeTab !== "geral") {
          const tab = state.activeTab
          rIC(() => loadStageTab(tab))
        }
        if (state.current && (state.current.wa_chatid || state.current.chatid) === id) {
          upsertStagePill(stageFromStream)
        }
      }

      if (state.activeTab === "geral" && startTab === "geral" && reqId === state.listReqId) {
        const curList = $("#chat-list")
        if (curList) appendChatSkeleton(curList, item)
      }

      if (!id) continue

      queueStageLookup(id)

      pushBg(async () => {
        // nome/imagem
        try {
          if (!state.nameCache.has(id)) {
            const resp = await fetchNameImage(id)
            state.nameCache.set(id, resp || {})
            const cardEl = document.querySelector(`.chat-item[data-chatid="${CSS.escape(id)}"]`)
            if (cardEl) hydrateChatCard(item)
          }
        } catch {}

        // preview última mensagem (usa cache)
        try {
          const pvKey = `pv:${id}`
          const pvHit = LStore.get(pvKey)
          if (pvHit && !state.lastMsg.has(id)) {
            state.lastMsg.set(id, pvHit.text || "")
            state.lastMsgFromMe.set(id, !!pvHit.fromMe)
            const card = document.querySelector(`.chat-item[data-chatid="${CSS.escape(id)}"] .preview`)
            if (card) {
              const txt = pvHit.text
                ? (pvHit.fromMe ? "Você: " : "") + truncatePreview(pvHit.text, 90)
                : "Sem mensagens"
              card.textContent = txt
              card.title = pvHit.text ? (pvHit.fromMe ? "Você: " : "") + pvHit.text : "Sem mensagens"
            }
          }

          const latest = await api("/api/messages", {
            method: "POST",
            body: JSON.stringify({ chatid: id, limit: 1, sort: "-messageTimestamp" }),
          })
          const last = Array.isArray(latest?.items) ? latest.items[0] : null
          const pv = last
            ? (last.text || last.caption || last?.message?.text || last?.message?.conversation || last?.body || "")
                .replace(/\s+/g, " ")
                .trim()
            : (item.wa_lastMessageText || "").replace(/\s+/g, " ").trim()
          const fromMe = last ? isFromMe(last) : false

          state.lastMsg.set(id, pv || "")
          state.lastMsgFromMe.set(id, fromMe)
          LStore.set(pvKey, { text: pv || "", fromMe }, TTL.PREVIEW)

          const card = document.querySelector(`.chat-item[data-chatid="${CSS.escape(id)}"] .preview`)
          if (card) {
            const txt = pv ? (fromMe ? "Você: " : "") + truncatePreview(pv, 90) : "Sem mensagens"
            card.textContent = txt
            card.title = pv ? (fromMe ? "Você: " : "") + pv : "Sem mensagens"
          }
          if (last) {
            updateLastActivity(id, last.messageTimestamp || last.timestamp || last.t || Date.now())
            const tEl = document.querySelector(`.chat-item[data-chatid="${CSS.escape(id)}"] .time`)
            if (tEl) tEl.textContent = formatTime(last.messageTimestamp || last.timestamp || last.t || "")
          }
        } catch {
          const card = document.querySelector(`.chat-item[data-chatid="${CSS.escape(id)}"] .preview`)
          if (card) {
            card.textContent = "Sem mensagens"
            card.title = "Sem mensagens"
          }
          const base = state.chats.find((c) => (c.wa_chatid || c.chatid || c.wa_fastid || c.wa_id || "") === id) || {}
          updateLastActivity(id, base.wa_lastMsgTimestamp || base.messageTimestamp || base.updatedAt || 0)
        }
      })
    }

    await flushStageLookup()

    if (state.activeTab !== "geral") await loadStageTab(state.activeTab)

    try {
      await api("/api/crm/sync", { method: "POST", body: JSON.stringify({ limit: 1000 }) })
      refreshCRMCounters()
    } catch {}
  } catch (e) {
    console.error(e)
    if (list && reqId === state.listReqId)
      list.innerHTML = `<div class='error'>${escapeHtml(e.message || "Falha ao carregar conversas")}</div>`
  } finally {
    if (reqId === state.listReqId) state.loadingChats = false
  }
}

/* =========================================
 * 10) LISTA (render + cards)
 * ======================================= */
async function progressiveRenderChats(chats, reqId = null) {
  const list = $("#chat-list")
  if (!list) return
  list.innerHTML = ""
  if (chats.length === 0) {
    if (reqId !== null && reqId !== state.listReqId) return
    list.innerHTML = "<div class='hint'>Nenhuma conversa encontrada</div>"
    return
  }
  const BATCH = 14
  for (let i = 0; i < chats.length; i += BATCH) {
    if (reqId !== null && reqId !== state.listReqId) return
    const slice = chats.slice(i, i + BATCH)
    slice.forEach((ch) => {
      if (reqId !== null && reqId !== state.listReqId) return
      appendChatSkeleton(list, ch)
    })
    await new Promise((r) => rIC(r))
  }
  chats.forEach((ch) => {
    if (reqId !== null && reqId !== state.listReqId) return
    hydrateChatCard(ch)
  })
  reorderChatList()
}

function appendChatSkeleton(list, ch) {
  const el = document.createElement("div")
  el.className = "chat-item"
  const chatid = ch.wa_chatid || ch.chatid || ch.wa_fastid || ch.wa_id || ""
  el.dataset.chatid = chatid
  el.onclick = () => openChat(ch)

  const avatar = document.createElement("div")
  avatar.className = "avatar"
  avatar.textContent = "··"

  const main = document.createElement("div")
  main.className = "chat-main"

  const top = document.createElement("div")
  top.className = "row1"
  const nm = document.createElement("div")
  nm.className = "name"
  nm.textContent = (ch.wa_contactName || ch.name || prettyId(el.dataset.chatid) || "Contato").toString()
  const tm = document.createElement("div")
  tm.className = "time"
  const lastTs = ch.wa_lastMsgTimestamp || ch.messageTimestamp || ""
  tm.textContent = lastTs ? formatTime(lastTs) : ""
  top.appendChild(nm)
  top.appendChild(tm)

  const bottom = document.createElement("div")
  bottom.className = "row2"
  const preview = document.createElement("div")
  preview.className = "preview"
  const pv = (ch.wa_lastMessageText || "").replace(/\s+/g, " ").trim()
  preview.textContent = pv ? truncatePreview(pv, 90) : "Carregando..."
  preview.title = pv || "Carregando..."
  // fallback para não travar em 'Carregando...'
  setTimeout(() => {
    if (preview && preview.textContent === 'Carregando...') {
      preview.textContent = 'Sem mensagens'
      preview.title = 'Sem mensagens'
    }
  }, 5000)

  const unread = document.createElement("span")
  unread.className = "badge"
  const count = state.unread.get(el.dataset.chatid) || ch.wa_unreadCount || 0
  if (count > 0) unread.textContent = count
  else unread.style.display = "none"

  bottom.appendChild(preview)
  bottom.appendChild(unread)
  main.appendChild(top)
  main.appendChild(bottom)
  el.appendChild(avatar)
  el.appendChild(main)
  list.appendChild(el)

  const baseTs = ch.wa_lastMsgTimestamp || ch.messageTimestamp || ch.updatedAt || 0
  updateLastActivity(el.dataset.chatid, baseTs)

  queueStageLookup(chatid)
  setTimeout(() => {
    if (!getStage(chatid)) fetchStageNow(chatid)
  }, 800)

  attachCRMControlsToCard(el, ch)
}

function hydrateChatCard(ch) {
  const chatid = ch.wa_chatid || ch.chatid || ch.wa_fastid || ch.wa_id || ""
  const cache = state.nameCache.get(chatid)
  if (!chatid || !cache) return
  const el = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"]`)
  if (!el) return

  const avatar = el.querySelector(".avatar")
  const nameEl = el.querySelector(".name")
  if (cache.imagePreview || cache.image) {
    avatar.innerHTML = ""
    const img = document.createElement("img")
    img.src = cache.imagePreview || cache.image
    img.alt = "avatar"
    avatar.appendChild(img)
  } else {
    avatar.textContent = initialsOf(cache.name || nameEl.textContent || prettyId(chatid))
  }
  if (cache.name) nameEl.textContent = cache.name
  else nameEl.textContent = nameEl.textContent || prettyId(chatid)
}

/* =========================================
 * 11) PREFETCH (nomes/últimas/classificação leve)
 * ======================================= */
async function prefetchCards(items) {
  const progressEl = document.getElementById("verification-progress")
  const counterEl = progressEl?.querySelector(".verification-counter")
  const fillEl = progressEl?.querySelector(".verification-fill")

  if (progressEl && items.length > 0) {
    progressEl.classList.remove("hidden")
    if (counterEl) counterEl.textContent = `0/${items.length} contatos`
    if (fillEl) fillEl.style.width = "0%"
  }

  let completed = 0

  const tasks = items.map((ch) => {
    const chatid = ch.wa_chatid || ch.chatid || ch.wa_fastid || ch.wa_id || ""
    return async () => {
      if (!chatid) return

      queueStageLookup(chatid)

      if (!state.nameCache.has(chatid)) {
        try {
          const resp = await fetchNameImage(chatid)
          state.nameCache.set(chatid, resp)
          hydrateChatCard(ch)
        } catch {}
      }
      if (!state.lastMsg.has(chatid) && !ch.wa_lastMessageText) {
        try {
          const pvKey = `pv:${chatid}`
          const pvHit = LStore.get(pvKey)
          if (pvHit) {
            state.lastMsg.set(chatid, pvHit.text || "")
            state.lastMsgFromMe.set(chatid, !!pvHit.fromMe)
            const card = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"] .preview`)
            if (card) {
              const txt =
                (pvHit.fromMe ? "Você: " : "") + (pvHit.text ? truncatePreview(pvHit.text, 90) : "Sem mensagens")
              card.textContent = txt
              card.title = (pvHit.fromMe ? "Você: " : "") + (pvHit.text || "")
            }
            const tEl = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"] .time`)
            if (tEl && ch.wa_lastMsgTimestamp) tEl.textContent = formatTime(ch.wa_lastMsgTimestamp)
          } else {
            const data = await api("/api/messages", {
              method: "POST",
              body: JSON.stringify({ chatid, limit: 1, sort: "-messageTimestamp" }),
            })
            const last = Array.isArray(data?.items) ? data.items[0] : null
            if (last) {
              const pv = (
                last.text ||
                last.caption ||
                last?.message?.text ||
                last?.message?.conversation ||
                last?.body ||
                ""
              )
                .replace(/\s+/g, " ")
                .trim()
              state.lastMsg.set(chatid, pv)
              const fromMe = isFromMe(last)
              state.lastMsgFromMe.set(chatid, fromMe)
              LStore.set(pvKey, { text: pv || "", fromMe }, TTL.PREVIEW)
              const card = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"] .preview`)
              if (card) {
                const txt = (fromMe ? "Você: " : "") + (pv ? truncatePreview(pv, 90) : "Sem mensagens")
                card.textContent = txt
                card.title = (fromMe ? "Você: " : "") + pv
              }
              const tEl = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"] .time`)
              if (tEl) tEl.textContent = formatTime(last.messageTimestamp || last.timestamp || last.t || "")
              updateLastActivity(chatid, last.messageTimestamp || last.timestamp || last.t || Date.now())
            } else {
              const card = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"] .preview`)
              if (card) {
                card.textContent = "Sem mensagens"
                card.title = "Sem mensagens"
              }
              updateLastActivity(chatid, ch.wa_lastMsgTimestamp || ch.messageTimestamp || ch.updatedAt || 0)
            }
          }
        } catch {}
      }

      completed++
      if (counterEl) counterEl.textContent = `${completed}/${items.length} contatos`
      if (fillEl) fillEl.style.width = `${(completed / items.length) * 100}%`
    }
  })

  const CHUNK = 16
  for (let i = 0; i < tasks.length; i += CHUNK) {
    const slice = tasks.slice(i, i + CHUNK)
    await runLimited(slice, 8)
    await new Promise((r) => rIC(r))
  }

  await flushStageLookup()

  if (progressEl) {
    setTimeout(() => {
      progressEl.classList.add("hidden")
    }, 1000)
  }
}

/* =========================================
 * 12) FORMATAÇÃO DE HORA
 * ======================================= */
function formatTime(ts) {
  const val = toMs(ts)
  if (!val) return ""
  try {
    const d = new Date(val)
    const now = new Date()
    const diffMs = now - d
    const diffH = diffMs / 36e5
    if (diffH < 24) {
      const hh = String(d.getHours()).padStart(2, "0")
      const mm = String(d.getMinutes()).padStart(2, "0")
      return `${hh}:${mm}`
    }
    const diffD = Math.floor(diffMs / 86400000)
    return `${diffD}d`
  } catch {
    return ""
  }
}

/* =========================================
 * 13) ABRIR CHAT / CARREGAR MENSAGENS
 * ======================================= */
async function openChat(ch) {
  state.current = ch
  const title = $("#chat-header")
  const status = $(".chat-status")
  const chatid = ch.wa_chatid || ch.chatid || ch.wa_fastid || ch.wa_id || ""

  const cache = state.nameCache.get(chatid) || {}
  const nm = (cache.name || ch.wa_contactName || ch.name || prettyId(chatid) || "Chat").toString()

  if (title) title.textContent = nm
  if (status) status.textContent = "Carregando mensagens..."

  setMobileMode("chat")
  await loadMessages(chatid)

  const known = getStage(chatid)
  if (known) {
    upsertStagePill(known.stage)
  } else {
    await fetchStageNow(chatid)
    const st = getStage(chatid)
    if (st) upsertStagePill(st.stage)
  }

  if (status) status.textContent = "Online"
}

function tsOf(m) {
  return Number(m?.messageTimestamp ?? m?.timestamp ?? m?.t ?? m?.message?.messageTimestamp ?? 0)
}

async function classifyInstant(chatid, items) {
  const got = await getOrInitStage(chatid, { messages: items || [] })
  if (got?.stage) {
    upsertStagePill(got.stage)
    refreshStageCounters()
    return got
  }
  return null
}

async function getOrInitStage(chatid, { messages = [] } = {}) {
  const c = getStage(chatid)
  if (c?.stage) return c

  try {
    const one = await callLeadStatusSingle(chatid)
    if (one?.stage) {
      const rec = setStage(chatid, one.stage)
      return rec
    }
  } catch {}

  try {
    if (!messages || !messages.length) {
      const data = await api("/api/messages", {
        method: "POST",
        body: JSON.stringify({ chatid, limit: 50, sort: "-messageTimestamp" }),
      })
      messages = Array.isArray(data?.items) ? data.items : []
      if (data?.stage) {
        const rec = setStage(chatid, data.stage)
        return rec
      }
    } else {
      const data = await api("/api/media/stage/classify", {
        method: "POST",
        body: JSON.stringify({ chatid, messages }),
      })
      if (data?.stage) {
        const rec = setStage(chatid, data.stage)
        return rec
      }
    }
  } catch {}

  return getStage(chatid) || null
}

async function loadMessages(chatid) {
  const pane = $("#messages")
  if (pane) pane.innerHTML = "<div class='hint'>Carregando mensagens...</div>"
  try {
    const data = await api("/api/messages", {
      method: "POST",
      body: JSON.stringify({ chatid, limit: 200, sort: "-messageTimestamp" }),
    })
    let items = Array.isArray(data?.items) ? data.items : []

    items = items.slice().sort((a, b) => tsOf(a) - tsOf(b))

    await classifyInstant(chatid, items)
    await progressiveRenderMessages(items)

    const last = items[items.length - 1]
    const pv = (last?.text || last?.caption || last?.message?.text || last?.message?.conversation || last?.body || "")
      .replace(/\s+/g, " ")
      .trim()
    if (pv) state.lastMsg.set(chatid, pv)
    const fromMeFlag = isFromMe(last || {})
    state.lastMsgFromMe.set(chatid, fromMeFlag)
    LStore.set(`pv:${chatid}`, { text: pv || "", fromMe: fromMeFlag }, TTL.PREVIEW)

    if (last) {
      updateLastActivity(chatid, last.messageTimestamp || last.timestamp || last.t || Date.now())
      const tEl = document.querySelector(`.chat-item[data-chatid="${CSS.escape(chatid)}"] .time`)
      if (tEl) tEl.textContent = formatTime(last.messageTimestamp || last.timestamp || last.t || "")
    }
  } catch (e) {
    console.error(e)
    if (pane) pane.innerHTML = `<div class='error'>Falha ao carregar mensagens: ${escapeHtml(e.message || "")}</div>`
  }
}

/* =========================================
 * 14) RENDERIZAÇÃO DE MENSAGENS
 * ======================================= */
async function progressiveRenderMessages(msgs) {
  const pane = $("#messages")
  if (!pane) return
  pane.innerHTML = ""

  if (!msgs.length) {
    pane.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">💬</div>
        <h3>Nenhuma mensagem</h3>
        <p>Esta conversa ainda não possui mensagens</p>
      </div>`
    return
  }

  const BATCH = 12
  for (let i = 0; i < msgs.length; i += BATCH) {
    const slice = msgs.slice(i, i + BATCH)
    slice.forEach((m) => {
      try {
        appendMessageBubble(pane, m)
      } catch {
        const el = document.createElement("div")
        el.className = "msg you"
        el.innerHTML =
          "(mensagem não suportada)<small style='display:block;opacity:.7;margin-top:6px'>Erro ao renderizar</small>"
        pane.appendChild(el)
      }
    })
    await new Promise((r) => rIC(r))
    pane.scrollTop = pane.scrollHeight
  }
}

/* =========================================
 * 15) MÍDIA / INTERATIVOS / REPLIES
 * ======================================= */
function pickMediaInfo(m) {
  const mm = m.message || m

  const mime =
    m.mimetype ||
    m.mime ||
    mm?.imageMessage?.mimetype ||
    mm?.videoMessage?.mimetype ||
    mm?.documentMessage?.mimetype ||
    mm?.audioMessage?.mimetype ||
    (mm?.stickerMessage ? "image/webp" : "") ||
    ""

  const url =
    m.mediaUrl ||
    m.url ||
    m.fileUrl ||
    m.downloadUrl ||
    m.image ||
    m.video ||
    mm?.imageMessage?.url ||
    mm?.videoMessage?.url ||
    mm?.documentMessage?.url ||
    mm?.stickerMessage?.url ||
    mm?.audioMessage?.url ||
    ""

  const dataUrl =
    m.dataUrl ||
    mm?.imageMessage?.dataUrl ||
    mm?.videoMessage?.dataUrl ||
    mm?.documentMessage?.dataUrl ||
    mm?.stickerMessage?.dataUrl ||
    mm?.audioMessage?.dataUrl ||
    ""

  const caption =
    m.caption ||
    mm?.imageMessage?.caption ||
    mm?.videoMessage?.caption ||
    mm?.documentMessage?.caption ||
    mm?.documentMessage?.fileName ||
    m.text ||
    mm?.conversation ||
    m.body ||
    ""

  return {
    mime: String(mime || ""),
    url: String(url || ""),
    dataUrl: String(dataUrl || ""),
    caption: String(caption || ""),
  }
}

async function fetchMediaBlobViaProxy(rawUrl) {
  const q = encodeURIComponent(String(rawUrl || ""))
  const r = await fetch(BACKEND() + "/api/media/proxy?u=" + q, { method: "GET", headers: { ...authHeaders() } })
  if (!r.ok) throw new Error("Falha ao baixar mídia")
  return await r.blob()
}

// Reply preview
function renderReplyPreview(container, m) {
  const ctx =
    m?.message?.extendedTextMessage?.contextInfo ||
    m?.message?.imageMessage?.contextInfo ||
    m?.message?.videoMessage?.contextInfo ||
    m?.message?.stickerMessage?.contextInfo ||
    m?.message?.documentMessage?.contextInfo ||
    m?.message?.audioMessage?.contextInfo ||
    m?.contextInfo ||
    {}

  const qm = ctx.quotedMessage || m?.quotedMsg || m?.quoted_message || null
  if (!qm) return
  const qt =
    qm?.extendedTextMessage?.text ||
    qm?.conversation ||
    qm?.imageMessage?.caption ||
    qm?.videoMessage?.caption ||
    qm?.documentMessage?.caption ||
    qm?.text ||
    ""
  const box = document.createElement("div")
  box.className = "bubble-quote"
  box.style.borderLeft = "3px solid var(--muted, #ccc)"
  box.style.padding = "6px 8px"
  box.style.marginBottom = "6px"
  box.style.opacity = ".8"
  box.style.fontSize = "12px"
  box.textContent = qt || "(mensagem citada)"
  container.appendChild(box)
}

// Interativos
function renderInteractive(container, m) {
  const listMsg = m?.message?.listMessage
  const btnsMsg = m?.message?.buttonsMessage || m?.message?.templateMessage?.hydratedTemplate
  const listResp = m?.message?.listResponseMessage
  const btnResp = m?.message?.buttonsResponseMessage

  if (listMsg) {
    const card = document.createElement("div")
    card.className = "bubble-actions"
    card.style.border = "1px solid var(--muted,#ddd)"
    card.style.borderRadius = "8px"
    card.style.padding = "8px"
    card.style.maxWidth = "320px"
    if (listMsg.title) {
      const h = document.createElement("div")
      h.style.fontWeight = "600"
      h.style.marginBottom = "6px"
      h.textContent = listMsg.title
      card.appendChild(h)
    }
    if (listMsg.description) {
      const d = document.createElement("div")
      d.style.fontSize = "12px"
      d.style.opacity = ".85"
      d.style.marginBottom = "6px"
      d.textContent = listMsg.description
      card.appendChild(d)
    }
    ;(listMsg.sections || []).forEach((sec) => {
      if (sec.title) {
        const st = document.createElement("div")
        st.style.margin = "6px 0 4px"
        st.style.fontSize = "12px"
        st.style.opacity = ".8"
        st.textContent = sec.title
        card.appendChild(st)
      }
      ;(sec.rows || []).forEach((row) => {
        const opt = document.createElement("div")
        opt.style.padding = "6px 8px"
        opt.style.border = "1px solid var(--muted,#eee)"
        opt.style.borderRadius = "6px"
        opt.style.marginBottom = "6px"
        opt.textContent = row.title || row.id || "(opção)"
        card.appendChild(opt)
      })
    })
    container.appendChild(card)
    return true
  }

  if (btnsMsg) {
    const card = document.createElement("div")
    card.className = "bubble-actions"
    card.style.border = "1px solid var(--muted,#ddd)"
    card.style.borderRadius = "8px"
    card.style.padding = "8px"
    card.style.maxWidth = "320px"
    const title = btnsMsg.title || btnsMsg.hydratedTitle
    const text = btnsMsg.text || btnsMsg.hydratedContentText
    if (title) {
      const h = document.createElement("div")
      h.style.fontWeight = "600"
      h.style.marginBottom = "6px"
      h.textContent = title
      card.appendChild(h)
    }
    if (text) {
      const d = document.createElement("div")
      d.style.fontSize = "12px"
      d.style.opacity = ".85"
      d.style.marginBottom = "6px"
      d.textContent = text
      card.appendChild(d)
    }
    const buttons = btnsMsg.buttons || btnsMsg.hydratedButtons || []
    buttons.forEach((b) => {
      const lbl =
        b?.quickReplyButton?.displayText ||
        b?.urlButton?.displayText ||
        b?.callButton?.displayText ||
        b?.displayText ||
        "Opção"
      const btn = document.createElement("div")
      btn.textContent = lbl
      btn.style.display = "inline-block"
      btn.style.padding = "6px 10px"
      btn.style.border = "1px solid var(--muted,#eee)"
      btn.style.borderRadius = "999px"
      btn.style.margin = "4px 6px 0 0"
      btn.style.fontSize = "12px"
      btn.style.opacity = ".9"
      card.appendChild(btn)
    })
    container.appendChild(card)
    return true
  }

  if (listResp) {
    const picked = listResp?.singleSelectReply?.selectedRowId || listResp?.title || "(resposta de lista)"
    const tag = document.createElement("div")
    tag.style.display = "inline-block"
    tag.style.padding = "6px 10px"
    tag.style.border = "1px solid var(--muted,#ddd)"
    tag.style.borderRadius = "6px"
    tag.style.fontSize = "12px"
    tag.textContent = picked
    container.appendChild(tag)
    return true
  }

  if (btnResp) {
    const picked = btnResp?.selectedDisplayText || btnResp?.selectedButtonId || "(resposta)"
    const tag = document.createElement("div")
    tag.style.display = "inline-block"
    tag.style.padding = "6px 10px"
    tag.style.border = "1px solid var(--muted,#ddd)"
    tag.style.borderRadius = "6px"
    tag.style.fontSize = "12px"
    tag.textContent = picked
    container.appendChild(tag)
    return true
  }

  return false
}

/* =========================================
 * 16) AUTORIA
 * ======================================= */
function isFromMe(m) {
  return !!(
    m?.fromMe ||
    m?.fromme ||
    m?.from_me ||
    m?.key?.fromMe ||
    m?.message?.key?.fromMe ||
    m?.sender?.fromMe ||
    (typeof m?.participant === "string" && /(:me|@s\.whatsapp\.net)$/i.test(m.participant)) ||
    (typeof m?.author === "string" &&
      (/(:me)$/i.test(m.author) || /@s\.whatsapp\.net/i.test(m.author)) &&
      m.fromMe === true) ||
    (typeof m?.id === "string" && /^true_/.test(m.id)) ||
    m?.user === "me"
  )
}

/* =========================================
 * 17) BOLHA DE MENSAGEM
 * ======================================= */
function appendMessageBubble(pane, m) {
  const me = isFromMe(m)
  const el = document.createElement("div")
  el.className = "msg " + (me ? "me" : "you")

  const top = document.createElement("div")
  renderReplyPreview(top, m)
  const hadInteractive = renderInteractive(top, m)

  const { mime, url, dataUrl, caption } = pickMediaInfo(m)
  const plainText =
    m.text ||
    m.message?.text ||
    m?.message?.extendedTextMessage?.text ||
    m?.message?.conversation ||
    m.caption ||
    m.body ||
    ""
  const who = m.senderName || m.pushName || ""
  const ts = m.messageTimestamp || m.timestamp || m.t || ""

  // Sticker
  if (mime && /^image\/webp$/i.test(mime) && (url || dataUrl)) {
    const img = document.createElement("img")
    img.alt = "figurinha"
    img.style.maxWidth = "160px"
    img.style.borderRadius = "8px"
    if (top.childNodes.length) el.appendChild(top)
    el.appendChild(img)
    const meta = document.createElement("small")
    meta.textContent = `${escapeHtml(who)} • ${formatTime(ts)}`
    meta.style.display = "block"
    meta.style.marginTop = "6px"
    meta.style.opacity = ".75"
    el.appendChild(meta)
    pane.appendChild(el)
    const after = () => {
      pane.scrollTop = pane.scrollHeight
    }
    if (dataUrl) {
      img.onload = after
      img.src = dataUrl
    } else if (url) {
      fetchMediaBlobViaProxy(url)
        .then((b) => {
          img.onload = after
          img.src = URL.createObjectURL(b)
        })
        .catch(() => {
          img.alt = "(Falha ao carregar figurinha)"
          after()
        })
    }
    return
  }

  // IMAGEM
  if ((mime && mime.startsWith("image/")) || (!mime && url && /\.(png|jpe?g|gif|webp)(\?|$)/i.test(url))) {
    const figure = document.createElement("figure")
    figure.style.maxWidth = "280px"
    figure.style.margin = "0"
    const img = document.createElement("img")
    img.alt = "imagem"
    img.style.maxWidth = "100%"
    img.style.borderRadius = "8px"
    img.style.display = "block"
    const cap = document.createElement("figcaption")
    cap.style.fontSize = "12px"
    cap.style.opacity = ".8"
    cap.style.marginTop = "6px"
    cap.textContent = caption || plainText || ""
    if (top.childNodes.length) el.appendChild(top)
    figure.appendChild(img)
    if (cap.textContent) figure.appendChild(cap)
    el.appendChild(figure)
    const meta = document.createElement("small")
    meta.textContent = `${escapeHtml(who)} • ${formatTime(ts)}`
    meta.style.display = "block"
    meta.style.marginTop = "6px"
    meta.style.opacity = ".75"
    el.appendChild(meta)
    pane.appendChild(el)
    const after = () => {
      pane.scrollTop = pane.scrollHeight
    }
    if (dataUrl) {
      img.onload = after
      img.src = dataUrl
    } else if (url) {
      fetchMediaBlobViaProxy(url)
        .then((b) => {
          img.onload = after
          img.src = URL.createObjectURL(b)
        })
        .catch(() => {
          img.alt = "(Falha ao carregar imagem)"
          after()
        })
    } else {
      img.alt = "(Imagem não disponível)"
      after()
    }
    return
  }

  // VÍDEO
  if ((mime && mime.startsWith("video/")) || (!mime && url && /\.(mp4|webm|mov|m4v)(\?|$)/i.test(url))) {
    const video = document.createElement("video")
    video.controls = true
    video.style.maxWidth = "320px"
    video.style.borderRadius = "8px"
    video.preload = "metadata"
    if (top.childNodes.length) el.appendChild(top)
    el.appendChild(video)
    const cap = document.createElement("div")
    cap.style.fontSize = "12px"
    cap.style.opacity = ".8"
    cap.style.marginTop = "6px"
    cap.textContent = caption || ""
    if (cap.textContent) el.appendChild(cap)
    const meta = document.createElement("small")
    meta.textContent = `${escapeHtml(who)} • ${formatTime(ts)}`
    meta.style.display = "block"
    meta.style.marginTop = "6px"
    meta.style.opacity = ".75"
    el.appendChild(meta)
    pane.appendChild(el)
    const after = () => {
      pane.scrollTop = pane.scrollHeight
    }
    if (dataUrl) {
      video.onloadeddata = after
      video.src = dataUrl
    } else if (url) {
      fetchMediaBlobViaProxy(url)
        .then((b) => {
          video.onloadeddata = after
          video.src = URL.createObjectURL(b)
        })
        .catch(() => {
          const err = document.createElement("div")
          err.style.fontSize = "12px"
          err.style.opacity = ".8"
          err.textContent = "(Falha ao carregar vídeo)"
          el.insertBefore(err, meta)
          after()
        })
    } else {
      const err = document.createElement("div")
      err.style.fontSize = "12px"
      err.style.opacity = ".8"
      err.textContent = "(Vídeo não disponível)"
      el.insertBefore(err, meta)
      after()
    }
    return
  }

  // ÁUDIO
  if ((mime && mime.startsWith("audio/")) || (!mime && url && /\.(mp3|ogg|m4a|wav)(\?|$)/i.test(url))) {
    const audio = document.createElement("audio")
    audio.controls = true
    audio.preload = "metadata"
    if (top.childNodes.length) el.appendChild(top)
    el.appendChild(audio)
    const meta = document.createElement("small")
    meta.textContent = `${escapeHtml(who)} • ${formatTime(ts)}`
    meta.style.display = "block"
    meta.style.marginTop = "6px"
    meta.style.opacity = ".75"
    el.appendChild(meta)
    pane.appendChild(el)
    const after = () => {
      pane.scrollTop = pane.scrollHeight
    }
    if (dataUrl) {
      audio.onloadeddata = after
      audio.src = dataUrl
    } else if (url) {
      fetchMediaBlobViaProxy(url)
        .then((b) => {
          audio.onloadeddata = after
          audio.src = URL.createObjectURL(b)
        })
        .catch(() => {
          const err = document.createElement("div")
          err.style.fontSize = "12px"
          err.style.opacity = ".8"
          err.textContent = "(Falha ao carregar áudio)"
          el.insertBefore(err, meta)
          after()
        })
    } else {
      const err = document.createElement("div")
      err.style.fontSize = "12px"
      err.style.opacity = ".8"
      err.textContent = "(Áudio não disponível)"
      el.insertBefore(err, meta)
      after()
    }
    return
  }

  // DOCUMENTO
  if ((mime && /^application\//.test(mime)) || (!mime && url && /\.(pdf|docx?|xlsx?|pptx?)$/i.test(url))) {
    if (top.childNodes.length) el.appendChild(top)
    const link = document.createElement("a")
    link.textContent = caption || plainText || "Documento"
    link.target = "_blank"
    link.rel = "noopener noreferrer"
    link.href = "javascript:void(0)"
    link.onclick = async () => {
      try {
        const b = await fetchMediaBlobViaProxy(url)
        const blobUrl = URL.createObjectURL(b)
        window.open(blobUrl, "_blank")
      } catch {
        alert("Falha ao baixar documento")
      }
    }
    el.appendChild(link)
    const meta = document.createElement("small")
    meta.textContent = `${escapeHtml(who)} • ${formatTime(ts)}`
    meta.style.display = "block"
    meta.style.marginTop = "6px"
    meta.style.opacity = ".75"
    el.appendChild(meta)
    pane.appendChild(el)
    pane.scrollTop = pane.scrollHeight
    return
  }

  // INTERATIVO sem texto
  if (hadInteractive && !plainText) {
    if (top.childNodes.length) el.appendChild(top)
    const meta = document.createElement("small")
    meta.textContent = `${escapeHtml(who)} • ${formatTime(ts)}`
    meta.style.display = "block"
    meta.style.marginTop = "6px"
    meta.style.opacity = ".75"
    el.appendChild(meta)
    pane.appendChild(el)
    pane.scrollTop = pane.scrollHeight
    return
  }

  // TEXTO
  if (top.childNodes.length) el.appendChild(top)
  el.innerHTML += `${escapeHtml(plainText)}<small>${escapeHtml(who)} • ${formatTime(ts)}</small>`
  pane.appendChild(el)
  pane.scrollTop = pane.scrollHeight
}

/* =========================================
 * 18) PILL DE STAGE NO HEADER
 * ======================================= */
function upsertStagePill(stage) {
  let pill = document.getElementById("ai-pill")
  if (!pill) {
    pill = document.createElement("span")
    pill.id = "ai-pill"
    pill.style.marginLeft = "8px"
    pill.style.padding = "4px 8px"
    pill.style.borderRadius = "999px"
    pill.style.fontSize = "12px"
    pill.style.background = "var(--muted)"
    pill.style.color = "var(--text)"
    const header = document.querySelector(".chatbar") || document.querySelector(".chat-title") || document.body
    header.appendChild(pill)
  }
  const label = STAGE_LABEL[normalizeStage(stage)] || stage
  pill.textContent = label
  pill.title = ""
}

/* =========================================
 * 19) RENDER “CLÁSSICO”
 * ======================================= */
function renderMessages(msgs) {
  const pane = $("#messages")
  if (!pane) return
  pane.innerHTML = ""
  if (msgs.length === 0) {
    pane.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">💬</div>
        <h3>Nenhuma mensagem</h3>
        <p>Esta conversa ainda não possui mensagens</p>
      </div>`
    return
  }
  msgs.forEach((m) => {
    const me = isFromMe(m)
    const el = document.createElement("div")
    el.className = "msg " + (me ? "me" : "you")
    const text = m.text || m.message?.text || m.caption || m?.message?.conversation || m?.body || ""
    const who = m.senderName || m.pushName || ""
    const ts = m.messageTimestamp || m.timestamp || m.t || ""
    el.innerHTML = `${escapeHtml(text)}<small>${escapeHtml(who)} • ${formatTime(ts)}</small>`
    pane.appendChild(el)
  })
  pane.scrollTop = pane.scrollHeight
}

/* =========================================
 * 20) ENVIO
 * ======================================= */
async function sendNow() {
  const number = $("#send-number")?.value?.trim()
  const text = $("#send-text")?.value?.trim()
  const btnEl = $("#btn-send")
  if (!number || !text) return

  if (btnEl) {
    btnEl.disabled = true
    btnEl.innerHTML =
      '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>'
  }

  try {
    await api("/api/send-text", { method: "POST", body: JSON.stringify({ number, text }) })
    updateLastActivity(number, Date.now())
    if ($("#send-text")) $("#send-text").value = ""
    if (state.current && (state.current.wa_chatid || state.current.chatid) === number) {
      setTimeout(() => loadMessages(number), 500)
    }
  } catch (e) {
    alert(e.message || "Falha ao enviar mensagem")
  } finally {
    if (btnEl) {
      btnEl.disabled = false
      btnEl.innerHTML =
        '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 2L11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>'
    }
  }
}

/* =========================================
 * 21) BOOT
 * ======================================= */
document.addEventListener("DOMContentLoaded", () => {
  $("#btn-login") && ($("#btn-login").onclick = doLogin)
  $("#btn-logout") &&
    ($("#btn-logout").onclick = () => {
      localStorage.clear()
      location.reload()
    })
  $("#btn-send") && ($("#btn-send").onclick = sendNow)
  $("#btn-refresh") &&
    ($("#btn-refresh").onclick = () => {
      if (state.current) {
        const chatid = state.current.wa_chatid || state.current.chatid
        loadMessages(chatid)
      } else {
        loadChats()
      }
    })

  const backBtn = document.getElementById("btn-back-mobile")
  if (backBtn) backBtn.onclick = () => setMobileMode("list")

  $("#send-text") &&
    $("#send-text").addEventListener("keypress", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault()
        sendNow()
      }
    })

  $("#token") &&
    $("#token").addEventListener("keypress", (e) => {
      if (e.key === "Enter") {
        e.preventDefault()
        doLogin()
      }
    })

  // Listener para fechar modal de QR Code
  const qrCloseBtn = document.getElementById("qr-close")
  if (qrCloseBtn) {
    qrCloseBtn.onclick = () => hideQrModal()
  }

  // Login por e-mail
  $("#btn-acct-login") && ($("#btn-acct-login").onclick = acctLogin)
  $("#acct-pass") &&
    $("#acct-pass").addEventListener("keypress", (e) => {
      if (e.key === "Enter") {
        e.preventDefault()
        acctLogin()
      }
    })

  // Billing system event listeners
  $("#btn-conversas") && ($("#btn-conversas").onclick = showConversasView)
  $("#btn-pagamentos") && ($("#btn-pagamentos").onclick = showBillingView)
  $("#btn-pay-getnet") && ($("#btn-pay-getnet").onclick = createCheckoutLink)

  // Billing modal event listeners
  $("#btn-go-to-payments") &&
    ($("#btn-go-to-payments").onclick = () => {
      hideBillingModal()
      showBillingView()
    })

  $("#btn-logout-modal") &&
    ($("#btn-logout-modal").onclick = () => {
      localStorage.clear()
      location.reload()
    })

  // Voltar para etapa de conta
  $("#btn-voltar-account") && ($("#btn-voltar-account").onclick = showStepAccount)

  // Link para tela de cadastro
  $("#link-acct-register") &&
    ($("#link-acct-register").onclick = (e) => {
      e.preventDefault()
      showStepRegister()
      $("#reg-email")?.focus()
    })
  // Botão de voltar do registro para o login
  $("#btn-back-to-login") &&
    ($("#btn-back-to-login").onclick = (e) => {
      e.preventDefault()
      showStepAccount()
      $("#acct-email")?.focus()
    })
  // Registro de conta
  $("#btn-acct-register") && ($("#btn-acct-register").onclick = acctRegister)
  $("#reg-pass") &&
    $("#reg-pass").addEventListener("keypress", (e) => {
      if (e.key === "Enter") {
        e.preventDefault()
        acctRegister()
      }
    })

  // Link de cadastro (rota bonita)
  $("#link-cadastrar") &&
    ($("#link-cadastrar").onclick = (e) => {
      e.preventDefault()
      // mantém URL bonita; página faz o redirect para a GetNet
      window.location.href = "/pagamentos/getnet"
    })

  ensureRoute()
})
