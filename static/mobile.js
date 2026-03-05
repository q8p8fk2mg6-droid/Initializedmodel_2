let importedStrategy = null;
let importedCode = "";
let selectedRobotId = "";
let robotsById = {};
let scanStream = null;
let scanTimer = null;
let scanBusy = false;
let scanActive = false;
let scanDetector = null;

function parseApiError(payload, fallback) {
  if (!payload) return fallback;
  if (typeof payload === "string" && payload.trim()) return payload;
  if (typeof payload.detail === "string" && payload.detail.trim()) return payload.detail;
  if (Array.isArray(payload.detail) && payload.detail.length) {
    const first = payload.detail[0];
    if (typeof first === "string" && first.trim()) return first;
    if (first && typeof first.msg === "string") return first.msg;
  }
  if (typeof payload.message === "string" && payload.message.trim()) return payload.message;
  return fallback;
}

function setText(id, msg) {
  const el = document.getElementById(id);
  if (el) el.innerText = msg;
}

function asNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function fmtPct(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return `${n.toFixed(digits)}%`;
}

function fmtUsd(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return `${n.toFixed(digits)} USDT`;
}

function fmtTime(v) {
  const txt = String(v || "").trim();
  if (!txt) return "-";
  return txt.replace("T", " ").replace("Z", "");
}

function collectRuntimeCreds() {
  const apiKey = String(document.getElementById("api_key")?.value || "").trim();
  const apiSecret = String(document.getElementById("api_secret")?.value || "").trim();
  if (!apiKey || !apiSecret) return { api_key: null, api_secret: null };
  return { api_key: apiKey, api_secret: apiSecret };
}

function normalizeTransferCode(raw) {
  return String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function extractTransferCodeFromScan(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";

  try {
    const url = new URL(text);
    const code = normalizeTransferCode(url.searchParams.get("code"));
    if (code) return code;
  } catch (_) {
    // Not a full URL, continue with regex fallback.
  }

  const direct = normalizeTransferCode(text);
  if (/^[A-Z0-9]{4,32}$/.test(direct)) return direct;

  const match = text.match(/[A-Z0-9]{4,32}/i);
  if (match && match[0]) {
    return normalizeTransferCode(match[0]);
  }
  return "";
}

function setScanStatus(msg) {
  setText("scan_status", msg);
}

function showScanModal(visible) {
  const modal = document.getElementById("scan_modal");
  if (!modal) return;
  modal.style.display = visible ? "block" : "none";
}

async function stopScanImport() {
  scanActive = false;
  if (scanTimer) {
    clearInterval(scanTimer);
    scanTimer = null;
  }
  const video = document.getElementById("scan_video");
  if (video) {
    try {
      video.pause();
    } catch (_) {
      // Ignore pause failures.
    }
    video.srcObject = null;
  }
  if (scanStream) {
    try {
      scanStream.getTracks().forEach((track) => track.stop());
    } catch (_) {
      // Ignore track stop failures.
    }
  }
  scanStream = null;
  showScanModal(false);
}

async function handleScanPayload(rawText) {
  const code = extractTransferCodeFromScan(rawText);
  if (!code) return false;
  const input = document.getElementById("transfer_code");
  if (input) input.value = code;
  await stopScanImport();
  setText("import_status", `扫码成功，导入码：${code}。正在导入策略...`);
  await importStrategyByCode({ consume: false });
  return true;
}

async function startScanImport() {
  if (scanActive) return;
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    setText("import_status", "当前浏览器不支持摄像头扫码，请手动输入导入码。");
    return;
  }

  const video = document.getElementById("scan_video");
  if (!video) return;
  showScanModal(true);
  setScanStatus("正在请求摄像头权限...");

  try {
    scanStream = await navigator.mediaDevices.getUserMedia({
      video: {
        facingMode: { ideal: "environment" },
      },
      audio: false,
    });
    video.srcObject = scanStream;
    await video.play();
  } catch (err) {
    await stopScanImport();
    setText("import_status", `无法打开摄像头：${err.message || "unknown error"}`);
    return;
  }

  if (!("BarcodeDetector" in window)) {
    setScanStatus("浏览器不支持 BarcodeDetector，请手动输入导入码。");
    return;
  }

  try {
    if (!scanDetector) {
      scanDetector = new window.BarcodeDetector({ formats: ["qr_code"] });
    }
  } catch (err) {
    setScanStatus(`扫码初始化失败：${err.message || "unknown error"}`);
    return;
  }

  setScanStatus("对准电脑端二维码，自动导入...");
  scanActive = true;
  scanBusy = false;
  scanTimer = setInterval(async () => {
    if (!scanActive || scanBusy) return;
    scanBusy = true;
    try {
      const codes = await scanDetector.detect(video);
      if (Array.isArray(codes) && codes.length > 0) {
        const first = codes[0];
        const rawValue = String(first.rawValue || "").trim();
        if (rawValue) {
          await handleScanPayload(rawValue);
        }
      }
    } catch (_) {
      // Ignore transient detect failures and continue scanning.
    } finally {
      scanBusy = false;
    }
  }, 260);
}

function renderImportedStrategy() {
  const box = document.getElementById("strategy_summary");
  if (!box) return;

  if (!importedStrategy) {
    box.innerHTML = "";
    return;
  }

  const p = importedStrategy.params || {};
  const portfolio = Array.isArray(importedStrategy.portfolio) ? importedStrategy.portfolio : [];
  const htmlList = portfolio
    .map((leg) => {
      const w = Number(leg.weight) * 100;
      const d = String(leg.direction || "long").toLowerCase();
      const lev = leg.leverage == null ? "-" : Number(leg.leverage).toFixed(2);
      return `<li>${String(leg.asset || "").toUpperCase()} | ${d} | ${w.toFixed(2)}% | lev=${lev}</li>`;
    })
    .join("");

  box.innerHTML = `
    <div><strong>导入码：</strong>${importedCode || "-"}</div>
    <div><strong>策略ID：</strong>${importedStrategy.strategy_id || "-"}</div>
    <div><strong>来源：</strong>${importedStrategy.source || "-"}</div>
    <div><strong>收益：</strong>年化 ${(asNum(importedStrategy.annualized_return) * 100).toFixed(2)}% | 
      夏普 ${asNum(importedStrategy.sharpe).toFixed(3)} | 回撤 ${(asNum(importedStrategy.max_drawdown) * 100).toFixed(2)}%</div>
    <div><strong>参数：</strong>rehedge=${p.rehedge_hours ?? "-"}h, 阈值=${asNum(p.rebalance_threshold_pct).toFixed(2)}%,
      longLev=${asNum(p.long_leverage, 1).toFixed(2)}, shortLev=${asNum(p.short_leverage, 1).toFixed(2)}</div>
    <ul>${htmlList || "<li>无持仓组合</li>"}</ul>
  `;

  const longLevEl = document.getElementById("long_lev");
  const shortLevEl = document.getElementById("short_lev");
  const nameEl = document.getElementById("robot_name");
  if (longLevEl && p.long_leverage != null) longLevEl.value = Number(p.long_leverage).toFixed(2);
  if (shortLevEl && p.short_leverage != null) shortLevEl.value = Number(p.short_leverage).toFixed(2);
  if (nameEl && !String(nameEl.value || "").trim()) {
    nameEl.value = `mobile-${String(importedStrategy.strategy_id || "").slice(0, 8)}`;
  }
}

async function importStrategyByCode(options = {}) {
  const consume = Boolean(options.consume);
  const code = String(document.getElementById("transfer_code")?.value || "").trim().toUpperCase();
  if (!code) {
    setText("import_status", "请输入导入码。");
    return;
  }
  try {
    const resp = await fetch("/api/strategy-transfer/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        transfer_code: code,
        consume,
      }),
    });
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "导入策略失败"));
    importedCode = String(payload.transfer_code || code).toUpperCase();
    importedStrategy = payload.strategy || null;
    renderImportedStrategy();
    setText("import_status", `导入成功：${importedCode}，有效期至 ${fmtTime(payload.expires_at)}。`);
  } catch (err) {
    importedStrategy = null;
    renderImportedStrategy();
    setText("import_status", err.message || "导入策略失败");
  }
}

async function buildPlanFromImportedStrategy() {
  if (!importedStrategy || !Array.isArray(importedStrategy.portfolio) || importedStrategy.portfolio.length === 0) {
    throw new Error("请先导入有效策略。");
  }
  const totalCapital = asNum(document.getElementById("capital_usdt")?.value, 0);
  const longLev = asNum(document.getElementById("long_lev")?.value, 1);
  const shortLev = asNum(document.getElementById("short_lev")?.value, 1);
  if (!(totalCapital > 0)) throw new Error("总资金必须大于 0。");
  if (!(longLev >= 1 && shortLev >= 1)) throw new Error("杠杆必须 >= 1。");

  const payload = {
    total_capital_usdt: totalCapital,
    long_leverage: longLev,
    short_leverage: shortLev,
    portfolio: importedStrategy.portfolio.map((leg) => ({
      asset: String(leg.asset || "").toUpperCase(),
      weight: asNum(leg.weight),
      direction: String(leg.direction || "long").toLowerCase() === "short" ? "short" : "long",
      leverage: leg.leverage == null ? null : asNum(leg.leverage),
    })),
  };
  const resp = await fetch("/api/calculator/plan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(parseApiError(data, "根据策略计算仓位失败"));
  return data;
}

function collectRobotRequestFromPlan(plan) {
  const name = String(document.getElementById("robot_name")?.value || "").trim() || "mobile-robot";
  const exchange = String(document.getElementById("robot_exchange")?.value || "bybit").toLowerCase();
  const mode = String(document.getElementById("robot_mode")?.value || "dry-run").toLowerCase();
  const tpPct = asNum(document.getElementById("tp_pct")?.value, 0);
  const slPct = asNum(document.getElementById("sl_pct")?.value, 0);
  const pollSec = Math.trunc(asNum(document.getElementById("poll_sec")?.value, 10));
  if (!(tpPct > 0)) throw new Error("TP(%) 必须大于 0。");
  if (!(slPct > 0)) throw new Error("SL(%) 必须大于 0。");
  if (!(pollSec >= 1)) throw new Error("轮询秒数必须 >= 1。");

  const creds = collectRuntimeCreds();
  return {
    name,
    exchange,
    execution_mode: mode,
    tp_pct: tpPct,
    sl_pct: slPct,
    poll_interval_seconds: pollSec,
    total_capital_usdt: asNum(plan.total_capital_usdt),
    rows: (plan.rows || []).map((row) => ({
      asset: String(row.asset || "").toUpperCase(),
      direction: String(row.direction || "long").toLowerCase(),
      weight_pct: asNum(row.weight_pct),
      margin: asNum(row.margin),
      notional: asNum(row.notional),
      leverage: asNum(row.leverage, 1),
    })),
    source_strategy_id: importedStrategy ? String(importedStrategy.strategy_id || "") : null,
    api_key: creds.api_key,
    api_secret: creds.api_secret,
  };
}

async function createRobotFromImported() {
  try {
    setText("create_status", "正在计算仓位并创建机器人...");
    const plan = await buildPlanFromImportedStrategy();
    const req = collectRobotRequestFromPlan(plan);
    const resp = await fetch("/api/live/robots", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "创建机器人失败"));
    const rid = String(payload.robot_id || "");
    setText(
      "create_status",
      `创建成功：${rid} | rows=${(req.rows || []).length} | 资金=${fmtUsd(req.total_capital_usdt)}`
    );
    selectedRobotId = rid;
    await loadRobots({ quiet: true });
    if (rid) await loadRobotEvents(rid);
  } catch (err) {
    setText("create_status", err.message || "创建机器人失败");
  }
}

function renderRobots(robots) {
  const listEl = document.getElementById("robots_list");
  if (!listEl) return;
  listEl.innerHTML = "";
  robotsById = {};

  (robots || []).forEach((robot) => {
    const rid = String(robot.robot_id || "");
    if (!rid) return;
    robotsById[rid] = robot;

    const cfg = robot.config || {};
    const st = robot.state || {};
    const div = document.createElement("div");
    div.className = "robot-card";
    div.innerHTML = `
      <div class="robot-title">${cfg.name || rid}</div>
      <div class="robot-meta">
        id=${rid}<br/>
        mode=${cfg.execution_mode || "-"} | exchange=${cfg.exchange || "-"} | status=${st.status || "-"}<br/>
        equity=${fmtUsd(st.current_equity)} | pnl=${fmtPct(st.pnl_pct, 4)} | tp/sl=${fmtPct(cfg.tp_pct)} / ${fmtPct(cfg.sl_pct)}
      </div>
      <div class="robot-actions">
        <button class="act-start" data-id="${rid}">启动</button>
        <button class="act-check secondary" data-id="${rid}">状态检查</button>
        <button class="act-stop secondary" data-id="${rid}">停止</button>
        <button class="act-close warn" data-id="${rid}">紧急平仓</button>
        <button class="act-events secondary" data-id="${rid}">事件</button>
        <button class="act-delete danger" data-id="${rid}">删除</button>
      </div>
    `;

    div.querySelector(".act-start")?.addEventListener("click", async (e) => {
      const id = e.currentTarget.dataset.id;
      if (!id) return;
      await robotAction(id, "start");
    });
    div.querySelector(".act-check")?.addEventListener("click", async (e) => {
      const id = e.currentTarget.dataset.id;
      if (!id) return;
      await robotAction(id, "status-check");
    });
    div.querySelector(".act-stop")?.addEventListener("click", async (e) => {
      const id = e.currentTarget.dataset.id;
      if (!id) return;
      await robotAction(id, "stop");
    });
    div.querySelector(".act-close")?.addEventListener("click", async (e) => {
      const id = e.currentTarget.dataset.id;
      if (!id) return;
      await robotAction(id, "close-all");
    });
    div.querySelector(".act-delete")?.addEventListener("click", async (e) => {
      const id = e.currentTarget.dataset.id;
      if (!id) return;
      await robotAction(id, "delete");
    });
    div.querySelector(".act-events")?.addEventListener("click", async (e) => {
      const id = e.currentTarget.dataset.id;
      if (!id) return;
      selectedRobotId = id;
      await loadRobotEvents(id);
    });

    listEl.appendChild(div);
  });
}

async function loadRobots(options = {}) {
  const quiet = Boolean(options && options.quiet);
  try {
    const resp = await fetch("/api/live/robots", { cache: "no-store" });
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "加载机器人列表失败"));
    const robots = payload.robots || [];
    renderRobots(robots);
    if (!selectedRobotId && robots.length > 0) selectedRobotId = String(robots[0].robot_id || "");
    if (!quiet) setText("robots_status", `已加载 ${robots.length} 个机器人。`);
  } catch (err) {
    if (!quiet) setText("robots_status", err.message || "加载机器人列表失败");
  }
}

async function robotAction(robotId, action) {
  const rid = String(robotId || "").trim();
  if (!rid) return;

  if (action === "delete") {
    const ok = window.confirm(`确认删除机器人 ${rid}？此操作不会自动平仓。`);
    if (!ok) return;
  }

  const actionMap = {
    start: { url: `/api/live/robots/${encodeURIComponent(rid)}/start`, method: "POST", withBody: true, label: "启动" },
    stop: { url: `/api/live/robots/${encodeURIComponent(rid)}/stop`, method: "POST", withBody: false, label: "停止" },
    "close-all": {
      url: `/api/live/robots/${encodeURIComponent(rid)}/close-all`,
      method: "POST",
      withBody: false,
      label: "紧急平仓",
    },
    "status-check": {
      url: `/api/live/robots/${encodeURIComponent(rid)}/status-check`,
      method: "POST",
      withBody: true,
      label: "状态检查",
    },
    delete: { url: `/api/live/robots/${encodeURIComponent(rid)}`, method: "DELETE", withBody: false, label: "删除" },
  };
  const info = actionMap[action];
  if (!info) return;

  try {
    const init = { method: info.method };
    if (info.withBody) {
      init.headers = { "Content-Type": "application/json" };
      init.body = JSON.stringify(collectRuntimeCreds());
    }
    const resp = await fetch(info.url, init);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, `${info.label}失败`));

    selectedRobotId = rid;
    setText("robots_status", `${info.label}成功：${rid}`);
    await loadRobots({ quiet: true });
    if (action !== "delete") {
      await loadRobotEvents(rid);
    } else {
      if (selectedRobotId === rid) {
        selectedRobotId = "";
        renderEvents([]);
      }
    }
  } catch (err) {
    setText("robots_status", err.message || `${info.label}失败`);
  }
}

function renderEvents(events) {
  const listEl = document.getElementById("events_list");
  if (!listEl) return;
  listEl.innerHTML = "";
  (events || []).forEach((ev) => {
    const item = document.createElement("div");
    item.className = "event-item";
    item.innerHTML = `
      <div class="event-time">${fmtTime(ev.timestamp)}</div>
      <div>[${String(ev.level || "-").toUpperCase()}] ${ev.type || "-"}</div>
      <div>${ev.message || "-"}</div>
    `;
    listEl.appendChild(item);
  });
}

async function loadRobotEvents(robotId, limit = 80) {
  const rid = String(robotId || "").trim();
  if (!rid) {
    setText("events_status", "请选择机器人查看事件。");
    renderEvents([]);
    return;
  }
  try {
    const resp = await fetch(`/api/live/robots/${encodeURIComponent(rid)}/events?limit=${encodeURIComponent(limit)}`);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "加载事件失败"));
    const events = payload.events || [];
    renderEvents(events);
    setText("events_status", `机器人 ${rid} 事件 ${events.length} 条。`);
  } catch (err) {
    setText("events_status", err.message || "加载事件失败");
  }
}

function initFromQueryCode() {
  try {
    const params = new URLSearchParams(window.location.search || "");
    const code = String(params.get("code") || "").trim().toUpperCase();
    if (!code) return;
    const input = document.getElementById("transfer_code");
    if (input) input.value = code;
    importStrategyByCode({ consume: false }).catch(() => {});
  } catch (_) {
    // Ignore query parsing errors.
  }
}

const importBtn = document.getElementById("import_strategy_btn");
if (importBtn) {
  importBtn.addEventListener("click", () => {
    importStrategyByCode({ consume: false }).catch(() => {});
  });
}

const scanBtn = document.getElementById("scan_strategy_btn");
if (scanBtn) {
  scanBtn.addEventListener("click", () => {
    startScanImport().catch(() => {});
  });
}

const stopScanBtn = document.getElementById("stop_scan_btn");
if (stopScanBtn) {
  stopScanBtn.addEventListener("click", () => {
    stopScanImport().catch(() => {});
  });
}

const scanModal = document.getElementById("scan_modal");
if (scanModal) {
  scanModal.addEventListener("click", (e) => {
    if (e.target === scanModal) {
      stopScanImport().catch(() => {});
    }
  });
}

const createBtn = document.getElementById("create_robot_btn");
if (createBtn) {
  createBtn.addEventListener("click", () => {
    createRobotFromImported().catch(() => {});
  });
}

const refreshBtn = document.getElementById("refresh_robots_btn");
if (refreshBtn) {
  refreshBtn.addEventListener("click", () => {
    loadRobots({ quiet: false }).catch(() => {});
  });
}

initFromQueryCode();
loadRobots({ quiet: true }).catch(() => {});
setInterval(() => {
  loadRobots({ quiet: true }).catch(() => {});
  if (selectedRobotId) {
    loadRobotEvents(selectedRobotId, 50).catch(() => {});
  }
}, 5000);

window.addEventListener("beforeunload", () => {
  stopScanImport().catch(() => {});
});
