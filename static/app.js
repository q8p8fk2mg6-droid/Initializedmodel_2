let latestStrategies = [];
let strategiesById = {};
let equityChart = null;
let selectedStrategyId = null;
let selectedCustomStrategyId = null;
let historyRunsById = {};
let latestDataSource = "none";
let openedHistoryByMode = {};
let openedHistoryFallback = [];
let currentTimelinessRunId = "";
let timelinessHistoryRunsById = {};
let timelinessLookbackLoadState = {
  running: false,
  runId: "",
  lookbackDays: 0,
  anchorIndex: 1,
};

const progressTimers = {};

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function parseNumber(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function parseApiError(payload, fallback) {
  if (!payload) return fallback;
  if (typeof payload === "string" && payload.trim()) return payload;
  if (typeof payload.detail === "string" && payload.detail.trim()) return payload.detail;
  if (Array.isArray(payload.detail) && payload.detail.length) {
    const first = payload.detail[0];
    if (typeof first === "string" && first.trim()) return first;
    if (first && typeof first.msg === "string") {
      if (Array.isArray(first.loc) && first.loc.length) {
        return `${first.msg} (${first.loc.join(".")})`;
      }
      return first.msg;
    }
  }
  if (typeof payload.message === "string" && payload.message.trim()) return payload.message;
  return fallback;
}

function isReverseFillEnabled() {
  const el = document.getElementById("reverse_fill_enabled");
  return Boolean(el && el.checked);
}

function flipDirection(direction) {
  const d = String(direction || "long").toLowerCase();
  return d === "long" ? "short" : "long";
}

function setText(id, msg) {
  const el = document.getElementById(id);
  if (el) el.innerText = msg;
}

function showStatus(msg) {
  setText("status", msg);
}

function showCurveHint(msg) {
  setText("curve_hint", msg);
}

function showCalcStatus(msg) {
  setText("calc_status", msg);
}

function showHistoryStatus(msg) {
  setText("history_status", msg);
}

function showRuntimeStatus(msg) {
  setText("runtime_status", msg);
}

function showCustomPrefillMeta(msg) {
  setText("custom_prefill_meta", msg);
}

function showTimelinessStatus(msg) {
  setText("timeliness_status", msg);
}

function showTimelinessHistoryStatus(msg) {
  setText("timeliness_history_status", msg);
}

function showTimelinessLookupRuntime(msg) {
  setText("timeliness_lookup_runtime", msg);
}

function toMb(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  return n;
}

function renderRuntimeStatus(stats) {
  if (!stats || typeof stats !== "object") {
    showRuntimeStatus("Runtime: unavailable");
    return;
  }
  const timeliness = (stats.timeliness && typeof stats.timeliness === "object") ? stats.timeliness : null;
  if (timeliness && Boolean(timeliness.running)) {
    const stage = String(timeliness.stage || "running");
    const completed = Number(timeliness.completed_steps || 0);
    const total = Math.max(Number(timeliness.total_steps || 0), 1);
    const pctRaw = Number(timeliness.progress_pct);
    const pct = Number.isFinite(pctRaw) ? pctRaw : (completed / total) * 100.0;
    const lIdx = Number(timeliness.lookback_index || 0);
    const lTotal = Number(timeliness.lookback_total || 0);
    const lDays = Number(timeliness.lookback_days || 0);
    const aIdx = Number(timeliness.anchor_index || 0);
    const aTotal = Number(timeliness.anchor_total || 0);
    const msg = String(timeliness.message || "");
    setProgress("backtest", pct);
    showRuntimeStatus(
      `Timeliness(running) stage=${stage} | progress=${pct.toFixed(1)}% (${completed}/${total}) ` +
      `| L=${lDays || "-"} (${lIdx}/${lTotal}) | anchor=${aIdx}/${aTotal}`
    );
    if (msg) {
      showTimelinessStatus(`${msg} (${completed}/${total})`);
    }
    return;
  }
  if (timeliness && !Boolean(timeliness.running)) {
    const total = Number(timeliness.total_steps || 0);
    const completed = Number(timeliness.completed_steps || 0);
    const msg = String(timeliness.message || "");
    if (total > 0 && completed >= total) {
      setProgressAtLeast("backtest", 100);
      if (msg) showTimelinessStatus(msg);
    } else if (String(timeliness.last_error || "").trim()) {
      if (msg) showTimelinessStatus(msg);
    }
  }
  const running = Boolean(stats.running);
  const stage = String(stats.stage || (running ? "running" : "idle"));
  const memMb = toMb(stats.memory_mb).toFixed(1);
  const peakMb = toMb(stats.peak_memory_mb).toFixed(1);
  const hits = Number(stats.cache_hits || 0);
  const misses = Number(stats.cache_misses || 0);
  const hitRate = (Number(stats.cache_hit_rate || 0) * 100).toFixed(1);
  const cacheSize = Number(stats.cache_size || 0);
  const cacheLimit = Number(stats.cache_limit || 0);
  const evaluated = Number(stats.evaluated_count || 0);
  const evalBudget = Number(stats.eval_budget || 0);
  const prefix = running ? "Runtime(running)" : "Runtime(idle)";
  showRuntimeStatus(
    `${prefix} stage=${stage} | mem=${memMb}MB peak=${peakMb}MB | cache hit=${hitRate}% (${hits}/${hits + misses}) ` +
      `size=${cacheSize}/${cacheLimit} | eval=${evaluated}/${evalBudget}`
  );

  if (timelinessLookbackLoadState.running) {
    const lookupAnchor = Math.trunc(Number(timelinessLookbackLoadState.anchorIndex || 1));
    const lookupAnchorText = Number.isFinite(lookupAnchor) && lookupAnchor > 0 ? ` | anchor=${lookupAnchor}` : "";
    if (running && evalBudget > 0) {
      const pct = clamp((evaluated / Math.max(evalBudget, 1)) * 100.0, 0, 99);
      setProgress("timeliness_lookup", pct);
      setProgressAtLeast("backtest", pct);
      showTimelinessLookupRuntime(
        `运行中: stage=${stage}${lookupAnchorText} | eval=${evaluated}/${evalBudget} (${pct.toFixed(1)}%)`
      );
    } else if (running) {
      showTimelinessLookupRuntime(`运行中: stage=${stage}${lookupAnchorText} | 正在准备数据...`);
    } else {
      const lastErr = String(stats.last_error || "").trim();
      if (lastErr) {
        showTimelinessLookupRuntime(`失败: ${lastErr}`);
      }
    }
  }
}

async function refreshRuntimeStatus() {
  try {
    const resp = await fetch("/api/system/runtime", { cache: "no-store" });
    if (!resp.ok) return;
    const payload = await resp.json();
    renderRuntimeStatus(payload);
  } catch (_) {
    // Keep UI responsive even if status endpoint is temporarily unavailable.
  }
}

function setButtonsDisabled(ids, disabled) {
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.disabled = disabled;
  });
}

function setTimelinessLookbackButtonsDisabled(disabled) {
  const buttons = document.querySelectorAll(".open-history-lookback-btn, .open-timeliness-params-btn");
  buttons.forEach((btn) => {
    btn.disabled = Boolean(disabled);
  });
}

function setProgress(prefix, value) {
  const pct = clamp(Number(value) || 0, 0, 100);
  const fill = document.getElementById(`${prefix}_progress_fill`);
  const text = document.getElementById(`${prefix}_progress_text`);
  if (fill) fill.style.width = `${pct.toFixed(0)}%`;
  if (text) text.innerText = `${pct.toFixed(0)}%`;
}

function getProgressValue(prefix) {
  const text = document.getElementById(`${prefix}_progress_text`);
  if (!text) return 0;
  const current = Number(String(text.innerText || "0").replace("%", ""));
  return Number.isFinite(current) ? current : 0;
}

function setProgressAtLeast(prefix, value) {
  const current = getProgressValue(prefix);
  setProgress(prefix, Math.max(current, Number(value) || 0));
}

function startAutoProgress(prefix, start = 1, cap = 90, step = 2, intervalMs = 250) {
  stopAutoProgress(prefix);
  setProgress(prefix, start);
  progressTimers[prefix] = setInterval(() => {
    const current = getProgressValue(prefix);
    if (current >= cap) return;
    const next = clamp(current + step, 0, cap);
    setProgress(prefix, next);
  }, intervalMs);
}

function stopAutoProgress(prefix, finalValue = 100) {
  if (progressTimers[prefix]) {
    clearInterval(progressTimers[prefix]);
    delete progressTimers[prefix];
  }
  setProgress(prefix, finalValue);
}

function parsePortfolioJson() {
  const raw = document.getElementById("portfolio_json").value.trim();
  if (!raw) throw new Error("手动组合为空");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("手动组合必须是 JSON 数组");
  }
  return parsed.map((item) => ({
    asset: String(item.asset || "").toUpperCase(),
    weight: Number(item.weight),
    direction: String(item.direction || "long").toLowerCase(),
    leverage: item.leverage == null ? null : Number(item.leverage),
  }));
}

function collectRequest() {
  return {
    start_date: document.getElementById("start_date").value,
    end_date: document.getElementById("end_date").value,
    initial_capital_usdt: parseNumber(document.getElementById("initial_capital").value, 1000),
    top_k: parseNumber(document.getElementById("top_k").value, 50),
    max_evals: parseNumber(document.getElementById("max_evals").value, 10000),
    parallel_workers: parseNumber(document.getElementById("parallel_workers").value, 32),
    execution_mode: (document.getElementById("execution_mode")?.value || "performance"),
    universe_limit: parseNumber(document.getElementById("universe_limit").value, 100),
    portfolio_size_min: parseNumber(document.getElementById("portfolio_min").value, 3),
    portfolio_size_max: parseNumber(document.getElementById("portfolio_max").value, 4),
    weight_step_pct: parseNumber(document.getElementById("weight_step").value, 5),
    candidate_pool_size: parseNumber(document.getElementById("candidate_pool").value, 0),
    require_both_directions: document.getElementById("require_both").value === "true",
    rehedge_hours_min: parseNumber(document.getElementById("rehedge_min").value, 1),
    rehedge_hours_max: parseNumber(document.getElementById("rehedge_max").value, 720),
    rehedge_hours_step: parseNumber(document.getElementById("rehedge_step").value, 1),
    rebalance_threshold_pct_min: parseNumber(document.getElementById("threshold_min").value, 0),
    rebalance_threshold_pct_max: parseNumber(document.getElementById("threshold_max").value, 5),
    rebalance_threshold_pct_step: parseNumber(document.getElementById("threshold_step").value, 1),
    long_leverage_min: parseNumber(document.getElementById("long_leverage_min").value, 1),
    long_leverage_max: parseNumber(document.getElementById("long_leverage_max").value, 3),
    long_leverage_step: parseNumber(document.getElementById("long_leverage_step").value, 0.5),
    short_leverage_min: parseNumber(document.getElementById("short_leverage_min").value, 1),
    short_leverage_max: parseNumber(document.getElementById("short_leverage_max").value, 3),
    short_leverage_step: parseNumber(document.getElementById("short_leverage_step").value, 0.5),
    ranking_mode: document.getElementById("ranking_mode").value || "sharpe_desc_return_desc",
    min_apy_pct: parseNumber(document.getElementById("min_apy_pct").value, 0),
    min_sharpe: parseNumber(document.getElementById("min_sharpe").value, 1.5),
    max_mdd_pct: parseNumber(document.getElementById("max_mdd_pct").value, 30),
    binance_api_key: document.getElementById("binance_api_key").value.trim() || null,
    binance_api_secret: document.getElementById("binance_api_secret").value.trim() || null,
  };
}

function collectCustomRequest() {
  if (!selectedCustomStrategyId || !strategiesById[selectedCustomStrategyId]) {
    throw new Error('Please click "Prefill Custom" in the ranking table first.');
  }
  const reverseDirections = isReverseFillEnabled();
  return {
    strategy_id: selectedCustomStrategyId,
    start_date: document.getElementById("custom_start_date").value || document.getElementById("start_date").value,
    end_date: document.getElementById("custom_end_date").value || document.getElementById("end_date").value,
    initial_capital_usdt: parseNumber(
      document.getElementById("custom_initial_capital").value,
      parseNumber(document.getElementById("initial_capital").value, 1000)
    ),
    reverse_directions: reverseDirections,
    ranking_mode: document.getElementById("ranking_mode").value || "sharpe_desc_return_desc",
    binance_api_key: document.getElementById("binance_api_key").value.trim() || null,
    binance_api_secret: document.getElementById("binance_api_secret").value.trim() || null,
  };
}

function parseLookbackWindows(raw) {
  const text = String(raw || "").trim();
  if (!text) return [30, 60, 90, 180, 360];
  const seen = new Set();
  const out = [];
  for (const token of text.split(",")) {
    const v = Number(String(token).trim());
    if (!Number.isFinite(v)) continue;
    const days = Math.trunc(v);
    if (days < 7 || days > 2000) continue;
    if (seen.has(days)) continue;
    seen.add(days);
    out.push(days);
  }
  return out.length ? out : [30, 60, 90, 180, 360];
}

function collectTimelinessRequest() {
  const req = collectRequest();
  return {
    decision_date: document.getElementById("decision_date").value || document.getElementById("end_date").value,
    forward_days: parseNumber(document.getElementById("forward_days").value, 30),
    lookback_windows_days: parseLookbackWindows(document.getElementById("lookback_windows").value),
    anchor_count: parseNumber(document.getElementById("anchor_count").value, 6),
    initial_capital_usdt: req.initial_capital_usdt,
    top_k: req.top_k,
    max_evals: req.max_evals,
    parallel_workers: req.parallel_workers,
    execution_mode: req.execution_mode,
    universe_limit: req.universe_limit,
    portfolio_size_min: req.portfolio_size_min,
    portfolio_size_max: req.portfolio_size_max,
    weight_step_pct: req.weight_step_pct,
    candidate_pool_size: req.candidate_pool_size,
    require_both_directions: req.require_both_directions,
    rehedge_hours_min: req.rehedge_hours_min,
    rehedge_hours_max: req.rehedge_hours_max,
    rehedge_hours_step: req.rehedge_hours_step,
    rebalance_threshold_pct_min: req.rebalance_threshold_pct_min,
    rebalance_threshold_pct_max: req.rebalance_threshold_pct_max,
    rebalance_threshold_pct_step: req.rebalance_threshold_pct_step,
    long_leverage_min: req.long_leverage_min,
    long_leverage_max: req.long_leverage_max,
    long_leverage_step: req.long_leverage_step,
    short_leverage_min: req.short_leverage_min,
    short_leverage_max: req.short_leverage_max,
    short_leverage_step: req.short_leverage_step,
    ranking_mode: req.ranking_mode,
    min_apy_pct: req.min_apy_pct,
    min_sharpe: req.min_sharpe,
    max_mdd_pct: req.max_mdd_pct,
    binance_api_key: req.binance_api_key,
    binance_api_secret: req.binance_api_secret,
  };
}

function resolveLookbackAnchorIndex(runId = "") {
  const input = document.getElementById("lookback_anchor_index");
  let raw = Math.trunc(parseNumber(input ? input.value : 1, 1));

  const rid = String(runId || "").trim();
  const run = rid ? timelinessHistoryRunsById[rid] : null;
  const runAnchorCount = Math.trunc(Number(run?.meta?.anchor_count || 0));
  const formAnchorCount = Math.trunc(parseNumber(document.getElementById("anchor_count")?.value, 24));
  const maxAnchor =
    Number.isFinite(runAnchorCount) && runAnchorCount > 0
      ? runAnchorCount
      : Number.isFinite(formAnchorCount) && formAnchorCount > 0
      ? formAnchorCount
      : 24;

  if (!Number.isFinite(raw) || raw < 1) raw = 1;
  const anchor = Math.trunc(clamp(raw, 1, maxAnchor));

  if (input) {
    input.max = String(maxAnchor);
    input.value = String(anchor);
  }
  return anchor;
}

function sampleCurve(curve, maxPoints = 320) {
  if (!curve || curve.length <= maxPoints) return curve || [];
  const step = Math.ceil(curve.length / maxPoints);
  const sampled = curve.filter((_, idx) => idx % step === 0);
  if (sampled[sampled.length - 1] !== curve[curve.length - 1]) sampled.push(curve[curve.length - 1]);
  return sampled;
}

function renderCurve(curve) {
  const sampled = sampleCurve(curve, 320);
  if (!sampled.length) {
    showCurveHint("No curve data available.");
    return;
  }
  if (typeof Chart === "undefined") {
    showCurveHint("Chart.js failed to load.");
    return;
  }

  const labels = sampled.map((p) => {
    const d = new Date(p[0]);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(
      d.getUTCDate()
    ).padStart(2, "0")} ${String(d.getUTCHours()).padStart(2, "0")}:00`;
  });
  const data = sampled.map((p) => Number(p[1]));

  const ctx = document.getElementById("equity_chart").getContext("2d");
  if (equityChart) equityChart.destroy();
  equityChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "净值 (NAV)",
          data,
          borderColor: "#1767d2",
          backgroundColor: "rgba(23, 103, 210, 0.12)",
          fill: true,
          pointRadius: 0,
          tension: 0.2,
        },
      ],
    },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: true } },
      scales: { x: { ticks: { autoSkip: true, maxTicksLimit: 12 } } },
    },
  });
}

function asPct(v) {
  return `${(Number(v) * 100).toFixed(2)}%`;
}

function asPctNullable(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return `${(n * 100).toFixed(2)}%`;
}

function asNumNullable(v, digits = 3) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(digits);
}

function renderTimelinessTable(rows, bestLookbackDays, runId = "") {
  const tbody = document.querySelector("#timeliness_table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  (rows || []).forEach((row) => {
    const tr = document.createElement("tr");
    const lookbackDays = Number(row.lookback_days || 0);
    const isBest = lookbackDays === Number(bestLookbackDays);
    const canOpen = Boolean(runId) && Number.isFinite(lookbackDays) && lookbackDays > 0;
    tr.innerHTML = `
      <td>${isBest ? "* " : ""}${lookbackDays}</td>
      <td>${asNumNullable(row.score, 4)}</td>
      <td>${Number(row.anchors_completed || 0)}/${Number(row.anchors_requested || 0)}</td>
      <td>${asPctNullable(row.avg_annualized_return)}</td>
      <td>${asPctNullable(row.avg_total_return)}</td>
      <td>${asNumNullable(row.avg_sharpe, 3)}</td>
      <td>${asPctNullable(row.avg_max_drawdown)}</td>
      <td>${asPctNullable(row.win_rate)}</td>
      <td>${row.notes || "-"}</td>
      <td>${canOpen ? `<button class="open-timeliness-params-btn" data-run-id="${runId}" data-lookback-days="${lookbackDays}">查看具体参数</button>` : "-"}</td>
    `;
    const openBtn = tr.querySelector(".open-timeliness-params-btn");
    if (openBtn) {
      openBtn.addEventListener("click", async (e) => {
        e.stopPropagation();
        const btn = e.currentTarget;
        const rid = String(btn.dataset.runId || "").trim();
        const lb = Number(btn.dataset.lookbackDays || 0);
        if (!rid || !Number.isFinite(lb) || lb <= 0) return;
        btn.disabled = true;
        try {
          await loadTimelinessLookback(rid, lb);
        } finally {
          btn.disabled = false;
        }
      });
    }
    tbody.appendChild(tr);
  });
}

function localRankStrategies(strategies, rankingMode, topK, minApyPct, minSharpe, maxMddPct) {
  const minAnnualizedReturn = Number(minApyPct) / 100;
  const minSharpeNum = Number(minSharpe);
  const maxDrawdownAbs = Number(maxMddPct) / 100;

  const filtered = (strategies || [])
    .map((item) => ({ ...item }))
    .filter(
      (item) =>
        Number(item.annualized_return ?? -999) >= minAnnualizedReturn &&
        Number(item.sharpe ?? -999) >= minSharpeNum &&
        Math.abs(Number(item.max_drawdown ?? -1)) <= maxDrawdownAbs
    );

  if (rankingMode === "mdd_asc_return_desc") {
    filtered.sort(
      (a, b) =>
        Math.abs(Number(a.max_drawdown)) - Math.abs(Number(b.max_drawdown)) ||
        Number(b.annualized_return) - Number(a.annualized_return)
    );
  } else if (rankingMode === "sharpe_desc_return_desc") {
    filtered.sort(
      (a, b) =>
        Number(b.sharpe) - Number(a.sharpe) ||
        Number(b.annualized_return) - Number(a.annualized_return) ||
        Math.abs(Number(a.max_drawdown)) - Math.abs(Number(b.max_drawdown))
    );
  } else {
    filtered.sort(
      (a, b) =>
        Number(b.annualized_return) - Number(a.annualized_return) ||
        Math.abs(Number(a.max_drawdown)) - Math.abs(Number(b.max_drawdown))
    );
  }

  return filtered.slice(0, Math.max(1, Number(topK) || 1)).map((item, idx) => ({ ...item, rank: idx + 1 }));
}

function renderTable(strategies) {
  const tbody = document.querySelector("#result_table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  strategiesById = {};

  strategies.forEach((s) => {
    strategiesById[s.strategy_id] = s;
    const portfolioText = (s.portfolio || [])
      .map((leg) => {
        const lev = Number(leg.leverage);
        const levText = Number.isFinite(lev) && lev > 0 ? ` x${lev.toFixed(1)}` : "";
        return `${leg.asset}${leg.direction === "long" ? "(L)" : "(S)"} ${(Number(leg.weight) * 100).toFixed(1)}%${levText}`;
      })
      .join(" / ");

    const tr = document.createElement("tr");
    tr.dataset.strategyId = s.strategy_id;
    tr.innerHTML = `
      <td>${Number(s.rank || 0)}</td>
      <td>${asPct(s.annualized_return)}</td>
      <td>${asPct(s.total_return)}</td>
      <td>${Number(s.sharpe).toFixed(2)}</td>
      <td>${asPct(s.max_drawdown)}</td>
      <td>${s.params?.rehedge_hours ?? "-"}</td>
      <td>${Number(s.params?.rebalance_threshold_pct ?? 0).toFixed(1)}</td>
      <td>${Number(s.params?.long_leverage ?? 0).toFixed(1)}</td>
      <td>${Number(s.params?.short_leverage ?? 0).toFixed(1)}</td>
      <td>${portfolioText}</td>
      <td>${Number(s.funding_income).toFixed(2)}</td>
      <td>${Number(s.trading_fees).toFixed(2)}</td>
      <td><button class="curve-btn" data-id="${s.strategy_id}">加载</button></td>
      <td><button class="prefill-btn" data-id="${s.strategy_id}">回填自定义</button></td>
      <td><button class="use-btn" data-id="${s.strategy_id}">选用</button></td>
    `;

    tr.querySelector(".curve-btn").addEventListener("click", async (e) => {
      e.stopPropagation();
      const btn = e.currentTarget;
      btn.disabled = true;
      setProgress("curve", 5);
      try {
        renderCurve(strategiesById[btn.dataset.id]?.equity_curve || []);
        setProgress("curve", 100);
        showCurveHint(`Loaded equity curve for Rank #${s.rank}.`);
      } finally {
        btn.disabled = false;
      }
    });

    tr.querySelector(".use-btn").addEventListener("click", (e) => {
      e.stopPropagation();
      selectedStrategyId = e.currentTarget.dataset.id;
      const reverseDirections = isReverseFillEnabled();
      showCalcStatus(
        `Selected strategy ${selectedStrategyId}. ` +
        (reverseDirections ? "当前为反向模式（多空互换）。" : "当前为正向模式。") +
        " You can calculate positions now."
      );
      const strategy = strategiesById[selectedStrategyId];
      if (strategy && strategy.params) {
        const longLev = Number(strategy.params.long_leverage ?? 1);
        const shortLev = Number(strategy.params.short_leverage ?? 1);
        document.getElementById("calc_long_leverage").value = Number(reverseDirections ? shortLev : longLev).toFixed(1);
        document.getElementById("calc_short_leverage").value = Number(reverseDirections ? longLev : shortLev).toFixed(1);
      }
    });

    tr.querySelector(".prefill-btn").addEventListener("click", (e) => {
      e.stopPropagation();
      const sid = e.currentTarget.dataset.id;
      prefillCustomStrategy(sid);
    });

    tbody.appendChild(tr);
  });

  if (selectedCustomStrategyId && !strategiesById[selectedCustomStrategyId]) {
    selectedCustomStrategyId = null;
    showCustomPrefillMeta("Prefill selection cleared. Please select again.");
  }
  updateSelectedCustomRowHighlight();
}

function updateSelectedCustomRowHighlight() {
  const rows = document.querySelectorAll("#result_table tbody tr");
  rows.forEach((row) => {
    if (row.dataset.strategyId === selectedCustomStrategyId) {
      row.classList.add("prefill-selected");
    } else {
      row.classList.remove("prefill-selected");
    }
  });
}

function prefillCustomStrategy(strategyId) {
  const s = strategiesById[strategyId];
  if (!s) {
    showCustomPrefillMeta("Prefill failed: strategy not found.");
    return;
  }
  selectedCustomStrategyId = strategyId;
  updateSelectedCustomRowHighlight();

  const customStart = document.getElementById("custom_start_date");
  const customEnd = document.getElementById("custom_end_date");
  const customCapital = document.getElementById("custom_initial_capital");
  if (customStart && !customStart.value) customStart.value = document.getElementById("start_date").value;
  if (customEnd && !customEnd.value) customEnd.value = document.getElementById("end_date").value;
  if (customCapital && !customCapital.value) customCapital.value = document.getElementById("initial_capital").value;

  const reverseDirections = isReverseFillEnabled();
  const p = s.params || {};
  showCustomPrefillMeta(
    `已回填 Rank #${Number(s.rank || 0)}: Rehedge=${p.rehedge_hours ?? "-"}h, 阈值=${Number(
      p.rebalance_threshold_pct ?? 0
    ).toFixed(1)}%, 多头杠杆=${Number(p.long_leverage ?? 0).toFixed(1)}x, 空头杠杆=${Number(
      p.short_leverage ?? 0
    ).toFixed(1)}x, 方向=${reverseDirections ? "反向" : "正向"}`
  );
}

async function runBacktest() {
  const req = collectRequest();
  startAutoProgress("backtest", 4, 90, 2, 300);
  setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], true);
  showStatus("Running backtest + ML optimization...");
  showCurveHint("Use Load in ranking table to view curve.");

  try {
    const resp = await fetch("/api/backtest/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    setProgressAtLeast("backtest", 75);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "回测失败"));

    selectedStrategyId = null;
    selectedCustomStrategyId = null;
    currentTimelinessRunId = "";
    latestDataSource = "live";
    openedHistoryByMode = {};
    openedHistoryFallback = [];
    latestStrategies = payload.strategies || [];
    renderTable(latestStrategies);
    loadHistoryRuns().catch(() => {});
    stopAutoProgress("backtest", 100);
    showStatus(`Done: returned ${latestStrategies.length} strategies.`);
  } catch (err) {
    stopAutoProgress("backtest", 100);
    showStatus(err.message || "Backtest failed");
  } finally {
    setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], false);
  }
}

async function runTimelinessAnalysis() {
  const req = collectTimelinessRequest();
  stopAutoProgress("backtest", 0);
  setProgress("backtest", 0);
  setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], true);
  showStatus("正在执行参数时效性分析...");
  showTimelinessStatus("运行中：将对多个 L 进行滚动前瞻评分。");

  try {
    const resp = await fetch("/api/backtest/timeliness", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "参数时效性分析失败"));

    selectedStrategyId = null;
    selectedCustomStrategyId = null;
    currentTimelinessRunId = String(payload.timeliness_run_id || "").trim();
    latestDataSource = "live";
    openedHistoryByMode = {};
    openedHistoryFallback = [];
    latestStrategies = payload.strategies || [];
    renderTable(latestStrategies);
    renderTimelinessTable(payload.lookback_results || [], payload.best_lookback_days, currentTimelinessRunId);
    setProgress("backtest", 100);

    const best = Number(payload.best_lookback_days || 0);
    const deployStart = payload.decision_date || req.decision_date;
    const deployEnd = payload.deploy_end_date || "-";
    const learnedMinApy = Number(payload.applied_min_apy_pct);
    const learnedMinSharpe = Number(payload.applied_min_sharpe);
    const learnedMaxMdd = Number(payload.applied_max_mdd_pct);
    const hasLearnedThresholds =
      Number.isFinite(learnedMinApy) &&
      Number.isFinite(learnedMinSharpe) &&
      Number.isFinite(learnedMaxMdd);
    if (hasLearnedThresholds) {
      showTimelinessStatus(
        `Best L=${best} 天, 实盘窗口 ${deployStart} ~ ${deployEnd}. ` +
        `学习阈值: APY>=${learnedMinApy.toFixed(2)}%, Sharpe>=${learnedMinSharpe.toFixed(3)}, MDD<=${learnedMaxMdd.toFixed(2)}%`
      );
    } else {
      showTimelinessStatus(`Best L=${best} 天, 实盘窗口 ${deployStart} ~ ${deployEnd}.`);
    }
    showStatus(`时效性分析完成：Best L=${best}，返回 ${latestStrategies.length} 个策略。`);
    loadHistoryRuns().catch(() => {});
    loadTimelinessHistoryRuns().catch(() => {});
    if (currentTimelinessRunId) {
      openTimelinessHistoryRun(currentTimelinessRunId).catch(() => {});
    }
  } catch (err) {
    setProgressAtLeast("backtest", 0);
    showStatus(err.message || "参数时效性分析失败");
    showTimelinessStatus(err.message || "参数时效性分析失败");
  } finally {
    setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], false);
  }
}

async function loadTimelinessLookback(runId, lookbackDays, anchorIndex = null) {
  const rid = String(runId || "").trim();
  const lb = Math.trunc(Number(lookbackDays));
  const anchor =
    Number.isFinite(Number(anchorIndex)) && Math.trunc(Number(anchorIndex)) > 0
      ? Math.trunc(Number(anchorIndex))
      : resolveLookbackAnchorIndex(rid);
  if (!rid || !Number.isFinite(lb) || lb <= 0) {
    throw new Error("无效的时效性参数窗口。");
  }
  if (!Number.isFinite(anchor) || anchor <= 0) {
    throw new Error("无效的锚点序号。");
  }

  timelinessLookbackLoadState = {
    running: true,
    runId: rid,
    lookbackDays: lb,
    anchorIndex: anchor,
  };
  setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], true);
  setTimelinessLookbackButtonsDisabled(true);
  setProgress("timeliness_lookup", 0);
  showTimelinessLookupRuntime(`运行中: L=${lb} 天, 锚点=${anchor}，等待任务启动...`);
  showTimelinessStatus(`正在加载 L=${lb} 天, 锚点=${anchor} 的具体参数...`);
  try {
    const resp = await fetch(
      `/api/history/timeliness/${encodeURIComponent(rid)}/lookback/${encodeURIComponent(lb)}?anchor_index=${encodeURIComponent(anchor)}`,
      { method: "POST" }
    );
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "加载具体参数失败"));

    const rankingMode = payload.ranking_mode || "sharpe_desc_return_desc";
    const rankingSelect = document.getElementById("ranking_mode");
    if (rankingSelect) rankingSelect.value = rankingMode;

    currentTimelinessRunId = rid;
    selectedStrategyId = null;
    selectedCustomStrategyId = null;
    latestDataSource = "live";
    openedHistoryByMode = {};
    openedHistoryFallback = [];
    latestStrategies = payload.strategies || [];
    renderTable(latestStrategies);

    const usedAnchor = Math.trunc(Number(payload.anchor_index || anchor));
    showStatus(`已加载 L=${lb} 天, 锚点=${usedAnchor} 的具体参数，返回 ${latestStrategies.length} 个策略。`);
    const lookbackMinApy = Number(payload.applied_min_apy_pct);
    const lookbackMinSharpe = Number(payload.applied_min_sharpe);
    const lookbackMaxMdd = Number(payload.applied_max_mdd_pct);
    const hasLookbackThresholds =
      Number.isFinite(lookbackMinApy) &&
      Number.isFinite(lookbackMinSharpe) &&
      Number.isFinite(lookbackMaxMdd);
    if (hasLookbackThresholds) {
      showTimelinessStatus(
        `L=${lb} 天, 锚点=${usedAnchor} | 训练区间: ${payload.train_start_date} ~ ${payload.train_end_date} | ` +
        `对应前瞻: ${payload.test_start_date} ~ ${payload.test_end_date} | ` +
        `阈值: APY>=${lookbackMinApy.toFixed(2)}%, Sharpe>=${lookbackMinSharpe.toFixed(3)}, MDD<=${lookbackMaxMdd.toFixed(2)}%`
      );
    } else {
      showTimelinessStatus(
        `L=${lb} 天, 锚点=${usedAnchor} | 训练区间: ${payload.train_start_date} ~ ${payload.train_end_date} | ` +
        `对应前瞻: ${payload.test_start_date} ~ ${payload.test_end_date}`
      );
    }
    showTimelinessHistoryStatus(`已加载历史时效性 run=${rid} 的 L=${lb}, 锚点=${usedAnchor} 参数。`);
    showCurveHint("请在收益率排名中点击“加载”查看曲线。");
    showCalcStatus("请在收益率排名中点击“选用”，再计算仓位。");
    setProgress("timeliness_lookup", 100);
    setProgress("backtest", 100);
    showTimelinessLookupRuntime(
      `已完成: L=${lb} 天, 锚点=${usedAnchor} | 训练区间 ${payload.train_start_date} ~ ${payload.train_end_date} | ` +
      `前瞻区间 ${payload.test_start_date} ~ ${payload.test_end_date}`
    );

    const table = document.getElementById("result_table");
    if (table) table.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (err) {
    const msg = err.message || "加载具体参数失败";
    showStatus(msg);
    showTimelinessStatus(msg);
    showTimelinessLookupRuntime(`失败: ${msg}`);
    throw err;
  } finally {
    timelinessLookbackLoadState = {
      running: false,
      runId: rid,
      lookbackDays: lb,
      anchorIndex: anchor,
    };
    setTimelinessLookbackButtonsDisabled(false);
    setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], false);
  }
}

async function runCustomBacktest() {
  const req = collectCustomRequest();
  startAutoProgress("backtest", 4, 90, 2, 300);
  setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], true);
  showStatus(
    "Running custom portfolio backtest..." +
    (req.reverse_directions ? "（反向模式）" : "（正向模式）")
  );

  try {
    const resp = await fetch("/api/backtest/custom/refill", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req),
    });
    setProgressAtLeast("backtest", 75);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "Custom backtest failed"));

    selectedStrategyId = null;
    selectedCustomStrategyId = null;
    currentTimelinessRunId = "";
    latestDataSource = "live";
    openedHistoryByMode = {};
    openedHistoryFallback = [];
    latestStrategies = payload.strategies || [];
    renderTable(latestStrategies);
    stopAutoProgress("backtest", 100);
    showStatus(
      `Custom backtest done: ${req.start_date} ~ ${req.end_date}, initial ${Number(req.initial_capital_usdt).toFixed(2)} USDT, ` +
      `mode=${req.reverse_directions ? "reverse" : "normal"}.`
    );
  } catch (err) {
    stopAutoProgress("backtest", 100);
    showStatus(err.message || "Custom backtest failed");
  } finally {
    setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], false);
  }
}

async function applyRank() {
  if (!latestStrategies.length) {
    showStatus("No backtest results to rerank.");
    return;
  }
  startAutoProgress("backtest", 6, 90, 2, 220);
  setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], true);
  const rankingMode = document.getElementById("ranking_mode").value || "sharpe_desc_return_desc";
  const minApyPct = parseNumber(document.getElementById("min_apy_pct").value, 0);
  const minSharpe = parseNumber(document.getElementById("min_sharpe").value, 1.5);
  const maxMddPct = parseNumber(document.getElementById("max_mdd_pct").value, 30);
  const rankTopK = parseNumber(document.getElementById("rank_top_k").value, 50);

  if (latestDataSource === "history") {
    try {
      const baseList = Array.isArray(openedHistoryByMode[rankingMode])
        ? openedHistoryByMode[rankingMode]
        : [];
      const sourceList =
        baseList.length > 0
          ? baseList
          : (openedHistoryFallback.length > 0 ? openedHistoryFallback : latestStrategies);
      selectedStrategyId = null;
      selectedCustomStrategyId = null;
      latestStrategies = localRankStrategies(sourceList, rankingMode, rankTopK, minApyPct, minSharpe, maxMddPct);
      renderTable(latestStrategies);
      stopAutoProgress("backtest", 100);
      showStatus("Applied filter/sort on opened history strategies.");
    } finally {
      setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], false);
    }
    return;
  }

  try {
    const resp = await fetch("/api/backtest/rerank", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ranking_mode: rankingMode,
        min_apy_pct: minApyPct,
        min_sharpe: minSharpe,
        max_mdd_pct: maxMddPct,
        top_k: rankTopK,
      }),
    });
    setProgressAtLeast("backtest", 75);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "Rerank failed"));

    selectedStrategyId = null;
    selectedCustomStrategyId = null;
    latestDataSource = "live";
    openedHistoryByMode = {};
    openedHistoryFallback = [];
    latestStrategies = payload.strategies || [];
    renderTable(latestStrategies);
    stopAutoProgress("backtest", 100);
    showStatus("Applied filter/sort.");
  } catch (err) {
    stopAutoProgress("backtest", 100);
    showStatus(err.message || "Rerank failed");
  } finally {
    setButtonsDisabled(["run_backtest", "run_timeliness", "run_custom_backtest", "apply_rank"], false);
  }
}

async function runPositionCalculator(useStrategy) {
  const totalCapital = parseNumber(document.getElementById("calc_capital").value, 100000);
  const longLev = parseNumber(document.getElementById("calc_long_leverage").value, 1);
  const shortLev = parseNumber(document.getElementById("calc_short_leverage").value, 1);

  const payload = {
    total_capital_usdt: totalCapital,
    long_leverage: longLev,
    short_leverage: shortLev,
  };

  if (useStrategy) {
    if (!selectedStrategyId) {
      showCalcStatus("Please select a strategy in ranking table first.");
      return;
    }
    const selected = strategiesById[selectedStrategyId];
    if (!selected || !Array.isArray(selected.portfolio) || selected.portfolio.length === 0) {
      showCalcStatus("Selected strategy has no portfolio.");
      return;
    }
    const reverseDirections = isReverseFillEnabled();
    payload.portfolio = selected.portfolio.map((leg) => ({
      asset: String(leg.asset || "").toUpperCase(),
      weight: Number(leg.weight),
      direction: reverseDirections
        ? flipDirection(String(leg.direction || "long").toLowerCase())
        : String(leg.direction || "long").toLowerCase(),
      leverage: leg.leverage == null ? null : Number(leg.leverage),
    }));
  } else {
    payload.portfolio = parsePortfolioJson();
  }

  try {
    const resp = await fetch("/api/calculator/plan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(data, "计算失败"));

    renderCalcTable(data.rows || []);
    const reverseDirections = isReverseFillEnabled();
    showCalcStatus(
      `Done (${reverseDirections ? "reverse" : "normal"}): long notional ${Number(data.total_long_notional).toFixed(2)}, short notional ${Number(
        data.total_short_notional
      ).toFixed(2)}.`
    );
  } catch (err) {
    showCalcStatus(err.message || "Calculation failed");
  }
}

function renderCalcTable(rows) {
  const tbody = document.querySelector("#calc_position_table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.asset}</td>
      <td>${r.direction}</td>
      <td>${Number(r.weight_pct || 0).toFixed(2)}%</td>
      <td>${Number(r.margin || 0).toFixed(2)}</td>
      <td>${Number(r.notional || 0).toFixed(2)}</td>
      <td>${Number(r.leverage || 0).toFixed(2)}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderHistoryTable(runs) {
  const tbody = document.querySelector("#history_table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  historyRunsById = {};
  runs.forEach((run) => {
    const runId = String(run.run_id || "");
    historyRunsById[runId] = run;
    const top = Array.isArray(run.top_strategies) ? run.top_strategies : [];
    const top1 = top.length > 0 ? top[0] : null;
    const range =
      run && run.meta
        ? `${run.meta.start_date || "-"} ~ ${run.meta.end_date || "-"}`
        : "-";
    const topSymbols = top1
      ? (top1.portfolio || []).map((leg) => leg.asset).join(" / ")
      : "-";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${String(run.created_at || "").replace("T", " ").replace("Z", "")}</td>
      <td>${range}</td>
      <td>${top1 ? `${(Number(top1.annualized_return) * 100).toFixed(2)}%` : "-"}</td>
      <td>${top1 ? Number(top1.sharpe).toFixed(2) : "-"}</td>
      <td>${top.length}</td>
      <td>${topSymbols}</td>
      <td><button class="open-history-btn" data-run-id="${runId}">Open</button></td>
    `;
    tr.querySelector(".open-history-btn").addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = e.currentTarget.dataset.runId;
      if (!id) return;
      await openHistoryRun(id);
    });
    tbody.appendChild(tr);
  });
}

async function openHistoryRun(runId) {
  try {
    const resp = await fetch(`/api/history/top/${encodeURIComponent(runId)}`);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "Open history failed"));

    const normalizeStrategyList = (list) =>
      (Array.isArray(list) ? list : []).map((item, idx) => ({
        ...item,
        rank: Number(item.rank || idx + 1),
        equity_curve: Array.isArray(item.equity_curve) ? item.equity_curve : [],
      }));

    openedHistoryByMode = {};
    const rawByMode =
      payload && payload.top_strategies_by_mode && typeof payload.top_strategies_by_mode === "object"
        ? payload.top_strategies_by_mode
        : {};
    const knownModes = ["sharpe_desc_return_desc", "return_desc", "mdd_asc_return_desc"];
    knownModes.forEach((mode) => {
      openedHistoryByMode[mode] = normalizeStrategyList(rawByMode[mode]);
    });

    const fallbackList = normalizeStrategyList(payload.top_strategies);
    openedHistoryFallback = fallbackList;
    if (!knownModes.some((mode) => openedHistoryByMode[mode].length > 0) && fallbackList.length > 0) {
      knownModes.forEach((mode) => {
        openedHistoryByMode[mode] = normalizeStrategyList(fallbackList);
      });
    }
    selectedStrategyId = null;
    selectedCustomStrategyId = null;
    currentTimelinessRunId = "";
    latestDataSource = "history";

    const rankingMode = payload && payload.meta ? payload.meta.ranking_mode : "";
    if (typeof rankingMode === "string" && rankingMode) {
      const rankingSelect = document.getElementById("ranking_mode");
      if (rankingSelect) rankingSelect.value = rankingMode;
    }

    const activeMode = document.getElementById("ranking_mode").value || "sharpe_desc_return_desc";
    const sourceList =
      Array.isArray(openedHistoryByMode[activeMode]) && openedHistoryByMode[activeMode].length
        ? openedHistoryByMode[activeMode]
        : fallbackList;
    latestStrategies = localRankStrategies(
      sourceList,
      activeMode,
      parseNumber(document.getElementById("rank_top_k").value, 50),
      parseNumber(document.getElementById("min_apy_pct").value, 0),
      parseNumber(document.getElementById("min_sharpe").value, 1.5),
      parseNumber(document.getElementById("max_mdd_pct").value, 30)
    );
    renderTable(latestStrategies);

    showStatus(`Opened history run ${runId}. Loaded ${latestStrategies.length} strategies.`);
    showCurveHint("Use the Load button in ranking table to view equity curve.");
    showCalcStatus("Use the Select button in ranking table, then calculate positions.");
    showHistoryStatus(`Opened history run: ${runId}`);

    const table = document.getElementById("result_table");
    if (table) table.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (err) {
    showHistoryStatus(err.message || "Open history failed");
  }
}

async function loadHistoryRuns(limit = 20) {
  try {
    const resp = await fetch(`/api/history/top?limit=${encodeURIComponent(limit)}`);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "Load history failed"));
    const runs = payload.runs || [];
    renderHistoryTable(runs);
    showHistoryStatus(`Loaded ${runs.length} history runs.`);
  } catch (err) {
    showHistoryStatus(err.message || "Load history failed");
  }
}

async function clearHistoryRuns() {
  try {
    const resp = await fetch("/api/history/top/clear", {
      method: "POST",
    });
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "Clear history failed"));
    historyRunsById = {};
    openedHistoryByMode = {};
    openedHistoryFallback = [];
    renderHistoryTable([]);
    showHistoryStatus("History cleared.");
  } catch (err) {
    showHistoryStatus(err.message || "Clear history failed");
  }
}

function renderTimelinessHistoryTable(runs) {
  const tbody = document.querySelector("#timeliness_history_table tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  timelinessHistoryRunsById = {};
  (runs || []).forEach((run) => {
    const runId = String(run.run_id || "");
    timelinessHistoryRunsById[runId] = run;
    const lookbacks = Array.isArray(run.lookback_results) ? run.lookback_results : [];
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${String(run.created_at || "").replace("T", " ").replace("Z", "")}</td>
      <td>${run?.meta?.decision_date || "-"}</td>
      <td>${Number(run?.meta?.forward_days || 0) || "-"}</td>
      <td>${Number(run?.meta?.best_lookback_days || 0) || "-"}</td>
      <td>${lookbacks.length}</td>
      <td><button class="open-timeliness-history-btn" data-run-id="${runId}">查看明细</button></td>
    `;
    const openBtn = tr.querySelector(".open-timeliness-history-btn");
    if (openBtn) {
      openBtn.addEventListener("click", async (e) => {
        e.stopPropagation();
        const btn = e.currentTarget;
        const id = String(btn.dataset.runId || "").trim();
        if (!id) return;
        btn.disabled = true;
        try {
          await openTimelinessHistoryRun(id);
        } finally {
          btn.disabled = false;
        }
      });
    }
    tbody.appendChild(tr);
  });
}

function renderTimelinessHistoryDetail(run) {
  const meta = document.getElementById("timeliness_history_detail_meta");
  const tbody = document.querySelector("#timeliness_history_detail_table tbody");
  if (!meta || !tbody) return;

  const runId = String(run?.run_id || "");
  const t0 = run?.meta?.decision_date || "-";
  const bestL = Number(run?.meta?.best_lookback_days || 0) || "-";
  const anchorTotal = Math.trunc(Number(run?.meta?.anchor_count || 0));
  meta.innerText = `当前记录: ${runId} | T0=${t0} | Best L=${bestL} | 锚点总数=${anchorTotal || "-"}`;

  const anchorInput = document.getElementById("lookback_anchor_index");
  if (anchorInput && Number.isFinite(anchorTotal) && anchorTotal > 0) {
    anchorInput.max = String(anchorTotal);
    const curAnchor = Math.trunc(parseNumber(anchorInput.value, 1));
    if (!Number.isFinite(curAnchor) || curAnchor < 1 || curAnchor > anchorTotal) {
      anchorInput.value = "1";
    }
  }

  tbody.innerHTML = "";
  const rows = Array.isArray(run?.lookback_results) ? run.lookback_results : [];
  rows.forEach((row) => {
    const lookbackDays = Number(row.lookback_days || 0);
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${lookbackDays || "-"}</td>
      <td>${asNumNullable(row.score, 4)}</td>
      <td>${Number(row.anchors_completed || 0)}/${Number(row.anchors_requested || 0)}</td>
      <td>${asPctNullable(row.avg_annualized_return)}</td>
      <td>${asPctNullable(row.avg_total_return)}</td>
      <td>${asNumNullable(row.avg_sharpe, 3)}</td>
      <td>${asPctNullable(row.avg_max_drawdown)}</td>
      <td>${asPctNullable(row.win_rate)}</td>
      <td><button class="open-history-lookback-btn" data-run-id="${runId}" data-lookback-days="${lookbackDays}">查看具体参数</button></td>
    `;
    const openBtn = tr.querySelector(".open-history-lookback-btn");
    if (openBtn) {
      openBtn.addEventListener("click", async (e) => {
        e.stopPropagation();
        const btn = e.currentTarget;
        const id = String(btn.dataset.runId || "").trim();
        const lb = Number(btn.dataset.lookbackDays || 0);
        if (!id || !Number.isFinite(lb) || lb <= 0) return;
        btn.disabled = true;
        try {
          await loadTimelinessLookback(id, lb);
        } finally {
          btn.disabled = false;
        }
      });
    }
    tbody.appendChild(tr);
  });
}

async function openTimelinessHistoryRun(runId) {
  try {
    const resp = await fetch(`/api/history/timeliness/${encodeURIComponent(runId)}`);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "打开时效性历史失败"));
    renderTimelinessHistoryDetail(payload);
    showTimelinessHistoryStatus(`已打开时效性历史记录: ${runId}`);
  } catch (err) {
    showTimelinessHistoryStatus(err.message || "打开时效性历史失败");
  }
}

async function loadTimelinessHistoryRuns(limit = 20) {
  try {
    const resp = await fetch(`/api/history/timeliness?limit=${encodeURIComponent(limit)}`);
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "加载时效性历史失败"));
    const runs = payload.runs || [];
    renderTimelinessHistoryTable(runs);
    showTimelinessHistoryStatus(`已加载 ${runs.length} 条时效性历史。`);
  } catch (err) {
    showTimelinessHistoryStatus(err.message || "加载时效性历史失败");
  }
}

async function clearTimelinessHistoryRuns() {
  try {
    const resp = await fetch("/api/history/timeliness/clear", { method: "POST" });
    const payload = await resp.json();
    if (!resp.ok) throw new Error(parseApiError(payload, "清空时效性历史失败"));
    timelinessHistoryRunsById = {};
    renderTimelinessHistoryTable([]);
    const meta = document.getElementById("timeliness_history_detail_meta");
    const tbody = document.querySelector("#timeliness_history_detail_table tbody");
    if (meta) meta.innerText = "请选择一条历史参数时效性记录查看明细。";
    if (tbody) tbody.innerHTML = "";
    setProgress("timeliness_lookup", 0);
    showTimelinessLookupRuntime("Runtime: idle");
    showTimelinessHistoryStatus("时效性历史已清空。");
  } catch (err) {
    showTimelinessHistoryStatus(err.message || "清空时效性历史失败");
  }
}

function setDefaultDates() {
  const end = new Date();
  const start = new Date(end.getTime() - 180 * 24 * 3600 * 1000);
  const fmt = (d) => d.toISOString().slice(0, 10);
  const startEl = document.getElementById("start_date");
  const endEl = document.getElementById("end_date");
  const customStartEl = document.getElementById("custom_start_date");
  const customEndEl = document.getElementById("custom_end_date");
  const decisionEl = document.getElementById("decision_date");
  if (startEl) startEl.value = fmt(start);
  if (endEl) endEl.value = fmt(end);
  if (customStartEl) customStartEl.value = fmt(start);
  if (customEndEl) customEndEl.value = fmt(end);
  if (decisionEl) decisionEl.value = fmt(end);
}

const runBacktestBtn = document.getElementById("run_backtest");
if (runBacktestBtn) runBacktestBtn.addEventListener("click", runBacktest);

const runTimelinessBtn = document.getElementById("run_timeliness");
if (runTimelinessBtn) runTimelinessBtn.addEventListener("click", runTimelinessAnalysis);

const runCustomBtn = document.getElementById("run_custom_backtest");
if (runCustomBtn) runCustomBtn.addEventListener("click", runCustomBacktest);

const applyRankBtn = document.getElementById("apply_rank");
if (applyRankBtn) applyRankBtn.addEventListener("click", applyRank);

const calcBtn = document.getElementById("calc_positions_btn");
if (calcBtn) calcBtn.addEventListener("click", () => runPositionCalculator(true));

const historyRefreshBtn = document.getElementById("history_refresh_btn");
if (historyRefreshBtn) historyRefreshBtn.addEventListener("click", () => loadHistoryRuns());

const historyClearBtn = document.getElementById("history_clear_btn");
if (historyClearBtn) historyClearBtn.addEventListener("click", clearHistoryRuns);

const timelinessHistoryRefreshBtn = document.getElementById("timeliness_history_refresh_btn");
if (timelinessHistoryRefreshBtn) timelinessHistoryRefreshBtn.addEventListener("click", () => loadTimelinessHistoryRuns());

const timelinessHistoryClearBtn = document.getElementById("timeliness_history_clear_btn");
if (timelinessHistoryClearBtn) timelinessHistoryClearBtn.addEventListener("click", clearTimelinessHistoryRuns);

setDefaultDates();
const initialCapitalEl = document.getElementById("initial_capital");
const customInitialCapitalEl = document.getElementById("custom_initial_capital");
if (initialCapitalEl && customInitialCapitalEl && !customInitialCapitalEl.value) {
  customInitialCapitalEl.value = initialCapitalEl.value || "1000";
}
setProgress("backtest", 0);
setProgress("curve", 0);
setProgress("timeliness_lookup", 0);
showTimelinessLookupRuntime("Runtime: idle");
resolveLookbackAnchorIndex("");
loadHistoryRuns().catch(() => {});
loadTimelinessHistoryRuns().catch(() => {});
refreshRuntimeStatus().catch(() => {});
setInterval(() => {
  refreshRuntimeStatus().catch(() => {});
}, 1500);

