/* 标定谱系页面：所有数据均来自真实接口 */
const $ = (sel) => document.querySelector(sel);

const state = { records: [] };

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(path, opts);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data?.error?.message || `HTTP ${resp.status}`);
    err.status = resp.status;
    err.code = data?.error?.code;
    err.details = data?.error?.details;
    throw err;
  }
  return data;
}

function feedback(msg, cls) {
  const el = $("#feedback");
  el.textContent = msg;
  el.className = "feedback " + (cls || "muted");
}

async function refresh() {
  const records = await api("GET", "/api/records");
  state.records = records;
  renderRecords();
  renderParentOptions();
}

function renderRecords() {
  if (!state.records.length) {
    $("#records").innerHTML = '<p class="muted">暂无记录，先建立一条原始记录吧。</p>';
    return;
  }
  $("#records").innerHTML = state.records.map((r) => {
    const text = typeof r.payload.value !== "undefined"
      ? r.payload.value : JSON.stringify(r.payload);
    const basis = r.parent_ids.length
      ? `<div class="parents-line">直接依据：${
          r.parent_ids.map((p) => `<span class="pid">${esc(p)}</span>`).join("、")
        }</div>`
      : "";
    const src = r.invalidated_by
      ? `<div class="meta">失效来源（稳定）：<span class="src">${esc(r.invalidated_by)}</span>${
          r.invalidated_at ? ` · ${esc(r.invalidated_at)}` : ""
        }</div>`
      : "";
    const rebuilt = r.rebuilt_from
      ? `<div class="meta">替代自：<span class="pid">${esc(r.rebuilt_from)}</span>` +
        `（修复标识 <span class="src rebuild-text">${esc(r.rebuilt_by || "")}</span>）</div>`
      : "";
    return `
      <div class="record">
        <div class="head">
          <span class="id">${esc(r.id)}</span>
          <span class="badge ${esc(r.kind)}">${r.kind === "raw" ? "原始" : "推导"}</span>
          <span class="badge ${esc(r.status)}">${r.status === "valid" ? "有效" : "已失效"}</span>
          ${r.rebuilt_from ? '<span class="badge rebuilt">替代副本</span>' : ""}
        </div>
        <div class="meta">${esc(text)}</div>
        ${basis}
        ${rebuilt}
        ${src}
      </div>`;
  }).join("");
  renderMappings();
}

function renderMappings() {
  const box = $("#mappings");
  if (!box) return;
  // 按修复标识归集旧->新映射（新记录上带 rebuilt_from / rebuilt_by）
  const groups = new Map();
  for (const r of state.records) {
    if (!r.rebuilt_from) continue;
    const key = r.rebuilt_by || "";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push([r.rebuilt_from, r.id, r]);
  }
  if (!groups.size) {
    box.innerHTML = '<p class="muted">尚无替代重建。发起重建后，这里会并排列出旧支与新支及其编号映射。</p>';
    return;
  }
  box.innerHTML = [...groups.entries()].map(([op, pairs]) => {
    pairs.sort((a, b) => a[0].localeCompare(b[0]));
    const rows = pairs.map(([oldId, newId, nr]) => {
      const old = state.records.find((x) => x.id === oldId);
      const oldText = old && typeof old.payload.value !== "undefined"
        ? old.payload.value : "";
      const newText = typeof nr.payload.value !== "undefined"
        ? nr.payload.value : "";
      return `
        <div class="map-row">
          <span class="branch old-branch">
            <span class="id">${esc(oldId)}</span>
            <span class="badge ${old ? esc(old.status) : "invalid"}">${
              old ? (old.status === "valid" ? "有效" : "已失效") : "不存在"}</span>
            <span class="muted map-text">${esc(oldText)}</span>
          </span>
          <span class="arrow">→</span>
          <span class="branch new-branch">
            <span class="id">${esc(newId)}</span>
            <span class="badge valid">有效</span>
            <span class="muted map-text">${esc(newText)}</span>
          </span>
        </div>`;
    }).join("");
    return `
      <div class="map-group">
        <div class="map-head">修复标识 <span class="src rebuild-text">${esc(op)}</span>
          · ${pairs.length} 对编号映射</div>
        ${rows}
      </div>`;
  }).join("");
}

function renderParentOptions() {
  const valids = state.records.filter((r) => r.status === "valid");
  const box = $("#parents");
  if (!valids.length) {
    box.innerHTML = '<span class="muted">当前没有可引用的有效记录</span>';
    return;
  }
  box.innerHTML = valids.map((r) => `
    <label><input type="checkbox" value="${esc(r.id)}">
      <span class="id">${esc(r.id)}</span>
      <span class="muted">（${r.kind === "raw" ? "原始" : "推导"}）</span>
    </label>`).join("");
}

$("#kind").addEventListener("change", () => {
  $("#parents-row").hidden = $("#kind").value !== "derived";
});

$("#create-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const body = {
    kind: $("#kind").value,
    payload: { value: $("#value").value || "" },
  };
  if ($("#kind").value === "derived") {
    body.parent_ids = [...document.querySelectorAll("#parents input:checked")]
      .map((c) => c.value);
  }
  try {
    const rec = await api("POST", "/api/records", body);
    feedback(`已创建记录 ${rec.id}（${rec.status}），直接依据：${
      rec.parent_ids.length ? rec.parent_ids.join("、") : "无"
    }`, "ok-text");
    $("#value").value = "";
    await refresh();
  } catch (err) {
    feedback(`创建被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

$("#invalidate-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const target = $("#inv-target").value.trim();
  const op = $("#inv-op").value.trim();
  try {
    const res = await api("POST",
      `/api/records/${encodeURIComponent(target)}/invalidate`,
      { operation_id: op });
    feedback(
      `裁决完成${res.replayed ? "（重复裁决，返回首次结果）" : ""}\n` +
      `操作标识：${res.operation_id}\n失效来源：${res.target_record_id}\n` +
      `级联失效 ${res.cascade.length} 条：${res.cascade.map((c) => c.id).join("、")}`,
      "ok-text");
    await refresh();
  } catch (err) {
    feedback(`裁决失败（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

$("#refresh").addEventListener("click", () =>
  refresh().catch((e) => feedback(e.message, "error-text")));

$("#rebuild-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const target = $("#rb-target").value.trim();
  const value = $("#rb-value").value;
  const op = $("#rb-op").value.trim();
  try {
    const res = await api("POST",
      `/api/records/${encodeURIComponent(target)}/rebuild`,
      { operation_id: op, replacement_payload: { value } });
    const mapLine = res.mapping
      .map((m) => `${m.old_id} → ${m.new_id}`).join("、");
    feedback(
      `替代重建完成${res.replayed ? "（同标识重试，重放首次映射）" : ""}\n` +
      `修复标识：${res.operation_id}\n` +
      `替代新根：${res.replacement_record_id}\n` +
      `编号映射（${res.mapping.length} 对）：${mapLine}\n` +
      `旧根 ${res.target_record_id} 及旧下游已整体失效`,
      "ok-text");
    $("#rb-value").value = "";
    await refresh();
  } catch (err) {
    feedback(`重建被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

refresh().catch((e) => feedback(e.message, "error-text"));
