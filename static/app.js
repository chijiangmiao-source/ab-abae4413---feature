/* 标定谱系页面：所有数据均来自真实接口 */
const $ = (sel) => document.querySelector(sel);

const state = { records: [], rebuilds: [] };

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
  const [records, rebuilds] = await Promise.all([
    api("GET", "/api/records"),
    api("GET", "/api/rebuilds"),
  ]);
  state.records = records;
  state.rebuilds = rebuilds;
  renderRecords();
  renderParentOptions();
  renderMappings();
}

/* 重建映射索引：旧编号 -> 新编号，新编号 -> 旧编号 */
function buildRebuildIndex() {
  const oldToNew = new Map();
  const newToOld = new Map();
  for (const rb of state.rebuilds) {
    for (const m of rb.mapping) {
      oldToNew.set(m.old_id, m.new_id);
      newToOld.set(m.new_id, m.old_id);
    }
  }
  return { oldToNew, newToOld };
}

function renderRecords() {
  if (!state.records.length) {
    $("#records").innerHTML = '<p class="muted">暂无记录，先建立一条原始记录吧。</p>';
    return;
  }
  const { oldToNew, newToOld } = buildRebuildIndex();
  $("#records").innerHTML = state.records.map((r) => {
    const text = typeof r.payload.value !== "undefined"
      ? r.payload.value : JSON.stringify(r.payload);
    const basis = r.parent_ids.length
      ? `<div class="parents-line">直接依据：${
          r.parent_ids.map((p) => {
            const repl = oldToNew.get(p);
            return repl
              ? `<span class="pid stale">${esc(p)}</span> <span class="arrow">→</span> <span class="pid fresh">${esc(repl)}</span>`
              : `<span class="pid">${esc(p)}</span>`;
          }).join("、")
        }</div>`
      : "";
    const src = r.invalidated_by
      ? `<div class="meta">失效来源（稳定）：<span class="src">${esc(r.invalidated_by)}</span>${
          r.invalidated_at ? ` · ${esc(r.invalidated_at)}` : ""
        }</div>`
      : "";
    const rebuiltFrom = newToOld.get(r.id);
    const rebuiltNote = rebuiltFrom
      ? `<div class="meta rebuild-note">替代重建副本，源自 <span class="pid stale">${esc(rebuiltFrom)}</span>
         <span class="arrow">→</span> <span class="pid fresh">${esc(r.id)}</span></div>`
      : "";
    const replacedOld = oldToNew.get(r.id);
    const oldNote = (r.status === "invalid" && replacedOld)
      ? `<div class="meta rebuild-note">已被重建替代：<span class="pid stale">${esc(r.id)}</span>
         <span class="arrow">→</span> <span class="pid fresh">${esc(replacedOld)}</span></div>`
      : "";
    return `
      <div class="record ${newToOld.has(r.id) ? "branch-new" : ""} ${
        (r.status === "invalid" && oldToNew.has(r.id)) ? "branch-old" : ""}">
        <div class="head">
          <span class="id">${esc(r.id)}</span>
          <span class="badge ${esc(r.kind)}">${r.kind === "raw" ? "原始" : "推导"}</span>
          <span class="badge ${esc(r.status)}">${r.status === "valid" ? "有效" : "已失效"}</span>
        </div>
        <div class="meta">${esc(text)}</div>
        ${basis}
        ${rebuiltNote}
        ${oldNote}
        ${src}
      </div>`;
  }).join("");
}

function renderMappings() {
  const box = $("#mappings");
  if (!state.rebuilds.length) {
    box.innerHTML = '<p class="muted">尚未发起替代重建。</p>';
    return;
  }
  box.innerHTML = state.rebuilds.map((rb) => {
    const rows = rb.mapping.map((m) => `
      <tr>
        <td class="level">L${m.level}</td>
        <td><span class="pid stale">${esc(m.old_id)}</span></td>
        <td class="arrow-cell">→</td>
        <td><span class="pid fresh">${esc(m.new_id)}</span></td>
        <td class="muted small">依据 ${
          m.parent_ids.length
            ? m.parent_ids.map((p) => `<span class="pid">${esc(p)}</span>`).join("、")
            : "（替代根，无前序）"
        }</td>
      </tr>`).join("");
    return `
      <div class="mapping-block">
        <div class="mapping-head">
          <span class="pid fresh">${esc(rb.replacement_record_id)}</span>
          <span class="muted small">替代旧根</span>
          <span class="pid stale">${esc(rb.target_record_id)}</span>
          <span class="muted small">· 操作标识 <code>${esc(rb.operation_id)}</code></span>
        </div>
        <table class="mapping-table">
          <thead><tr><th>层级</th><th>旧编号</th><th></th><th>新编号</th><th>副本直接依据</th>
        </tr></thead>
          <tbody>${rows}</tbody>
        </table>
        <div class="mapping-foot muted small">
          新支有效 ${rb.rebuilt.length} 条 · 旧支失效 ${rb.invalidated.length} 条
        </div>
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
      { operation_id: op, payload: { value } });
    const lines = res.mapping.map((m) =>
      `L${m.level} ${m.old_id} → ${m.new_id}` +
      (m.parent_ids.length ? `（依据 ${m.parent_ids.join("、")}）` : "（替代根）"));
    feedback(
      `替代重建完成${res.replayed ? "（同标识重试，重放首次映射）" : ""}\n` +
      `操作标识：${res.operation_id}\n旧根：${res.target_record_id}` +
      ` → 新根：${res.replacement_record_id}\n映射：\n${lines.join("\n")}\n` +
      `新支有效 ${res.rebuilt.length} 条，旧支失效 ${res.invalidated.length} 条`,
      "ok-text");
    $("#rb-value").value = "";
    await refresh();
  } catch (err) {
    feedback(`替代重建被拒绝（${err.code || err.status}）：${err.message}\n` +
      (err.details ? `定位信息：${JSON.stringify(err.details, null, 2)}` : ""),
      "error-text");
  }
});

refresh().catch((e) => feedback(e.message, "error-text"));
