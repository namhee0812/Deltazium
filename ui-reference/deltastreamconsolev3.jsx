import React, { useState, useEffect } from "react";

/* DeltaStream NG Console v3 — adds: prerequisite check step + DDL history timeline */
const T = {
  bg: "#0E1526", surface: "#151F36", surface2: "#1B2745", line: "#26334F",
  text: "#E6ECF7", dim: "#8A97B4", accent: "#53C8E8",
  ok: "#56D89C", warn: "#F5B453", crit: "#F0647A",
  mono: "'IBM Plex Mono', ui-monospace, monospace",
  sans: "'Space Grotesk', system-ui, sans-serif",
};

const seedTables = [
  { id: 1, schema: "SALES", name: "ORDERS", source: "ORCL-RAC1", target: "PostgreSQL", suppLog: "full", status: "ok" },
  { id: 2, schema: "SALES", name: "ORDER_ITEMS", source: "ORCL-RAC1", target: "PostgreSQL", suppLog: "full", status: "ok" },
  { id: 3, schema: "SALES", name: "CUSTOMERS", source: "ORCL-RAC1", target: "Kafka", suppLog: "pk-only", status: "ok" },
  { id: 4, schema: "HR", name: "EMPLOYEES", source: "ORCL-EXA", target: "PostgreSQL", suppLog: "full", status: "warn" },
  { id: 5, schema: "HR", name: "PAYROLL_HISTORY", source: "ORCL-EXA", target: "PostgreSQL", suppLog: "none", status: "crit" },
  { id: 6, schema: "INV", name: "STOCK_MOVEMENTS", source: "ORCL-RAC1", target: "Kafka", suppLog: "full", status: "ok" },
];

const dictTables = [
  { schema: "SALES", name: "RETURNS", rows: "2.1M", suppLog: "none", longCol: false },
  { schema: "SALES", name: "SHIPMENTS", rows: "8.4M", suppLog: "pk-only", longCol: false },
  { schema: "FIN", name: "GL_JOURNAL_ENTRIES", rows: "44M", suppLog: "none", longCol: true },
  { schema: "FIN", name: "AP_INVOICES", rows: "12M", suppLog: "full", longCol: false },
  { schema: "HR", name: "ATTENDANCE_LOG", rows: "96M", suppLog: "none", longCol: false },
  { schema: "INV", name: "WAREHOUSE_BINS", rows: "310K", suppLog: "pk-only", longCol: false },
];

/* DDL history events */
const seedDDL = [
  { id: 1, ts: "2026-07-16 09:41:22", scn: "48219442810", table: "SALES.ORDERS", op: "ADD COLUMN",
    sql: "ALTER TABLE SALES.ORDERS ADD (PROMO_CODE VARCHAR2(24))",
    propagated: "applied", note: "타깃 PostgreSQL에 자동 반영 (ALTER TABLE … ADD COLUMN)" },
  { id: 2, ts: "2026-07-15 22:03:57", scn: "48214980112", table: "HR.EMPLOYEES", op: "MODIFY COLUMN",
    sql: "ALTER TABLE HR.EMPLOYEES MODIFY (EMAIL VARCHAR2(320))",
    propagated: "applied", note: "길이 확장 — 무중단 반영" },
  { id: 3, ts: "2026-07-14 14:18:03", scn: "48198231544", table: "HR.PAYROLL_HISTORY", op: "ADD INDEX",
    sql: "CREATE INDEX HR.IDX_PAYROLL_YM ON HR.PAYROLL_HISTORY(PAY_MONTH)",
    propagated: "skipped", note: "인덱스 DDL은 정책상 타깃 미전파 (policy: indexes=skip)" },
  { id: 4, ts: "2026-07-13 10:52:41", scn: "48176003921", table: "SALES.CUSTOMERS", op: "DROP COLUMN",
    sql: "ALTER TABLE SALES.CUSTOMERS DROP (FAX_NUMBER)",
    propagated: "pending", note: "파괴적 변경 — 관리자 승인 대기 (destructive DDL requires approval)" },
  { id: 5, ts: "2026-07-11 16:29:10", scn: "48140887265", table: "INV.STOCK_MOVEMENTS", op: "TRUNCATE",
    sql: "TRUNCATE TABLE INV.STOCK_MOVEMENTS",
    propagated: "blocked", note: "TRUNCATE 전파 차단됨 — 수동 확인 필요 (policy: truncate=block)" },
];

const sColor = (s) => (s === "ok" ? T.ok : s === "warn" ? T.warn : T.crit);
const propColor = (p) => (p === "applied" ? T.ok : p === "pending" ? T.warn : p === "skipped" ? T.dim : T.crit);

function Spark({ data, color, w = 90, h = 20 }) {
  const max = Math.max(...data, 1);
  const pts = data.map((v, i) => `${(i / (data.length - 1)) * w},${h - (v / max) * (h - 3) - 1}`).join(" ");
  return <svg width={w} height={h} style={{ display: "block" }}><polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" /></svg>;
}

const Btn = ({ children, primary, ghost, style, ...p }) => (
  <button {...p} style={{
    padding: "8px 14px", borderRadius: 8, fontSize: 12.5, fontWeight: 600,
    cursor: p.disabled ? "default" : "pointer", fontFamily: T.sans,
    border: ghost ? `1px solid ${T.line}` : "none",
    background: primary ? T.accent : ghost ? "transparent" : T.surface2,
    color: primary ? "#0A1220" : ghost ? T.dim : T.text,
    opacity: p.disabled ? 0.45 : 1, ...style,
  }}>{children}</button>
);

/* ════════ per-table monitoring (from v2, condensed) ════════ */
function TablesView({ tables, metrics, onSelect, selectedId }) {
  const [q, setQ] = useState("");
  const rows = tables.filter((t) => `${t.schema}.${t.name}`.toLowerCase().includes(q.toLowerCase()));
  const sel = tables.find((t) => t.id === selectedId);
  const selM = metrics[selectedId];
  return (
    <div style={{ display: "flex", height: "100%", minHeight: 0 }}>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
        <div style={{ display: "flex", gap: 10, padding: "12px 16px", alignItems: "center" }}>
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="스키마.테이블 검색"
            style={{ width: 220, background: T.surface2, border: `1px solid ${T.line}`, borderRadius: 8, padding: "8px 11px", color: T.text, fontSize: 12.5 }} />
          <div style={{ marginLeft: "auto", fontFamily: T.mono, fontSize: 11, color: T.dim }}>{rows.length} tables</div>
        </div>
        <div style={{ flex: 1, overflowY: "auto", padding: "0 16px 16px" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12.5 }}>
            <thead><tr style={{ color: T.dim, fontFamily: T.mono, fontSize: 10.5, textAlign: "left" }}>
              {["", "TABLE", "SRC → TGT", "DML/s", "LAG", "SUPP.LOG"].map((h, i) =>
                <th key={i} style={{ padding: "8px 10px", borderBottom: `1px solid ${T.line}`, fontWeight: 500 }}>{h}</th>)}
            </tr></thead>
            <tbody>
              {rows.map((t) => {
                const m = metrics[t.id]; if (!m) return null;
                const lag = Math.round(m.lag[m.lag.length - 1]);
                return (
                  <tr key={t.id} onClick={() => onSelect(t.id)} style={{ cursor: "pointer", background: selectedId === t.id ? T.surface2 : "transparent" }}>
                    <td style={{ padding: 10 }}><span style={{ color: sColor(t.status) }}>●</span></td>
                    <td style={{ padding: 10, fontFamily: T.mono }}>{t.schema}.<b style={{ color: T.text }}>{t.name}</b></td>
                    <td style={{ padding: 10, color: T.dim, fontFamily: T.mono, fontSize: 11 }}>{t.source} → {t.target}</td>
                    <td style={{ padding: 10 }}><Spark data={m.dml} color={t.status === "ok" ? T.accent : sColor(t.status)} /></td>
                    <td style={{ padding: 10, fontFamily: T.mono, color: lag > 300 ? T.warn : T.text }}>{lag}ms</td>
                    <td style={{ padding: 10 }}><span style={{ fontFamily: T.mono, fontSize: 10, padding: "3px 7px", borderRadius: 5,
                      background: t.suppLog === "full" ? "rgba(86,216,156,.12)" : t.suppLog === "pk-only" ? "rgba(245,180,83,.12)" : "rgba(240,100,122,.12)",
                      color: t.suppLog === "full" ? T.ok : t.suppLog === "pk-only" ? T.warn : T.crit }}>{t.suppLog}</span></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
      {sel && selM && (
        <div style={{ width: 300, borderLeft: `1px solid ${T.line}`, background: T.surface, padding: 16, overflowY: "auto" }}>
          <div style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 600 }}>{sel.schema}.{sel.name}</div>
          <div style={{ background: T.surface2, borderRadius: 10, padding: 12, border: `1px solid ${T.line}`, marginTop: 12 }}>
            <div style={{ fontSize: 10.5, color: T.dim, fontFamily: T.mono }}>replication lag</div>
            <div style={{ fontSize: 20, fontWeight: 600, margin: "3px 0 7px" }}>{Math.round(selM.lag[selM.lag.length - 1])} <span style={{ fontSize: 11, color: T.dim }}>ms</span></div>
            <Spark data={selM.lag} color={sel.status === "ok" ? T.accent : sColor(sel.status)} w={240} h={40} />
          </div>
          <div style={{ background: T.surface2, borderRadius: 10, padding: 12, border: `1px solid ${T.line}`, marginTop: 12, fontFamily: T.mono, fontSize: 11, lineHeight: 2, color: T.dim }}>
            last SCN <span style={{ color: T.text, float: "right" }}>{selM.scn}</span><br />
            trail file <span style={{ color: T.text, float: "right" }}>ds000142.trl</span>
          </div>
        </div>
      )}
    </div>
  );
}

/* ════════ DDL history timeline ════════ */
function DDLView() {
  const [filter, setFilter] = useState("all");
  const [open, setOpen] = useState(1);
  const events = seedDDL.filter((e) => filter === "all" || e.propagated === filter);
  const states = ["all", "applied", "pending", "blocked", "skipped"];
  return (
    <div style={{ height: "100%", overflowY: "auto", padding: "18px 22px" }}>
      <div style={{ display: "flex", gap: 6, marginBottom: 18 }}>
        {states.map((s) => (
          <Btn key={s} primary={filter === s} ghost={filter !== s} onClick={() => setFilter(s)} style={{ padding: "6px 11px" }}>
            {s === "all" ? "전체" : s}
          </Btn>
        ))}
        <div style={{ marginLeft: "auto", fontFamily: T.mono, fontSize: 11, color: T.dim, alignSelf: "center" }}>{events.length} events · 최근 7일</div>
      </div>

      <div style={{ position: "relative", paddingLeft: 26 }}>
        <div style={{ position: "absolute", left: 8, top: 6, bottom: 6, width: 2, background: T.line }} />
        <div style={{ display: "grid", gap: 12 }}>
          {events.map((e) => {
            const isOpen = open === e.id;
            return (
              <div key={e.id} style={{ position: "relative" }}>
                <div style={{ position: "absolute", left: -24, top: 14, width: 10, height: 10, borderRadius: "50%", background: propColor(e.propagated), border: `2px solid ${T.bg}` }} />
                <div onClick={() => setOpen(isOpen ? null : e.id)}
                  style={{ background: T.surface, border: `1px solid ${isOpen ? T.accent : T.line}`, borderRadius: 12, padding: "13px 16px", cursor: "pointer" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
                    <span style={{ fontFamily: T.mono, fontSize: 11, color: T.dim }}>{e.ts}</span>
                    <span style={{ fontFamily: T.mono, fontSize: 12.5, fontWeight: 600 }}>{e.table}</span>
                    <span style={{ fontSize: 11, fontFamily: T.mono, padding: "2px 8px", borderRadius: 5, background: T.surface2, color: T.text }}>{e.op}</span>
                    <span style={{ marginLeft: "auto", fontSize: 10.5, fontFamily: T.mono, padding: "3px 9px", borderRadius: 5,
                      background: `${propColor(e.propagated)}1d`, color: propColor(e.propagated) }}>{e.propagated}</span>
                  </div>
                  {isOpen && (
                    <div style={{ marginTop: 12 }}>
                      <pre style={{ background: "#0B1120", border: `1px solid ${T.line}`, borderRadius: 9, padding: 12, fontFamily: T.mono, fontSize: 11.5, color: "#B9C7E4", whiteSpace: "pre-wrap", margin: 0 }}>{e.sql}</pre>
                      <div style={{ display: "flex", gap: 14, marginTop: 10, fontFamily: T.mono, fontSize: 11, color: T.dim, flexWrap: "wrap" }}>
                        <span>SCN {e.scn}</span>
                        <span style={{ color: T.text }}>{e.note}</span>
                      </div>
                      {e.propagated === "pending" && (
                        <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                          <Btn primary onClick={(ev) => ev.stopPropagation()}>타깃에 전파 승인</Btn>
                          <Btn ghost onClick={(ev) => ev.stopPropagation()}>이 변경 건너뛰기</Btn>
                        </div>
                      )}
                      {e.propagated === "blocked" && (
                        <div style={{ marginTop: 12, fontSize: 12, color: T.crit }}>
                          정책에 의해 차단됨 — 타깃 데이터 보존을 위해 수동 처리가 필요합니다.
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

/* ════════ prerequisite checks ════════ */
const buildChecks = (picked, needsGG) => [
  { id: "arch", label: "ARCHIVELOG 모드", detail: "v$database.log_mode = ARCHIVELOG", result: "pass" },
  { id: "priv", label: "캡처 계정 권한", detail: "SELECT ANY TRANSACTION · LOGMINING · EXECUTE ON DBMS_LOGMNR", result: "pass" },
  { id: "dbsupp", label: "DB 레벨 supplemental logging", detail: "SUPPLEMENTAL_LOG_DATA_MIN = YES", result: "pass" },
  {
    id: "tblsupp", label: "테이블 supplemental logging",
    detail: `${picked.filter((t) => t.suppLog !== "full").length}개 테이블 부족 — ALTER TABLE … ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`,
    result: picked.some((t) => t.suppLog !== "full") ? "fixable" : "pass",
  },
  {
    id: "gg", label: "enable_goldengate_replication",
    detail: needsGG ? "30자 초과 컬럼명 감지 — 파라미터 TRUE 필요" : "긴 컬럼명 없음 — 불필요",
    result: needsGG ? "fixable" : "pass",
  },
  { id: "fra", label: "FRA 여유 공간", detail: "used 82% — 아카이브 보존 기간 검토 권장", result: "warnOnly" },
];

/* ════════ wizard (5 steps) ════════ */
function Wizard({ onClose, onRegister }) {
  const [step, setStep] = useState(0);
  const [src, setSrc] = useState(null);
  const [picked, setPicked] = useState([]);
  const [q, setQ] = useState("");
  const [target, setTarget] = useState(null);
  const [checks, setChecks] = useState(null);
  const [running, setRunning] = useState(false);
  const steps = ["소스", "테이블", "타깃", "사전 점검", "검토·배포"];
  const needsGG = picked.some((t) => t.longCol);

  const toggle = (t) => setPicked((p) => p.some((x) => x.name === t.name) ? p.filter((x) => x.name !== t.name) : [...p, t]);
  const cands = dictTables.filter((t) => `${t.schema}.${t.name}`.toLowerCase().includes(q.toLowerCase()));

  const runChecks = () => {
    setRunning(true);
    const full = buildChecks(picked, needsGG);
    setChecks(full.map((c) => ({ ...c, state: "running" })));
    full.forEach((c, i) => {
      setTimeout(() => {
        setChecks((cur) => cur.map((x) => x.id === c.id ? { ...x, state: c.result === "pass" ? "pass" : c.result === "warnOnly" ? "warn" : "fail" } : x));
        if (i === full.length - 1) setRunning(false);
      }, 420 * (i + 1));
    });
  };
  useEffect(() => { if (step === 3 && !checks) runChecks(); }, [step]); // eslint-disable-line

  const fix = (id) => setChecks((cur) => cur.map((c) => c.id === id ? { ...c, state: "pass", detail: c.detail + " → 적용 완료" } : c));
  const allClear = checks && checks.every((c) => c.state === "pass" || c.state === "warn");
  const canNext = [!!src, picked.length > 0, !!target, allClear, true][step];

  const yaml = `apiVersion: deltastream/v2
kind: Capture
metadata:
  name: cdc-${(src || "src").toLowerCase()}
spec:
  source: ${src}
  tables:
${picked.map((t) => `    - ${t.schema}.${t.name}`).join("\n")}
${needsGG ? "  parameters:\n    enableGoldenGateReplication: true\n" : ""}  target: ${target}
  startFrom: current-scn`;

  const icon = (s) => s === "running" ? <span style={{ color: T.accent }}>◌</span>
    : s === "pass" ? <span style={{ color: T.ok }}>✓</span>
    : s === "warn" ? <span style={{ color: T.warn }}>!</span>
    : <span style={{ color: T.crit }}>✕</span>;

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(5,9,18,.72)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 50 }}>
      <div style={{ width: 660, maxHeight: "88vh", background: T.surface, border: `1px solid ${T.line}`, borderRadius: 14, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ padding: "16px 20px", borderBottom: `1px solid ${T.line}` }}>
          <div style={{ fontWeight: 600, fontSize: 15 }}>새 CDC 등록</div>
          <div style={{ display: "flex", gap: 6, marginTop: 12 }}>
            {steps.map((s, i) => (
              <div key={s} style={{ flex: 1 }}>
                <div style={{ height: 3, borderRadius: 2, background: i <= step ? T.accent : T.line }} />
                <div style={{ fontSize: 10.5, marginTop: 5, color: i === step ? T.text : T.dim, fontFamily: T.mono }}>{i + 1}. {s}</div>
              </div>
            ))}
          </div>
        </div>

        <div style={{ flex: 1, overflowY: "auto", padding: 20 }}>
          {step === 0 && (
            <div style={{ display: "grid", gap: 10 }}>
              {[{ id: "ORCL-RAC1", sub: "Oracle 19c RAC · redo-direct" }, { id: "ORCL-EXA", sub: "Exadata · LogMiner" }].map((s) => (
                <div key={s.id} onClick={() => setSrc(s.id)}
                  style={{ padding: 14, borderRadius: 10, cursor: "pointer", border: `1px solid ${src === s.id ? T.accent : T.line}`, background: src === s.id ? T.surface2 : "transparent" }}>
                  <div style={{ fontWeight: 600, fontSize: 13.5 }}>{s.id}</div>
                  <div style={{ fontSize: 11.5, color: T.dim, fontFamily: T.mono, marginTop: 3 }}>{s.sub}</div>
                </div>
              ))}
            </div>
          )}

          {step === 1 && (
            <div>
              <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="딕셔너리에서 테이블 검색"
                style={{ width: "100%", boxSizing: "border-box", background: T.surface2, border: `1px solid ${T.line}`, borderRadius: 8, padding: "9px 12px", color: T.text, fontSize: 12.5, marginBottom: 12 }} />
              <div style={{ display: "grid", gap: 7 }}>
                {cands.map((t) => {
                  const on = picked.some((x) => x.name === t.name);
                  return (
                    <div key={t.name} onClick={() => toggle(t)}
                      style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 9, cursor: "pointer", border: `1px solid ${on ? T.accent : T.line}`, background: on ? T.surface2 : "transparent" }}>
                      <div style={{ width: 16, height: 16, borderRadius: 4, border: `1.5px solid ${on ? T.accent : T.dim}`, background: on ? T.accent : "transparent", display: "flex", alignItems: "center", justifyContent: "center", color: "#0A1220", fontSize: 11, fontWeight: 700 }}>{on ? "✓" : ""}</div>
                      <div style={{ fontFamily: T.mono, fontSize: 12.5 }}>{t.schema}.<b>{t.name}</b></div>
                      <div style={{ marginLeft: "auto", display: "flex", gap: 8, fontFamily: T.mono, fontSize: 10.5 }}>
                        <span style={{ color: T.dim }}>{t.rows}</span>
                        {t.suppLog !== "full" && <span style={{ color: T.warn }}>supp.log {t.suppLog}</span>}
                        {t.longCol && <span style={{ color: T.crit }}>col&gt;30자</span>}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {step === 2 && (
            <div style={{ display: "grid", gap: 10 }}>
              {[{ id: "PostgreSQL", sub: "postgres-main · parallelism 4" }, { id: "Kafka", sub: "topic per table (cdc.<schema>.<table>)" }].map((t) => (
                <div key={t.id} onClick={() => setTarget(t.id)}
                  style={{ padding: 14, borderRadius: 10, cursor: "pointer", border: `1px solid ${target === t.id ? T.accent : T.line}`, background: target === t.id ? T.surface2 : "transparent" }}>
                  <div style={{ fontWeight: 600, fontSize: 13.5 }}>{t.id}</div>
                  <div style={{ fontSize: 11.5, color: T.dim, fontFamily: T.mono, marginTop: 3 }}>{t.sub}</div>
                </div>
              ))}
            </div>
          )}

          {step === 3 && checks && (
            <div style={{ display: "grid", gap: 8 }}>
              {checks.map((c) => (
                <div key={c.id} style={{ display: "flex", alignItems: "flex-start", gap: 12, padding: "12px 14px", borderRadius: 10, background: T.surface2, border: `1px solid ${c.state === "fail" ? T.crit : T.line}` }}>
                  <div style={{ width: 18, textAlign: "center", fontFamily: T.mono, fontSize: 14, marginTop: 1 }}>{icon(c.state)}</div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 600 }}>{c.label}</div>
                    <div style={{ fontSize: 11.5, color: T.dim, fontFamily: T.mono, marginTop: 3, lineHeight: 1.5 }}>{c.detail}</div>
                  </div>
                  {c.state === "fail" && (
                    <Btn primary style={{ padding: "6px 11px", alignSelf: "center" }} onClick={() => fix(c.id)}>자동 수정</Btn>
                  )}
                </div>
              ))}
              {!running && (
                <div style={{ fontSize: 12, color: allClear ? T.ok : T.dim, marginTop: 4 }}>
                  {allClear ? "✓ 모든 필수 점검 통과 — 배포 가능" : "실패 항목을 수정하면 다음 단계로 진행할 수 있어요."}
                </div>
              )}
            </div>
          )}

          {step === 4 && (
            <pre style={{ background: "#0B1120", border: `1px solid ${T.line}`, borderRadius: 10, padding: 14, fontFamily: T.mono, fontSize: 11.5, lineHeight: 1.65, color: "#B9C7E4", whiteSpace: "pre-wrap", margin: 0 }}>{yaml}</pre>
          )}
        </div>

        <div style={{ display: "flex", gap: 8, padding: "14px 20px", borderTop: `1px solid ${T.line}` }}>
          <Btn ghost onClick={onClose}>취소</Btn>
          <div style={{ flex: 1 }} />
          {step > 0 && <Btn ghost onClick={() => setStep(step - 1)}>이전</Btn>}
          {step < 4
            ? <Btn primary disabled={!canNext} onClick={() => setStep(step + 1)}>다음</Btn>
            : <Btn primary onClick={() => { onRegister(picked, src, target); onClose(); }}>배포</Btn>}
        </div>
      </div>
    </div>
  );
}

/* ════════ shell ════════ */
export default function App() {
  const [view, setView] = useState("tables");
  const [tables, setTables] = useState(seedTables);
  const [metrics, setMetrics] = useState({});
  const [selectedId, setSelectedId] = useState(4);
  const [wizardOpen, setWizardOpen] = useState(false);
  const [toast, setToast] = useState(null);

  useEffect(() => {
    const gen = (t) => ({
      dml: Array.from({ length: 24 }, () => 50 + Math.random() * 500),
      lag: Array.from({ length: 24 }, () => (t.status === "ok" ? 20 + Math.random() * 80 : 300 + Math.random() * 900)),
      scn: (48210000000 + ((Math.random() * 9e6) | 0)).toString(),
    });
    setMetrics(Object.fromEntries(tables.map((t) => [t.id, gen(t)])));
    const id = setInterval(() => {
      setMetrics((m) => {
        const next = { ...m };
        for (const t of tables) {
          const cur = next[t.id]; if (!cur) { next[t.id] = gen(t); continue; }
          next[t.id] = { ...cur,
            dml: [...cur.dml.slice(1), 50 + Math.random() * 500],
            lag: [...cur.lag.slice(1), t.status === "ok" ? 20 + Math.random() * 80 : 300 + Math.random() * 900],
            scn: (BigInt(cur.scn) + BigInt((Math.random() * 40000) | 0)).toString() };
        }
        return next;
      });
    }, 1800);
    return () => clearInterval(id);
  }, [tables]);

  const register = (picked, src, target) => {
    setTables((ts) => [...ts, ...picked.map((p, i) => ({ id: ts.length + i + 1, schema: p.schema, name: p.name, source: src, target, suppLog: "full", status: "ok" }))]);
    setToast(`${picked.length}개 테이블 CDC 배포 완료 — 사전 점검 결과가 감사 로그에 기록되었습니다`);
    setTimeout(() => setToast(null), 4200);
    setView("tables");
  };

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column", background: T.bg, color: T.text, fontFamily: T.sans }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
        tbody tr:hover { background: ${T.surface2}55; }
        button:focus-visible, input:focus-visible { outline: 2px solid ${T.accent}; outline-offset: 1px; }
        ::-webkit-scrollbar { width: 8px; height: 8px; } ::-webkit-scrollbar-thumb { background:${T.line}; border-radius:4px; }
      `}</style>

      <div style={{ display: "flex", alignItems: "center", gap: 18, padding: "10px 18px", borderBottom: `1px solid ${T.line}`, background: T.surface }}>
        <div style={{ fontWeight: 600 }}>Delta<span style={{ color: T.accent }}>Stream</span> <span style={{ color: T.dim, fontWeight: 400 }}>NG</span></div>
        {[["tables", "테이블 모니터링"], ["ddl", "DDL 이력"]].map(([k, label]) => (
          <button key={k} onClick={() => setView(k)} style={{ background: "none", border: "none", cursor: "pointer", fontFamily: T.sans, fontSize: 13, color: view === k ? T.text : T.dim, fontWeight: view === k ? 600 : 400, borderBottom: view === k ? `2px solid ${T.accent}` : "2px solid transparent", padding: "4px 2px" }}>{label}</button>
        ))}
        <div style={{ marginLeft: "auto" }}>
          <Btn primary onClick={() => setWizardOpen(true)}>＋ 새 CDC 등록</Btn>
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 0 }}>
        {view === "tables"
          ? <TablesView tables={tables} metrics={metrics} onSelect={setSelectedId} selectedId={selectedId} />
          : <DDLView />}
      </div>

      {wizardOpen && <Wizard onClose={() => setWizardOpen(false)} onRegister={register} />}
      {toast && (
        <div style={{ position: "fixed", bottom: 22, left: "50%", transform: "translateX(-50%)", background: T.surface2, border: `1px solid ${T.ok}`, color: T.text, borderRadius: 10, padding: "11px 18px", fontSize: 12.5, boxShadow: "0 8px 30px rgba(0,0,0,.4)" }}>
          <span style={{ color: T.ok }}>✓</span> {toast}
        </div>
      )}
    </div>
  );
}
