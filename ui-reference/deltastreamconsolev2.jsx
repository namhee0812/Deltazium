import React, { useState, useEffect, useMemo, useRef } from "react";

/* DeltaStream NG Console v2 — adds: per-table monitoring + CDC registration wizard */
const T = {
  bg: "#0E1526", surface: "#151F36", surface2: "#1B2745", line: "#26334F",
  text: "#E6ECF7", dim: "#8A97B4", accent: "#53C8E8",
  ok: "#56D89C", warn: "#F5B453", crit: "#F0647A",
  mono: "'IBM Plex Mono', ui-monospace, monospace",
  sans: "'Space Grotesk', system-ui, sans-serif",
};

/* ── seed: registered CDC tables ── */
const seedTables = [
  { id: 1, schema: "SALES", name: "ORDERS", source: "ORCL-RAC1", target: "PostgreSQL", suppLog: "full", status: "ok" },
  { id: 2, schema: "SALES", name: "ORDER_ITEMS", source: "ORCL-RAC1", target: "PostgreSQL", suppLog: "full", status: "ok" },
  { id: 3, schema: "SALES", name: "CUSTOMERS", source: "ORCL-RAC1", target: "Kafka", suppLog: "pk-only", status: "ok" },
  { id: 4, schema: "HR", name: "EMPLOYEES", source: "ORCL-EXA", target: "PostgreSQL", suppLog: "full", status: "warn" },
  { id: 5, schema: "HR", name: "PAYROLL_HISTORY", source: "ORCL-EXA", target: "PostgreSQL", suppLog: "none", status: "crit" },
  { id: 6, schema: "INV", name: "STOCK_MOVEMENTS", source: "ORCL-RAC1", target: "Kafka", suppLog: "full", status: "ok" },
];

/* candidate tables for the wizard (discovered from source dictionary) */
const dictTables = [
  { schema: "SALES", name: "RETURNS", rows: "2.1M", suppLog: "none", longCol: false },
  { schema: "SALES", name: "SHIPMENTS", rows: "8.4M", suppLog: "pk-only", longCol: false },
  { schema: "FIN", name: "GL_JOURNAL_ENTRIES", rows: "44M", suppLog: "none", longCol: true },
  { schema: "FIN", name: "AP_INVOICES", rows: "12M", suppLog: "full", longCol: false },
  { schema: "HR", name: "ATTENDANCE_LOG", rows: "96M", suppLog: "none", longCol: false },
  { schema: "INV", name: "WAREHOUSE_BINS", rows: "310K", suppLog: "pk-only", longCol: false },
];

const sColor = (s) => (s === "ok" ? T.ok : s === "warn" ? T.warn : T.crit);

function Spark({ data, color, w = 90, h = 20 }) {
  const max = Math.max(...data, 1);
  const pts = data.map((v, i) => `${(i / (data.length - 1)) * w},${h - (v / max) * (h - 3) - 1}`).join(" ");
  return <svg width={w} height={h} style={{ display: "block" }}><polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" /></svg>;
}

const Btn = ({ children, primary, ghost, danger, style, ...p }) => (
  <button {...p} style={{
    padding: "8px 14px", borderRadius: 8, fontSize: 12.5, fontWeight: 600, cursor: p.disabled ? "default" : "pointer",
    fontFamily: T.sans, border: ghost ? `1px solid ${T.line}` : "none",
    background: primary ? T.accent : danger ? T.crit : ghost ? "transparent" : T.surface2,
    color: primary ? "#0A1220" : danger ? "#fff" : ghost ? T.dim : T.text,
    opacity: p.disabled ? 0.45 : 1, ...style,
  }}>{children}</button>
);

/* ════════ per-table monitoring view ════════ */
function TablesView({ tables, metrics, onSelect, selectedId }) {
  const [q, setQ] = useState("");
  const [srcFilter, setSrc] = useState("all");
  const rows = tables.filter((t) =>
    (srcFilter === "all" || t.source === srcFilter) &&
    (`${t.schema}.${t.name}`.toLowerCase().includes(q.toLowerCase()))
  );
  const sel = tables.find((t) => t.id === selectedId);
  const selM = metrics[selectedId];

  return (
    <div style={{ display: "flex", height: "100%", minHeight: 0 }}>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
        <div style={{ display: "flex", gap: 10, padding: "12px 16px", alignItems: "center" }}>
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="스키마.테이블 검색"
            style={{ width: 220, background: T.surface2, border: `1px solid ${T.line}`, borderRadius: 8, padding: "8px 11px", color: T.text, fontSize: 12.5 }} />
          {["all", "ORCL-RAC1", "ORCL-EXA"].map((s) => (
            <Btn key={s} onClick={() => setSrc(s)} primary={srcFilter === s} ghost={srcFilter !== s}
              style={{ padding: "6px 11px" }}>{s === "all" ? "전체" : s}</Btn>
          ))}
          <div style={{ marginLeft: "auto", fontFamily: T.mono, fontSize: 11, color: T.dim }}>{rows.length} tables</div>
        </div>
        <div style={{ flex: 1, overflowY: "auto", padding: "0 16px 16px" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12.5 }}>
            <thead>
              <tr style={{ color: T.dim, fontFamily: T.mono, fontSize: 10.5, textAlign: "left" }}>
                {["", "TABLE", "SOURCE → TARGET", "DML/s (5m)", "I / U / D", "LAG", "LAST SCN", "SUPP.LOG"].map((h, i) => (
                  <th key={i} style={{ padding: "8px 10px", borderBottom: `1px solid ${T.line}`, fontWeight: 500 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((t) => {
                const m = metrics[t.id]; if (!m) return null;
                const lag = Math.round(m.lag[m.lag.length - 1]);
                return (
                  <tr key={t.id} onClick={() => onSelect(t.id)}
                    style={{ cursor: "pointer", background: selectedId === t.id ? T.surface2 : "transparent" }}>
                    <td style={{ padding: "10px" }}><span style={{ color: sColor(t.status) }}>●</span></td>
                    <td style={{ padding: "10px", fontFamily: T.mono }}>{t.schema}.<b style={{ color: T.text }}>{t.name}</b></td>
                    <td style={{ padding: "10px", color: T.dim, fontFamily: T.mono, fontSize: 11 }}>{t.source} → {t.target}</td>
                    <td style={{ padding: "10px" }}><Spark data={m.dml} color={t.status === "ok" ? T.accent : sColor(t.status)} /></td>
                    <td style={{ padding: "10px", fontFamily: T.mono, fontSize: 11, color: T.dim }}>
                      <span style={{ color: T.ok }}>{m.ins}</span> / <span style={{ color: T.warn }}>{m.upd}</span> / <span style={{ color: T.crit }}>{m.del}</span>
                    </td>
                    <td style={{ padding: "10px", fontFamily: T.mono, color: lag > 300 ? T.warn : T.text }}>{lag}ms</td>
                    <td style={{ padding: "10px", fontFamily: T.mono, fontSize: 11, color: T.dim }}>{m.scn}</td>
                    <td style={{ padding: "10px" }}>
                      <span style={{
                        fontFamily: T.mono, fontSize: 10, padding: "3px 7px", borderRadius: 5,
                        background: t.suppLog === "full" ? "rgba(86,216,156,.12)" : t.suppLog === "pk-only" ? "rgba(245,180,83,.12)" : "rgba(240,100,122,.12)",
                        color: t.suppLog === "full" ? T.ok : t.suppLog === "pk-only" ? T.warn : T.crit,
                      }}>{t.suppLog}</span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* drill-down */}
      {sel && selM && (
        <div style={{ width: 330, borderLeft: `1px solid ${T.line}`, background: T.surface, padding: 16, overflowY: "auto" }}>
          <div style={{ fontFamily: T.mono, fontSize: 13, fontWeight: 600 }}>{sel.schema}.{sel.name}</div>
          <div style={{ fontSize: 11, color: T.dim, fontFamily: T.mono, marginTop: 3 }}>{sel.source} → {sel.target}</div>
          {[
            { label: "DML throughput", data: selM.dml, unit: "ops/s", color: T.accent },
            { label: "replication lag", data: selM.lag, unit: "ms", color: sel.status === "ok" ? T.accent : sColor(sel.status) },
          ].map((c) => (
            <div key={c.label} style={{ background: T.surface2, borderRadius: 10, padding: 12, border: `1px solid ${T.line}`, marginTop: 12 }}>
              <div style={{ fontSize: 10.5, color: T.dim, fontFamily: T.mono }}>{c.label}</div>
              <div style={{ fontSize: 20, fontWeight: 600, margin: "3px 0 7px" }}>
                {Math.round(c.data[c.data.length - 1]).toLocaleString()} <span style={{ fontSize: 11, color: T.dim }}>{c.unit}</span>
              </div>
              <Spark data={c.data} color={c.color} w={270} h={40} />
            </div>
          ))}
          <div style={{ background: T.surface2, borderRadius: 10, padding: 12, border: `1px solid ${T.line}`, marginTop: 12, fontFamily: T.mono, fontSize: 11, lineHeight: 2, color: T.dim }}>
            last SCN <span style={{ color: T.text, float: "right" }}>{selM.scn}</span><br />
            checkpoint <span style={{ color: T.text, float: "right" }}>2s ago</span><br />
            trail file <span style={{ color: T.text, float: "right" }}>ds000142.trl</span><br />
            DDL events (24h) <span style={{ color: T.text, float: "right" }}>1</span>
          </div>
          {sel.suppLog !== "full" && (
            <div style={{ marginTop: 12, background: "rgba(240,100,122,.08)", border: `1px solid ${sel.suppLog === "none" ? T.crit : T.warn}`, borderRadius: 10, padding: 11, fontSize: 12, lineHeight: 1.55 }}>
              <b style={{ color: sel.suppLog === "none" ? T.crit : T.warn }}>supplemental logging {sel.suppLog}.</b>{" "}
              UPDATE 시 미변경 컬럼 유실 가능 — ALTER TABLE … ADD SUPPLEMENTAL LOG DATA (ALL) 권장.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ════════ CDC registration wizard ════════ */
function Wizard({ onClose, onRegister }) {
  const [step, setStep] = useState(0);
  const [src, setSrcSel] = useState(null);
  const [picked, setPicked] = useState([]);
  const [q, setQ] = useState("");
  const [target, setTarget] = useState(null);
  const steps = ["소스 선택", "테이블 선택", "타깃 매핑", "검토 및 배포"];

  const cands = dictTables.filter((t) => `${t.schema}.${t.name}`.toLowerCase().includes(q.toLowerCase()));
  const toggle = (t) => setPicked((p) => p.some((x) => x.name === t.name) ? p.filter((x) => x.name !== t.name) : [...p, t]);
  const needsGG = picked.some((t) => t.longCol);
  const canNext = [!!src, picked.length > 0, !!target, true][step];

  const yaml = `apiVersion: deltastream/v2
kind: Capture
metadata:
  name: cdc-${(src || "src").toLowerCase()}-${Date.now() % 1000}
spec:
  source: ${src}
  tables:
${picked.map((t) => `    - ${t.schema}.${t.name}`).join("\n")}
  prerequisites:
    supplementalLog: ensure-full   # ${picked.filter((t) => t.suppLog !== "full").length}개 테이블 자동 적용
${needsGG ? "    enableGoldenGateReplication: true   # 30자 초과 컬럼명 감지됨\n" : ""}  target: ${target}
  startFrom: current-scn`;

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(5,9,18,.72)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 50 }}>
      <div style={{ width: 640, maxHeight: "86vh", background: T.surface, border: `1px solid ${T.line}`, borderRadius: 14, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* header + steps */}
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
              {[
                { id: "ORCL-RAC1", sub: "Oracle 19c RAC · redo-direct capture", ok: true },
                { id: "ORCL-EXA", sub: "Exadata · LogMiner capture", ok: true },
                { id: "+ 새 소스 연결", sub: "Oracle / PostgreSQL / MySQL…", ok: false },
              ].map((s) => (
                <div key={s.id} onClick={() => s.ok && setSrcSel(s.id)}
                  style={{ padding: 14, borderRadius: 10, cursor: s.ok ? "pointer" : "default", border: `1px solid ${src === s.id ? T.accent : T.line}`, background: src === s.id ? T.surface2 : "transparent", opacity: s.ok ? 1 : 0.55 }}>
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
                      <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center", fontFamily: T.mono, fontSize: 10.5 }}>
                        <span style={{ color: T.dim }}>{t.rows} rows</span>
                        {t.suppLog !== "full" && <span style={{ color: T.warn }}>supp.log {t.suppLog}</span>}
                        {t.longCol && <span style={{ color: T.crit }}>col&gt;30자</span>}
                      </div>
                    </div>
                  );
                })}
              </div>
              {picked.some((t) => t.suppLog !== "full") && (
                <div style={{ marginTop: 12, fontSize: 12, color: T.warn, lineHeight: 1.55 }}>
                  ⚠ 선택 테이블 중 {picked.filter((t) => t.suppLog !== "full").length}개는 supplemental logging이 부족합니다 — 배포 시 자동으로 활성화됩니다(DDL 권한 필요).
                </div>
              )}
            </div>
          )}

          {step === 2 && (
            <div style={{ display: "grid", gap: 10 }}>
              {[
                { id: "PostgreSQL", sub: "postgres-main · apply parallelism 4" },
                { id: "Kafka", sub: "kafka-cdc · topic per table (cdc.<schema>.<table>)" },
              ].map((t) => (
                <div key={t.id} onClick={() => setTarget(t.id)}
                  style={{ padding: 14, borderRadius: 10, cursor: "pointer", border: `1px solid ${target === t.id ? T.accent : T.line}`, background: target === t.id ? T.surface2 : "transparent" }}>
                  <div style={{ fontWeight: 600, fontSize: 13.5 }}>{t.id}</div>
                  <div style={{ fontSize: 11.5, color: T.dim, fontFamily: T.mono, marginTop: 3 }}>{t.sub}</div>
                </div>
              ))}
              <div style={{ fontSize: 12, color: T.dim, lineHeight: 1.6 }}>
                컬럼 매핑·필터·마스킹 규칙은 등록 후 테이블 상세에서 편집할 수 있어요.
              </div>
            </div>
          )}

          {step === 3 && (
            <div>
              <div style={{ fontSize: 12.5, color: T.dim, marginBottom: 10 }}>
                아래 선언적 설정이 저장소의 원본이 됩니다 — UI 편집과 Git 편집 모두 이 파일을 수정합니다.
              </div>
              <pre style={{ background: "#0B1120", border: `1px solid ${T.line}`, borderRadius: 10, padding: 14, fontFamily: T.mono, fontSize: 11.5, lineHeight: 1.65, color: "#B9C7E4", whiteSpace: "pre-wrap", margin: 0 }}>{yaml}</pre>
            </div>
          )}
        </div>

        <div style={{ display: "flex", gap: 8, padding: "14px 20px", borderTop: `1px solid ${T.line}` }}>
          <Btn ghost onClick={onClose}>취소</Btn>
          <div style={{ flex: 1 }} />
          {step > 0 && <Btn ghost onClick={() => setStep(step - 1)}>이전</Btn>}
          {step < 3
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
      ins: (Math.random() * 900 | 0), upd: (Math.random() * 400 | 0), del: (Math.random() * 60 | 0),
      scn: (48210000000 + ((Math.random() * 9e6) | 0)).toString(),
    });
    setMetrics(Object.fromEntries(tables.map((t) => [t.id, gen(t)])));
    const id = setInterval(() => {
      setMetrics((m) => {
        const next = { ...m };
        for (const t of tables) {
          const cur = next[t.id]; if (!cur) { next[t.id] = gen(t); continue; }
          next[t.id] = {
            ...cur,
            dml: [...cur.dml.slice(1), 50 + Math.random() * 500],
            lag: [...cur.lag.slice(1), t.status === "ok" ? 20 + Math.random() * 80 : 300 + Math.random() * 900],
            scn: (BigInt(cur.scn) + BigInt((Math.random() * 40000) | 0)).toString(),
          };
        }
        return next;
      });
    }, 1800);
    return () => clearInterval(id);
  }, [tables]);

  const register = (picked, src, target) => {
    setTables((ts) => [
      ...ts,
      ...picked.map((p, i) => ({ id: ts.length + i + 1, schema: p.schema, name: p.name, source: src, target, suppLog: "full", status: "ok" })),
    ]);
    setToast(`${picked.length}개 테이블 CDC 배포 완료 — initial SCN 동기화 시작`);
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
        {[["topology", "토폴로지"], ["tables", "테이블 모니터링"]].map(([k, label]) => (
          <button key={k} onClick={() => setView(k)} style={{ background: "none", border: "none", cursor: "pointer", fontFamily: T.sans, fontSize: 13, color: view === k ? T.text : T.dim, fontWeight: view === k ? 600 : 400, borderBottom: view === k ? `2px solid ${T.accent}` : "2px solid transparent", padding: "4px 2px" }}>{label}</button>
        ))}
        <div style={{ marginLeft: "auto" }}>
          <Btn primary onClick={() => setWizardOpen(true)}>＋ 새 CDC 등록</Btn>
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 0 }}>
        {view === "tables"
          ? <TablesView tables={tables} metrics={metrics} onSelect={setSelectedId} selectedId={selectedId} />
          : <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: T.dim, fontSize: 13 }}>
              토폴로지 캔버스는 v1 프로토타입 참조 — 통합 시 이 탭에 렌더링됩니다.
            </div>}
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
