import React, { useState, useEffect } from "react";

/* DeltaStream NG Console v4 — adds: column mapping step (rename / type convert / exclude / masking) */
const T = {
  bg: "#0E1526", surface: "#151F36", surface2: "#1B2745", line: "#26334F",
  text: "#E6ECF7", dim: "#8A97B4", accent: "#53C8E8",
  ok: "#56D89C", warn: "#F5B453", crit: "#F0647A",
  mono: "'IBM Plex Mono', ui-monospace, monospace",
  sans: "'Space Grotesk', system-ui, sans-serif",
};

const dictTables = [
  { schema: "SALES", name: "RETURNS", rows: "2.1M", suppLog: "none", longCol: false },
  { schema: "FIN", name: "AP_INVOICES", rows: "12M", suppLog: "full", longCol: false },
  { schema: "HR", name: "ATTENDANCE_LOG", rows: "96M", suppLog: "none", longCol: false },
];

/* mock source dictionary: columns per table */
const dictColumns = {
  "SALES.RETURNS": [
    { name: "RETURN_ID", type: "NUMBER(12)", pk: true },
    { name: "ORDER_ID", type: "NUMBER(12)", pk: false },
    { name: "RETURN_REASON_CD", type: "VARCHAR2(8)", pk: false },
    { name: "CUST_EMAIL", type: "VARCHAR2(320)", pk: false, sensitive: true },
    { name: "REFUND_AMT", type: "NUMBER(14,2)", pk: false },
    { name: "CREATED_DT", type: "DATE", pk: false },
    { name: "MEMO_CLOB", type: "CLOB", pk: false },
  ],
  "FIN.AP_INVOICES": [
    { name: "INVOICE_ID", type: "NUMBER(15)", pk: true },
    { name: "VENDOR_NO", type: "VARCHAR2(20)", pk: false },
    { name: "INVOICE_AMT", type: "NUMBER(18,2)", pk: false },
    { name: "DUE_DATE", type: "DATE", pk: false },
    { name: "APPROVED_YN", type: "CHAR(1)", pk: false },
  ],
  "HR.ATTENDANCE_LOG": [
    { name: "LOG_ID", type: "NUMBER(18)", pk: true },
    { name: "EMP_NO", type: "VARCHAR2(12)", pk: false },
    { name: "CHECKIN_TS", type: "TIMESTAMP(6)", pk: false },
    { name: "DEVICE_IP", type: "VARCHAR2(45)", pk: false, sensitive: true },
  ],
};

/* Oracle → PostgreSQL type conversion */
const pgType = (t) => {
  if (/^NUMBER\((\d+),(\d+)\)/.test(t)) return t.replace(/NUMBER\((\d+),(\d+)\)/, "numeric($1,$2)");
  if (/^NUMBER\((\d+)\)/.test(t)) { const p = +t.match(/\((\d+)\)/)[1]; return p <= 9 ? "integer" : "bigint"; }
  if (/^VARCHAR2\((\d+)\)/.test(t)) return t.replace(/VARCHAR2\((\d+)\)/, "varchar($1)");
  if (/^CHAR\((\d+)\)/.test(t)) return t.replace(/^CHAR/, "char");
  if (t === "DATE") return "timestamp(0)";
  if (/^TIMESTAMP/.test(t)) return "timestamptz";
  if (t === "CLOB") return "text";
  return t.toLowerCase();
};
const snake = (s) => s.toLowerCase();

const sColor = (s) => (s === "ok" ? T.ok : s === "warn" ? T.warn : T.crit);

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

/* ════════ column mapping editor ════════ */
function MappingStep({ picked, mapping, setMapping }) {
  const [activeTbl, setActiveTbl] = useState(`${picked[0].schema}.${picked[0].name}`);
  const cols = mapping[activeTbl] || [];

  const update = (colName, patch) =>
    setMapping((m) => ({
      ...m,
      [activeTbl]: m[activeTbl].map((c) => (c.name === colName ? { ...c, ...patch } : c)),
    }));

  const applySnakeAll = () =>
    setMapping((m) => ({ ...m, [activeTbl]: m[activeTbl].map((c) => ({ ...c, target: snake(c.name) })) }));

  const excluded = cols.filter((c) => !c.include).length;
  const masked = cols.filter((c) => c.mask).length;

  return (
    <div>
      {/* table tabs */}
      <div style={{ display: "flex", gap: 6, marginBottom: 12, flexWrap: "wrap" }}>
        {picked.map((t) => {
          const key = `${t.schema}.${t.name}`;
          return (
            <Btn key={key} primary={activeTbl === key} ghost={activeTbl !== key}
              onClick={() => setActiveTbl(key)} style={{ padding: "6px 11px", fontFamily: T.mono, fontSize: 11.5 }}>
              {key}
            </Btn>
          );
        })}
        <div style={{ marginLeft: "auto" }}>
          <Btn ghost onClick={applySnakeAll} style={{ padding: "6px 11px" }}>전체 snake_case 변환</Btn>
        </div>
      </div>

      {/* mapping grid */}
      <div style={{ border: `1px solid ${T.line}`, borderRadius: 10, overflow: "hidden" }}>
        <div style={{ display: "grid", gridTemplateColumns: "36px 1.2fr 1fr 1.2fr 1fr 70px", gap: 0,
          padding: "8px 12px", background: T.surface2, fontFamily: T.mono, fontSize: 10.5, color: T.dim }}>
          <span></span><span>SOURCE COLUMN</span><span>ORACLE TYPE</span><span>TARGET COLUMN</span><span>PG TYPE</span><span>MASK</span>
        </div>
        {cols.map((c) => (
          <div key={c.name} style={{
            display: "grid", gridTemplateColumns: "36px 1.2fr 1fr 1.2fr 1fr 70px", alignItems: "center",
            padding: "7px 12px", borderTop: `1px solid ${T.line}`,
            opacity: c.include ? 1 : 0.4, fontSize: 12,
          }}>
            <div onClick={() => !c.pk && update(c.name, { include: !c.include })}
              title={c.pk ? "PK 컬럼은 제외할 수 없어요" : "포함/제외"}
              style={{ width: 15, height: 15, borderRadius: 4, cursor: c.pk ? "default" : "pointer",
                border: `1.5px solid ${c.include ? T.accent : T.dim}`,
                background: c.include ? T.accent : "transparent",
                display: "flex", alignItems: "center", justifyContent: "center", color: "#0A1220", fontSize: 10, fontWeight: 700 }}>
              {c.include ? "✓" : ""}
            </div>
            <div style={{ fontFamily: T.mono, display: "flex", alignItems: "center", gap: 6 }}>
              {c.name}
              {c.pk && <span style={{ fontSize: 9, padding: "1px 5px", borderRadius: 4, background: "rgba(83,200,232,.15)", color: T.accent }}>PK</span>}
              {c.sensitive && <span style={{ fontSize: 9, padding: "1px 5px", borderRadius: 4, background: "rgba(240,100,122,.15)", color: T.crit }}>PII</span>}
            </div>
            <div style={{ fontFamily: T.mono, fontSize: 11, color: T.dim }}>{c.type}</div>
            <div style={{ paddingRight: 10 }}>
              <input value={c.target} disabled={!c.include}
                onChange={(e) => update(c.name, { target: e.target.value })}
                style={{
                  width: "100%", boxSizing: "border-box", background: T.surface2,
                  border: `1px solid ${c.target !== snake(c.name) && c.target !== c.name.toLowerCase() ? T.accent : T.line}`,
                  borderRadius: 6, padding: "5px 8px", color: T.text, fontFamily: T.mono, fontSize: 11.5,
                }} />
            </div>
            <div style={{ fontFamily: T.mono, fontSize: 11, color: T.dim }}>{pgType(c.type)}</div>
            <div onClick={() => c.sensitive && update(c.name, { mask: !c.mask })}
              title={c.sensitive ? "마스킹 토글" : "PII 컬럼만 마스킹 가능"}
              style={{ justifySelf: "center", cursor: c.sensitive ? "pointer" : "default",
                fontFamily: T.mono, fontSize: 10.5,
                color: c.mask ? T.crit : c.sensitive ? T.dim : `${T.dim}55` }}>
              {c.mask ? "sha256" : c.sensitive ? "off" : "—"}
            </div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", gap: 16, marginTop: 10, fontFamily: T.mono, fontSize: 11, color: T.dim }}>
        <span>{cols.filter((c) => c.include).length}/{cols.length} 컬럼 복제</span>
        {excluded > 0 && <span style={{ color: T.warn }}>{excluded}개 제외</span>}
        {masked > 0 && <span style={{ color: T.crit }}>{masked}개 마스킹(sha256)</span>}
        <span style={{ marginLeft: "auto" }}>타깃 이름을 바꾸면 테두리가 하이라이트됩니다</span>
      </div>
    </div>
  );
}

/* ════════ prerequisite checks (v3 그대로) ════════ */
const buildChecks = (picked) => [
  { id: "arch", label: "ARCHIVELOG 모드", detail: "v$database.log_mode = ARCHIVELOG", result: "pass" },
  { id: "priv", label: "캡처 계정 권한", detail: "SELECT ANY TRANSACTION · LOGMINING · EXECUTE ON DBMS_LOGMNR", result: "pass" },
  { id: "tblsupp", label: "테이블 supplemental logging",
    detail: `${picked.filter((t) => t.suppLog !== "full").length}개 테이블 부족 — 배포 시 자동 활성화`,
    result: picked.some((t) => t.suppLog !== "full") ? "fixable" : "pass" },
  { id: "fra", label: "FRA 여유 공간", detail: "used 82% — 보존 기간 검토 권장", result: "warnOnly" },
];

/* ════════ wizard (6 steps) ════════ */
function Wizard({ onClose, onRegister }) {
  const [step, setStep] = useState(0);
  const [src, setSrc] = useState(null);
  const [picked, setPicked] = useState([]);
  const [target, setTarget] = useState(null);
  const [mapping, setMapping] = useState({});
  const [checks, setChecks] = useState(null);
  const [running, setRunning] = useState(false);
  const steps = ["소스", "테이블", "타깃", "컬럼 매핑", "사전 점검", "검토·배포"];

  const toggle = (t) =>
    setPicked((p) => (p.some((x) => x.name === t.name) ? p.filter((x) => x.name !== t.name) : [...p, t]));

  /* init mapping when entering the mapping step */
  useEffect(() => {
    if (step === 3) {
      setMapping((m) => {
        const next = { ...m };
        picked.forEach((t) => {
          const key = `${t.schema}.${t.name}`;
          if (!next[key]) {
            next[key] = (dictColumns[key] || []).map((c) => ({
              ...c, include: true, target: snake(c.name), mask: false,
            }));
          }
        });
        return next;
      });
    }
    if (step === 4 && !checks) {
      setRunning(true);
      const full = buildChecks(picked);
      setChecks(full.map((c) => ({ ...c, state: "running" })));
      full.forEach((c, i) => setTimeout(() => {
        setChecks((cur) => cur.map((x) => x.id === c.id
          ? { ...x, state: c.result === "pass" ? "pass" : c.result === "warnOnly" ? "warn" : "fail" } : x));
        if (i === full.length - 1) setRunning(false);
      }, 420 * (i + 1)));
    }
  }, [step]); // eslint-disable-line

  const fix = (id) => setChecks((cur) => cur.map((c) => (c.id === id ? { ...c, state: "pass", detail: c.detail + " → 적용 완료" } : c)));
  const allClear = checks && checks.every((c) => c.state === "pass" || c.state === "warn");
  const canNext = [!!src, picked.length > 0, !!target, true, allClear, true][step];

  const yaml = `apiVersion: deltastream/v2
kind: Capture
metadata:
  name: cdc-${(src || "src").toLowerCase()}
spec:
  source: ${src}
  target: ${target}
  tables:
${picked.map((t) => {
    const key = `${t.schema}.${t.name}`;
    const cols = mapping[key] || [];
    const renamed = cols.filter((c) => c.include && c.target !== snake(c.name));
    const dropped = cols.filter((c) => !c.include);
    const masks = cols.filter((c) => c.mask);
    let s = `    - source: ${key}\n      target: ${snake(t.schema)}.${snake(t.name)}`;
    if (renamed.length) s += `\n      columnMap:\n${renamed.map((c) => `        ${c.name}: ${c.target}`).join("\n")}`;
    if (dropped.length) s += `\n      exclude: [${dropped.map((c) => c.name).join(", ")}]`;
    if (masks.length) s += `\n      mask:\n${masks.map((c) => `        ${c.name}: sha256`).join("\n")}`;
    return s;
  }).join("\n")}
  startFrom: current-scn`;

  const icon = (s) => (s === "running" ? <span style={{ color: T.accent }}>◌</span>
    : s === "pass" ? <span style={{ color: T.ok }}>✓</span>
    : s === "warn" ? <span style={{ color: T.warn }}>!</span>
    : <span style={{ color: T.crit }}>✕</span>);

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(5,9,18,.72)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 50 }}>
      <div style={{ width: 720, maxHeight: "90vh", background: T.surface, border: `1px solid ${T.line}`, borderRadius: 14, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ padding: "16px 20px", borderBottom: `1px solid ${T.line}` }}>
          <div style={{ fontWeight: 600, fontSize: 15 }}>새 CDC 등록</div>
          <div style={{ display: "flex", gap: 5, marginTop: 12 }}>
            {steps.map((s, i) => (
              <div key={s} style={{ flex: 1 }}>
                <div style={{ height: 3, borderRadius: 2, background: i <= step ? T.accent : T.line }} />
                <div style={{ fontSize: 10, marginTop: 5, color: i === step ? T.text : T.dim, fontFamily: T.mono }}>{i + 1}. {s}</div>
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
            <div style={{ display: "grid", gap: 7 }}>
              {dictTables.map((t) => {
                const on = picked.some((x) => x.name === t.name);
                return (
                  <div key={t.name} onClick={() => toggle(t)}
                    style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 9, cursor: "pointer", border: `1px solid ${on ? T.accent : T.line}`, background: on ? T.surface2 : "transparent" }}>
                    <div style={{ width: 16, height: 16, borderRadius: 4, border: `1.5px solid ${on ? T.accent : T.dim}`, background: on ? T.accent : "transparent", display: "flex", alignItems: "center", justifyContent: "center", color: "#0A1220", fontSize: 11, fontWeight: 700 }}>{on ? "✓" : ""}</div>
                    <div style={{ fontFamily: T.mono, fontSize: 12.5 }}>{t.schema}.<b>{t.name}</b></div>
                    <div style={{ marginLeft: "auto", display: "flex", gap: 8, fontFamily: T.mono, fontSize: 10.5 }}>
                      <span style={{ color: T.dim }}>{t.rows}</span>
                      {t.suppLog !== "full" && <span style={{ color: T.warn }}>supp.log {t.suppLog}</span>}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {step === 2 && (
            <div style={{ display: "grid", gap: 10 }}>
              {[{ id: "PostgreSQL", sub: "postgres-main · parallelism 4" }, { id: "Kafka", sub: "topic per table" }].map((t) => (
                <div key={t.id} onClick={() => setTarget(t.id)}
                  style={{ padding: 14, borderRadius: 10, cursor: "pointer", border: `1px solid ${target === t.id ? T.accent : T.line}`, background: target === t.id ? T.surface2 : "transparent" }}>
                  <div style={{ fontWeight: 600, fontSize: 13.5 }}>{t.id}</div>
                  <div style={{ fontSize: 11.5, color: T.dim, fontFamily: T.mono, marginTop: 3 }}>{t.sub}</div>
                </div>
              ))}
            </div>
          )}

          {step === 3 && picked.length > 0 && Object.keys(mapping).length > 0 && (
            <MappingStep picked={picked} mapping={mapping} setMapping={setMapping} />
          )}

          {step === 4 && checks && (
            <div style={{ display: "grid", gap: 8 }}>
              {checks.map((c) => (
                <div key={c.id} style={{ display: "flex", alignItems: "flex-start", gap: 12, padding: "12px 14px", borderRadius: 10, background: T.surface2, border: `1px solid ${c.state === "fail" ? T.crit : T.line}` }}>
                  <div style={{ width: 18, textAlign: "center", fontFamily: T.mono, fontSize: 14, marginTop: 1 }}>{icon(c.state)}</div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 13, fontWeight: 600 }}>{c.label}</div>
                    <div style={{ fontSize: 11.5, color: T.dim, fontFamily: T.mono, marginTop: 3, lineHeight: 1.5 }}>{c.detail}</div>
                  </div>
                  {c.state === "fail" && <Btn primary style={{ padding: "6px 11px", alignSelf: "center" }} onClick={() => fix(c.id)}>자동 수정</Btn>}
                </div>
              ))}
              {!running && (
                <div style={{ fontSize: 12, color: allClear ? T.ok : T.dim, marginTop: 4 }}>
                  {allClear ? "✓ 모든 필수 점검 통과 — 배포 가능" : "실패 항목을 수정하면 진행할 수 있어요."}
                </div>
              )}
            </div>
          )}

          {step === 5 && (
            <pre style={{ background: "#0B1120", border: `1px solid ${T.line}`, borderRadius: 10, padding: 14, fontFamily: T.mono, fontSize: 11.5, lineHeight: 1.65, color: "#B9C7E4", whiteSpace: "pre-wrap", margin: 0 }}>{yaml}</pre>
          )}
        </div>

        <div style={{ display: "flex", gap: 8, padding: "14px 20px", borderTop: `1px solid ${T.line}` }}>
          <Btn ghost onClick={onClose}>취소</Btn>
          <div style={{ flex: 1 }} />
          {step > 0 && <Btn ghost onClick={() => setStep(step - 1)}>이전</Btn>}
          {step < 5
            ? <Btn primary disabled={!canNext} onClick={() => setStep(step + 1)}>다음</Btn>
            : <Btn primary onClick={() => { onRegister(picked, src, target); onClose(); }}>배포</Btn>}
        </div>
      </div>
    </div>
  );
}

/* ════════ shell (간소화 — 위저드 데모 중심) ════════ */
export default function App() {
  const [wizardOpen, setWizardOpen] = useState(true);
  const [toast, setToast] = useState(null);
  const [registered, setRegistered] = useState([]);

  const register = (picked, src, target) => {
    setRegistered((r) => [...r, ...picked.map((p) => `${p.schema}.${p.name} → ${target}`)]);
    setToast(`${picked.length}개 테이블 CDC 배포 완료 — 컬럼 매핑 규칙이 저장되었습니다`);
    setTimeout(() => setToast(null), 4200);
  };

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column", background: T.bg, color: T.text, fontFamily: T.sans }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
        button:focus-visible, input:focus-visible { outline: 2px solid ${T.accent}; outline-offset: 1px; }
        ::-webkit-scrollbar { width: 8px; height: 8px; } ::-webkit-scrollbar-thumb { background:${T.line}; border-radius:4px; }
      `}</style>

      <div style={{ display: "flex", alignItems: "center", gap: 18, padding: "10px 18px", borderBottom: `1px solid ${T.line}`, background: T.surface }}>
        <div style={{ fontWeight: 600 }}>Delta<span style={{ color: T.accent }}>Stream</span> <span style={{ color: T.dim, fontWeight: 400 }}>NG · 컬럼 매핑 데모</span></div>
        <div style={{ marginLeft: "auto" }}>
          <Btn primary onClick={() => setWizardOpen(true)}>＋ 새 CDC 등록</Btn>
        </div>
      </div>

      <div style={{ flex: 1, padding: 24, overflowY: "auto" }}>
        {registered.length === 0
          ? <div style={{ color: T.dim, fontSize: 13 }}>위저드에서 SALES.RETURNS를 선택해 컬럼 매핑을 확인해보세요 — PII 컬럼(CUST_EMAIL)에 마스킹을 걸어볼 수 있습니다.</div>
          : (
            <div style={{ display: "grid", gap: 8 }}>
              {registered.map((r, i) => (
                <div key={i} style={{ background: T.surface, border: `1px solid ${T.line}`, borderRadius: 10, padding: "12px 16px", fontFamily: T.mono, fontSize: 12.5 }}>
                  <span style={{ color: T.ok }}>●</span>&nbsp; {r}
                </div>
              ))}
            </div>
          )}
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
