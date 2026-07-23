import React, { useState, useEffect, useRef, useMemo } from "react";

/* ────────────────────────────────────────────────
   DeltaStream NG Console — design tokens
   bg deep slate-indigo, surfaces layered, trail-cyan accent
──────────────────────────────────────────────── */
const T = {
  bg: "#0E1526",
  surface: "#151F36",
  surface2: "#1B2745",
  line: "#26334F",
  text: "#E6ECF7",
  dim: "#8A97B4",
  accent: "#53C8E8",
  ok: "#56D89C",
  warn: "#F5B453",
  crit: "#F0647A",
  mono: "'IBM Plex Mono', ui-monospace, monospace",
  sans: "'Space Grotesk', system-ui, sans-serif",
};

/* ── seed data ─────────────────────────────────── */
const seedNodes = [
  { id: "src1", kind: "source", label: "ORCL-RAC1", sub: "Oracle 19c · thread 1,2", x: 90, y: 120, status: "ok" },
  { id: "src2", kind: "source", label: "ORCL-EXA", sub: "Exadata · LogMiner", x: 90, y: 300, status: "warn" },
  { id: "agt1", kind: "agent", label: "agent-01", sub: "redo parse → trail", x: 320, y: 120, status: "ok" },
  { id: "agt2", kind: "agent", label: "agent-02", sub: "LogMiner → trail", x: 320, y: 300, status: "warn" },
  { id: "srv", kind: "server", label: "delta-server", sub: "route · apply", x: 560, y: 210, status: "ok" },
  { id: "tgt1", kind: "target", label: "PostgreSQL", sub: "apply · 4 workers", x: 800, y: 110, status: "ok" },
  { id: "tgt2", kind: "target", label: "Kafka", sub: "topic: cdc.orders", x: 800, y: 310, status: "ok" },
];
const edges = [
  ["src1", "agt1"], ["src2", "agt2"],
  ["agt1", "srv"], ["agt2", "srv"],
  ["srv", "tgt1"], ["srv", "tgt2"],
];

const yamlFor = (n) => {
  if (!n) return "";
  if (n.kind === "agent")
    return `# declarative config — source of truth
apiVersion: deltastream/v2
kind: Agent
metadata:
  name: ${n.label}
spec:
  source: ${n.id === "agt1" ? "ORCL-RAC1" : "ORCL-EXA"}
  capture:
    mode: ${n.id === "agt1" ? "redo-direct" : "logminer"}
    threads: [1, 2]
    supplementalLog: full
  trail:
    format: dstrail/v1
    rotateMB: 256
    compress: zstd
    checkpoint: every 2s
  ship:
    to: delta-server:7801
    tls: true
    retry: exponential`;
  if (n.kind === "server")
    return `apiVersion: deltastream/v2
kind: Server
metadata:
  name: delta-server
spec:
  listen: 0.0.0.0:7801
  routes:
    - from: agent-01
      to: [postgres-main, kafka-cdc]
    - from: agent-02
      to: [postgres-main]
  apply:
    parallelism: 4
    conflictPolicy: source-wins`;
  return `apiVersion: deltastream/v2
kind: ${n.kind === "source" ? "Source" : "Target"}
metadata:
  name: ${n.label}
spec:
  # generated from UI — export to Git
  status: ${n.status}`;
};

/* ── tiny sparkline ────────────────────────────── */
function Spark({ data, color, w = 92, h = 22 }) {
  const max = Math.max(...data, 1);
  const pts = data
    .map((v, i) => `${(i / (data.length - 1)) * w},${h - (v / max) * (h - 3) - 1}`)
    .join(" ");
  return (
    <svg width={w} height={h} style={{ display: "block" }}>
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

const statusColor = (s) => (s === "ok" ? T.ok : s === "warn" ? T.warn : T.crit);

/* ── main ──────────────────────────────────────── */
export default function DeltaStreamConsole() {
  const [nodes] = useState(seedNodes);
  const [selected, setSelected] = useState("agt2");
  const [tab, setTab] = useState("metrics");
  const [metrics, setMetrics] = useState({});
  const [chat, setChat] = useState([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const chatEnd = useRef(null);

  /* live-ish metrics */
  useEffect(() => {
    const init = {};
    nodes.forEach((n) => {
      init[n.id] = {
        lag: Array.from({ length: 24 }, () =>
          n.status === "warn" ? 400 + Math.random() * 900 : 20 + Math.random() * 90
        ),
        tps: Array.from({ length: 24 }, () => 800 + Math.random() * 2400),
      };
    });
    setMetrics(init);
    const id = setInterval(() => {
      setMetrics((m) => {
        const next = {};
        for (const k in m) {
          const isWarn = nodes.find((n) => n.id === k)?.status === "warn";
          next[k] = {
            lag: [...m[k].lag.slice(1), isWarn ? 400 + Math.random() * 900 : 20 + Math.random() * 90],
            tps: [...m[k].tps.slice(1), 800 + Math.random() * 2400],
          };
        }
        return next;
      });
    }, 1800);
    return () => clearInterval(id);
  }, [nodes]);

  useEffect(() => { chatEnd.current?.scrollIntoView({ behavior: "smooth" }); }, [chat, busy]);

  const sel = nodes.find((n) => n.id === selected);
  const selM = metrics[selected];
  const lagNow = selM ? Math.round(selM.lag[selM.lag.length - 1]) : 0;
  const tpsNow = selM ? Math.round(selM.tps[selM.tps.length - 1]) : 0;

  async function ask(q) {
    if (!q.trim() || busy) return;
    const userMsg = { role: "user", content: q };
    setChat((c) => [...c, userMsg]);
    setInput("");
    setBusy(true);
    try {
      const context = `You are the embedded AI assistant inside "DeltaStream NG", a CDC pipeline management console.
Currently selected node: ${sel?.label} (${sel?.kind}), status=${sel?.status}.
Live metrics: lag=${lagNow}ms, throughput=${tpsNow} ops/s.
Recent lag samples (ms): ${selM ? selM.lag.map((v) => Math.round(v)).join(", ") : "n/a"}.
Topology: Oracle sources → agents (redo parse, ship trail files) → delta-server (route/apply) → PostgreSQL, Kafka.
agent-02 captures from Exadata via LogMiner and shows elevated lag.
Answer concisely (3-6 sentences) in the same language the user writes in. Be a pragmatic SRE-style assistant.`;
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: "claude-sonnet-4-6",
          max_tokens: 1000,
          messages: [
            ...chat.map((m) => ({ role: m.role, content: m.content })),
            { role: "user", content: `${context}\n\nUser question: ${q}` },
          ],
        }),
      });
      const data = await res.json();
      const text = (data.content || []).filter((b) => b.type === "text").map((b) => b.text).join("\n") || "(no response)";
      setChat((c) => [...c, { role: "assistant", content: text }]);
    } catch (e) {
      setChat((c) => [...c, { role: "assistant", content: "assistant unreachable — check network and retry." }]);
    } finally {
      setBusy(false);
    }
  }

  const nodeW = 148, nodeH = 78;

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column", background: T.bg, color: T.text, fontFamily: T.sans }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
        @keyframes flow { to { stroke-dashoffset: -28; } }
        @media (prefers-reduced-motion: reduce) { .flowline { animation: none !important; } }
        ::-webkit-scrollbar { width: 8px; } ::-webkit-scrollbar-thumb { background:${T.line}; border-radius:4px; }
        button:focus-visible, input:focus-visible { outline: 2px solid ${T.accent}; outline-offset: 1px; }
      `}</style>

      {/* top bar */}
      <div style={{ display: "flex", alignItems: "center", gap: 14, padding: "10px 18px", borderBottom: `1px solid ${T.line}`, background: T.surface }}>
        <div style={{ fontWeight: 600, letterSpacing: "0.02em" }}>
          Delta<span style={{ color: T.accent }}>Stream</span> <span style={{ color: T.dim, fontWeight: 400 }}>NG Console</span>
        </div>
        <div style={{ marginLeft: "auto", display: "flex", gap: 16, fontFamily: T.mono, fontSize: 12, color: T.dim }}>
          <span><span style={{ color: T.ok }}>●</span> 5 healthy</span>
          <span><span style={{ color: T.warn }}>●</span> 2 degraded</span>
          <span>trail shipped <span style={{ color: T.text }}>1.42 GB/h</span></span>
        </div>
      </div>

      <div style={{ flex: 1, display: "flex", minHeight: 0 }}>
        {/* ── topology canvas ── */}
        <div style={{ flex: 1, position: "relative", overflow: "auto" }}>
          <svg viewBox="0 0 960 430" style={{ width: "100%", minWidth: 760, height: "100%" }}>
            {/* edges with trail-flow animation */}
            {edges.map(([a, b]) => {
              const na = nodes.find((n) => n.id === a), nb = nodes.find((n) => n.id === b);
              const x1 = na.x + nodeW, y1 = na.y + nodeH / 2, x2 = nb.x, y2 = nb.y + nodeH / 2;
              const mx = (x1 + x2) / 2;
              const warn = na.status === "warn" || nb.status === "warn";
              return (
                <g key={a + b}>
                  <path d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`} fill="none" stroke={T.line} strokeWidth="2" />
                  <path className="flowline" d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`} fill="none"
                    stroke={warn ? T.warn : T.accent} strokeWidth="2" strokeDasharray="6 8"
                    style={{ animation: `flow ${warn ? 2.6 : 1.1}s linear infinite`, opacity: 0.9 }} />
                </g>
              );
            })}
            {/* nodes */}
            {nodes.map((n) => {
              const m = metrics[n.id];
              const isSel = n.id === selected;
              return (
                <g key={n.id} transform={`translate(${n.x},${n.y})`} style={{ cursor: "pointer" }}
                   onClick={() => { setSelected(n.id); }}>
                  <rect width={nodeW} height={nodeH} rx="10"
                    fill={T.surface2} stroke={isSel ? T.accent : T.line} strokeWidth={isSel ? 2 : 1} />
                  <circle cx="14" cy="16" r="4" fill={statusColor(n.status)} />
                  <text x="26" y="20" fill={T.text} fontSize="13" fontWeight="600" fontFamily={T.sans}>{n.label}</text>
                  <text x="14" y="37" fill={T.dim} fontSize="10" fontFamily={T.mono}>{n.sub}</text>
                  {m && (
                    <foreignObject x="12" y="46" width="124" height="26">
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <Spark data={m.lag} color={n.status === "warn" ? T.warn : T.accent} w={78} h={20} />
                        <span style={{ fontFamily: T.mono, fontSize: 10, color: T.dim }}>
                          {Math.round(m.lag[m.lag.length - 1])}ms
                        </span>
                      </div>
                    </foreignObject>
                  )}
                </g>
              );
            })}
          </svg>
        </div>

        {/* ── right panel ── */}
        <div style={{ width: 380, borderLeft: `1px solid ${T.line}`, background: T.surface, display: "flex", flexDirection: "column", minHeight: 0 }}>
          <div style={{ padding: "14px 16px 0" }}>
            <div style={{ fontSize: 15, fontWeight: 600 }}>{sel?.label}</div>
            <div style={{ fontFamily: T.mono, fontSize: 11, color: T.dim, marginTop: 2 }}>
              {sel?.kind} · <span style={{ color: statusColor(sel?.status) }}>{sel?.status === "ok" ? "healthy" : "degraded"}</span>
            </div>
            <div style={{ display: "flex", gap: 6, marginTop: 12 }}>
              {["metrics", "config", "assistant"].map((t) => (
                <button key={t} onClick={() => setTab(t)}
                  style={{
                    padding: "6px 12px", fontSize: 12, borderRadius: 7, border: "none", cursor: "pointer",
                    fontFamily: T.sans, fontWeight: 500,
                    background: tab === t ? T.accent : T.surface2,
                    color: tab === t ? "#0A1220" : T.dim,
                  }}>{t === "metrics" ? "관측성" : t === "config" ? "설정 (YAML)" : "AI 어시스턴트"}</button>
              ))}
            </div>
          </div>

          <div style={{ flex: 1, minHeight: 0, overflowY: "auto", padding: 16 }}>
            {tab === "metrics" && selM && (
              <div style={{ display: "grid", gap: 12 }}>
                {[
                  { label: "replication lag", data: selM.lag, unit: "ms", val: lagNow, color: sel.status === "warn" ? T.warn : T.accent },
                  { label: "throughput", data: selM.tps, unit: "ops/s", val: tpsNow, color: T.ok },
                ].map((c) => (
                  <div key={c.label} style={{ background: T.surface2, borderRadius: 10, padding: 14, border: `1px solid ${T.line}` }}>
                    <div style={{ fontSize: 11, color: T.dim, fontFamily: T.mono }}>{c.label}</div>
                    <div style={{ fontSize: 24, fontWeight: 600, margin: "4px 0 8px" }}>
                      {c.val.toLocaleString()} <span style={{ fontSize: 12, color: T.dim }}>{c.unit}</span>
                    </div>
                    <Spark data={c.data} color={c.color} w={310} h={44} />
                  </div>
                ))}
                {sel.status === "warn" && (
                  <div style={{ background: "rgba(245,180,83,0.08)", border: `1px solid ${T.warn}`, borderRadius: 10, padding: 12, fontSize: 12.5, lineHeight: 1.55 }}>
                    <span style={{ color: T.warn, fontWeight: 600 }}>lag elevated.</span>{" "}
                    LogMiner capture on Exadata — archive switch interval 확인 필요.
                    <button onClick={() => { setTab("assistant"); ask("agent-02 lag이 왜 높은지 분석해줘"); }}
                      style={{ display: "block", marginTop: 8, background: "none", border: `1px solid ${T.warn}`, color: T.warn, borderRadius: 6, padding: "5px 10px", fontSize: 12, cursor: "pointer" }}>
                      AI에게 원인 분석 요청 →
                    </button>
                  </div>
                )}
              </div>
            )}

            {tab === "config" && (
              <div>
                <pre style={{ background: "#0B1120", border: `1px solid ${T.line}`, borderRadius: 10, padding: 14, fontFamily: T.mono, fontSize: 11.5, lineHeight: 1.6, color: "#B9C7E4", whiteSpace: "pre-wrap", margin: 0 }}>
                  {yamlFor(sel)}
                </pre>
                <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
                  <button style={{ flex: 1, padding: "8px 0", borderRadius: 8, border: "none", background: T.accent, color: "#0A1220", fontWeight: 600, fontSize: 12.5, cursor: "pointer" }}>Apply changes</button>
                  <button style={{ flex: 1, padding: "8px 0", borderRadius: 8, border: `1px solid ${T.line}`, background: "transparent", color: T.dim, fontSize: 12.5, cursor: "pointer" }}>Export to Git</button>
                </div>
              </div>
            )}

            {tab === "assistant" && (
              <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 300 }}>
                <div style={{ flex: 1, overflowY: "auto", display: "grid", gap: 10, alignContent: "start" }}>
                  {chat.length === 0 && (
                    <div style={{ color: T.dim, fontSize: 12.5, lineHeight: 1.6 }}>
                      선택한 노드의 실시간 메트릭이 컨텍스트로 전달됩니다.<br />
                      예: <em>"이 커넥터 왜 느려?"</em>, <em>"lag 급증 원인 후보는?"</em>
                    </div>
                  )}
                  {chat.map((m, i) => (
                    <div key={i} style={{
                      justifySelf: m.role === "user" ? "end" : "start",
                      maxWidth: "88%", padding: "9px 12px", borderRadius: 10, fontSize: 12.5, lineHeight: 1.6,
                      background: m.role === "user" ? T.accent : T.surface2,
                      color: m.role === "user" ? "#0A1220" : T.text,
                      whiteSpace: "pre-wrap",
                    }}>{m.content}</div>
                  ))}
                  {busy && <div style={{ color: T.dim, fontSize: 12, fontFamily: T.mono }}>analyzing…</div>}
                  <div ref={chatEnd} />
                </div>
                <div style={{ display: "flex", gap: 8, paddingTop: 10 }}>
                  <input value={input} onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && ask(input)}
                    placeholder={`${sel?.label}에 대해 질문…`}
                    style={{ flex: 1, background: T.surface2, border: `1px solid ${T.line}`, borderRadius: 8, padding: "9px 11px", color: T.text, fontSize: 12.5, fontFamily: T.sans }} />
                  <button onClick={() => ask(input)} disabled={busy}
                    style={{ padding: "0 14px", borderRadius: 8, border: "none", background: T.accent, color: "#0A1220", fontWeight: 600, cursor: "pointer", opacity: busy ? 0.5 : 1 }}>↑</button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
