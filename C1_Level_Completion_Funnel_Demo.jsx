import { useState, useMemo } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceLine, Cell, ComposedChart, Line, Area
} from "recharts";

// ══════════════════════════════════════════════════════════════
// SIMULATED DATA — Flood-It Level Funnel
// Based on realistic casual puzzle game patterns
// Total users: ~52,000 (from Firebase public dataset range)
// ══════════════════════════════════════════════════════════════
const TOTAL_USERS = 52847;

const generateLevelData = () => {
  const rawCompletion = [
    // Early levels — high completion, smooth onboarding
    98.2, 95.1, 92.8, 90.5, 88.2, 86.0, 83.5, 81.2, 79.0, 76.8,
    // Level 11-15 — first difficulty ramp (new mechanic: 5 colors → 6)
    74.2, 71.5, 68.0, 58.3, 55.9,
    // Level 16-20 — recovery, slightly easier
    54.2, 52.8, 51.5, 50.0, 48.6,
    // Level 21-25 — gradual decline
    47.0, 45.2, 43.8, 42.1, 40.5,
    // Level 26-30 — second cliff (board size increase 10x10 → 12x12)
    38.8, 37.0, 35.2, 25.8, 24.1,
    // Level 31-35 — stabilize after cliff
    23.2, 22.5, 21.8, 21.0, 20.3,
    // Level 36-40 — gradual
    19.5, 18.8, 18.1, 17.3, 16.5,
    // Level 41-45 — third cliff (reduced moves + 7 colors)
    15.6, 14.8, 10.2, 9.5, 9.0,
    // Level 46-50 — endgame, only dedicated players
    8.5, 8.0, 7.5, 7.0, 6.5,
  ];

  return rawCompletion.map((rate, i) => {
    const levelNum = i + 1;
    const prevRate = i > 0 ? rawCompletion[i - 1] : 100;
    const dropRate = prevRate - rate;
    const dropPct = ((prevRate - rate) / prevRate) * 100;
    const usersCompleted = Math.round(TOTAL_USERS * (rate / 100));

    // Simulate avg_attempts (higher at cliff levels)
    let avgAttempts;
    if (dropRate > 8) avgAttempts = 4.2 + Math.random() * 2;
    else if (dropRate > 4) avgAttempts = 2.8 + Math.random() * 1.2;
    else avgAttempts = 1.2 + Math.random() * 1.5;

    // Simulate avg_best_score
    const avgScore = Math.max(40, 85 - levelNum * 0.6 + (Math.random() * 10 - 5));

    // Simulate avg_days_to_complete
    const avgDays = Math.round((0.5 + levelNum * 0.35 + (dropRate > 8 ? 3 : 0)) * 10) / 10;

    // Classify severity
    let severity = "normal";
    if (dropRate > 8) severity = "severe";
    else if (dropRate > 4) severity = "warning";

    // Difficulty index
    const difficultyIndex = Math.round(avgAttempts * (1 - rate / 100) * 100) / 100;

    return {
      level: levelNum,
      completionRate: Math.round(rate * 100) / 100,
      usersCompleted,
      totalUsers: TOTAL_USERS,
      dropRate: Math.round(dropRate * 100) / 100,
      dropRatePct: Math.round(dropPct * 100) / 100,
      avgAttempts: Math.round(avgAttempts * 10) / 10,
      avgBestScore: Math.round(avgScore * 10) / 10,
      avgDaysToComplete: avgDays,
      severity,
      difficultyIndex: Math.round(difficultyIndex * 100) / 100,
    };
  });
};

const LEVEL_DATA = generateLevelData();

// ══════════════════════════════════════════════════════════════
// CUSTOM TOOLTIP
// ══════════════════════════════════════════════════════════════
const FunnelTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  const severityColors = { severe: "#f87171", warning: "#fbbf24", normal: "#60a5fa" };
  const severityLabels = { severe: "SEVERE DROP", warning: "WARNING", normal: "Normal" };

  return (
    <div style={{
      background: "#1a1e2a", border: "1px solid #3a4260", borderRadius: 10,
      padding: "14px 18px", boxShadow: "0 8px 32px rgba(0,0,0,.5)", minWidth: 260,
      borderTop: `3px solid ${severityColors[d.severity]}`
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
        <span style={{ fontWeight: 700, fontSize: 15, color: "#e4e7f0" }}>Level {d.level}</span>
        <span style={{
          fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 4,
          background: `${severityColors[d.severity]}18`, color: severityColors[d.severity]
        }}>{severityLabels[d.severity]}</span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px 16px", fontSize: 12 }}>
        <div style={{ color: "#8b92a8" }}>Completion Rate</div>
        <div style={{ color: "#e4e7f0", fontWeight: 600, textAlign: "right" }}>{d.completionRate}%</div>
        <div style={{ color: "#8b92a8" }}>Users Completed</div>
        <div style={{ color: "#e4e7f0", fontWeight: 600, textAlign: "right" }}>{d.usersCompleted.toLocaleString()}</div>
        <div style={{ color: "#8b92a8" }}>Drop Rate</div>
        <div style={{ color: severityColors[d.severity], fontWeight: 600, textAlign: "right" }}>
          -{d.dropRate}% {d.severity !== "normal" && "⚠️"}
        </div>
        <div style={{ color: "#8b92a8" }}>Relative Drop</div>
        <div style={{ color: "#e4e7f0", textAlign: "right" }}>{d.dropRatePct}% of prev level</div>
        <div style={{ borderTop: "1px solid #272d40", gridColumn: "span 2", margin: "4px 0" }} />
        <div style={{ color: "#8b92a8" }}>Avg Attempts</div>
        <div style={{ color: d.avgAttempts > 4 ? "#fbbf24" : "#e4e7f0", fontWeight: 500, textAlign: "right" }}>
          {d.avgAttempts}x {d.avgAttempts > 4 && "🔥"}
        </div>
        <div style={{ color: "#8b92a8" }}>Avg Best Score</div>
        <div style={{ color: "#e4e7f0", textAlign: "right" }}>{d.avgBestScore}</div>
        <div style={{ color: "#8b92a8" }}>Days to Complete</div>
        <div style={{ color: "#e4e7f0", textAlign: "right" }}>{d.avgDaysToComplete}d</div>
        <div style={{ color: "#8b92a8" }}>Difficulty Index</div>
        <div style={{ color: d.difficultyIndex > 3 ? "#f87171" : "#e4e7f0", fontWeight: 500, textAlign: "right" }}>
          {d.difficultyIndex}
        </div>
      </div>
    </div>
  );
};

const DropTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  const severityColors = { severe: "#f87171", warning: "#fbbf24", normal: "#60a5fa" };
  return (
    <div style={{
      background: "#1a1e2a", border: "1px solid #3a4260", borderRadius: 10,
      padding: "12px 16px", boxShadow: "0 8px 32px rgba(0,0,0,.5)", minWidth: 220,
      borderTop: `3px solid ${severityColors[d.severity]}`
    }}>
      <div style={{ fontWeight: 700, fontSize: 14, color: "#e4e7f0", marginBottom: 8 }}>
        Level {d.level - 1} → {d.level}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 14px", fontSize: 12 }}>
        <div style={{ color: "#8b92a8" }}>Users Lost</div>
        <div style={{ color: severityColors[d.severity], fontWeight: 700, textAlign: "right" }}>
          -{d.dropRate}%
        </div>
        <div style={{ color: "#8b92a8" }}>≈ Players Lost</div>
        <div style={{ color: "#e4e7f0", textAlign: "right" }}>
          ~{Math.round(TOTAL_USERS * d.dropRate / 100).toLocaleString()}
        </div>
        <div style={{ color: "#8b92a8" }}>Avg Attempts</div>
        <div style={{ color: "#e4e7f0", textAlign: "right" }}>{d.avgAttempts}x</div>
      </div>
    </div>
  );
};

// ══════════════════════════════════════════════════════════════
// SEVERITY COLORS
// ══════════════════════════════════════════════════════════════
const getBarColor = (severity) => {
  switch (severity) {
    case "severe": return "#f87171";
    case "warning": return "#fbbf24";
    default: return "#60a5fa";
  }
};

const getBarOpacity = (severity) => {
  switch (severity) {
    case "severe": return 1;
    case "warning": return 0.9;
    default: return 0.65;
  }
};

// ══════════════════════════════════════════════════════════════
// CLIFF ANNOTATIONS
// ══════════════════════════════════════════════════════════════
const cliffZones = [
  { level: 14, label: "Cliff #1", desc: "6 colors introduced", color: "#f87171" },
  { level: 29, label: "Cliff #2", desc: "Board 12×12", color: "#f87171" },
  { level: 43, label: "Cliff #3", desc: "7 colors + reduced moves", color: "#f87171" },
];

// ══════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ══════════════════════════════════════════════════════════════
export default function LevelCompletionFunnel() {
  const [activeView, setActiveView] = useState("funnel");
  const [highlightSevere, setHighlightSevere] = useState(true);
  const [showAvgLine, setShowAvgLine] = useState(true);
  const [levelRange, setLevelRange] = useState([1, 50]);

  const filteredData = useMemo(() =>
    LEVEL_DATA.filter(d => d.level >= levelRange[0] && d.level <= levelRange[1]),
    [levelRange]
  );

  const avgDropRate = useMemo(() => {
    const rates = filteredData.map(d => d.dropRate);
    return Math.round((rates.reduce((a, b) => a + b, 0) / rates.length) * 100) / 100;
  }, [filteredData]);

  const severeCount = filteredData.filter(d => d.severity === "severe").length;
  const warningCount = filteredData.filter(d => d.severity === "warning").length;
  const worstLevel = filteredData.reduce((a, b) => a.dropRate > b.dropRate ? a : b, filteredData[0]);

  // Scorecards
  const scorecards = [
    { label: "Total Levels", value: filteredData.length, color: "#60a5fa" },
    { label: "Avg Drop Rate", value: `${avgDropRate}%`, color: "#fbbf24" },
    { label: "Severe Drops", value: severeCount, color: "#f87171", sub: "> 8% drop" },
    { label: "Warning Drops", value: warningCount, color: "#fbbf24", sub: "> 4% drop" },
    { label: "Worst Level", value: `Lv ${worstLevel?.level}`, color: "#f87171", sub: `-${worstLevel?.dropRate}%` },
    { label: "End Funnel", value: `${filteredData[filteredData.length - 1]?.completionRate}%`, color: "#34d399", sub: "users remaining" },
  ];

  return (
    <div style={{ background: "#0b0d13", minHeight: "100vh", padding: "28px 32px", fontFamily: "'Inter',-apple-system,sans-serif", color: "#e4e7f0" }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>
          <span style={{ color: "#f87171" }}>C1</span> — Level Completion Funnel
          <span style={{ color: "#8b92a8", fontSize: 13, fontWeight: 400, marginLeft: 12 }}>Flood-It Demo Visualization</span>
        </h1>
        <p style={{ color: "#8b92a8", fontSize: 13, marginTop: 4 }}>
          Source: <code style={{ background: "#7c6cf018", color: "#a78bfa", padding: "1px 6px", borderRadius: 3, fontSize: 11 }}>mart_level_funnel</code>
          {" "}• {TOTAL_USERS.toLocaleString()} total users • {LEVEL_DATA.length} levels
          {" "}• <span style={{ color: "#555c72" }}>Simulated data dựa trên pattern thực tế của casual puzzle game</span>
        </p>
      </div>

      {/* Scorecards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 12, marginBottom: 24 }}>
        {scorecards.map((s, i) => (
          <div key={i} style={{
            background: "#181c28", border: "1px solid #272d40", borderRadius: 10,
            padding: "14px 16px", textAlign: "center"
          }}>
            <div style={{ fontSize: 22, fontWeight: 700, color: s.color }}>{s.value}</div>
            <div style={{ fontSize: 11, color: "#8b92a8", marginTop: 2 }}>{s.label}</div>
            {s.sub && <div style={{ fontSize: 10, color: "#555c72", marginTop: 1 }}>{s.sub}</div>}
          </div>
        ))}
      </div>

      {/* Controls */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12, marginBottom: 20,
        padding: "12px 16px", background: "#181c28", border: "1px solid #272d40", borderRadius: 10
      }}>
        {/* View toggle */}
        <div style={{ display: "flex", gap: 4, background: "#12151e", borderRadius: 8, padding: 3 }}>
          {[
            { key: "funnel", label: "Completion Funnel" },
            { key: "drop", label: "Drop Rate Spikes" },
            { key: "overlay", label: "Overlay (Drop + Attempts)" },
          ].map(v => (
            <button key={v.key} onClick={() => setActiveView(v.key)} style={{
              padding: "6px 14px", borderRadius: 6, border: "none", fontSize: 12, fontWeight: 500,
              cursor: "pointer", transition: "all .15s",
              background: activeView === v.key ? "#7c6cf0" : "transparent",
              color: activeView === v.key ? "#fff" : "#8b92a8",
            }}>{v.label}</button>
          ))}
        </div>

        <div style={{ flex: 1 }} />

        {/* Toggles */}
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#8b92a8", cursor: "pointer" }}>
          <input type="checkbox" checked={highlightSevere} onChange={e => setHighlightSevere(e.target.checked)}
            style={{ accentColor: "#f87171" }} />
          Highlight drops
        </label>
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#8b92a8", cursor: "pointer" }}>
          <input type="checkbox" checked={showAvgLine} onChange={e => setShowAvgLine(e.target.checked)}
            style={{ accentColor: "#fbbf24" }} />
          Avg reference line
        </label>

        {/* Level range */}
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#8b92a8" }}>
          <span>Levels:</span>
          <select value={levelRange[0]} onChange={e => setLevelRange([+e.target.value, levelRange[1]])}
            style={{ background: "#12151e", border: "1px solid #272d40", color: "#e4e7f0", borderRadius: 4, padding: "3px 6px", fontSize: 11 }}>
            {[1, 10, 20, 30, 40].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
          <span>—</span>
          <select value={levelRange[1]} onChange={e => setLevelRange([levelRange[0], +e.target.value])}
            style={{ background: "#12151e", border: "1px solid #272d40", color: "#e4e7f0", borderRadius: 4, padding: "3px 6px", fontSize: 11 }}>
            {[10, 20, 30, 40, 50].map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
      </div>

      {/* Legend */}
      <div style={{ display: "flex", gap: 20, marginBottom: 16, paddingLeft: 4 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#8b92a8" }}>
          <div style={{ width: 12, height: 12, borderRadius: 3, background: "#f87171" }} />
          Severe drop (&gt;8%)
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#8b92a8" }}>
          <div style={{ width: 12, height: 12, borderRadius: 3, background: "#fbbf24" }} />
          Warning (&gt;4%)
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#8b92a8" }}>
          <div style={{ width: 12, height: 12, borderRadius: 3, background: "#60a5fa", opacity: 0.65 }} />
          Normal attrition
        </div>
        {showAvgLine && (
          <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#8b92a8" }}>
            <div style={{ width: 16, height: 2, background: "#fbbf24", borderRadius: 1 }} />
            Avg drop rate ({avgDropRate}%)
          </div>
        )}
      </div>

      {/* ══════════════ CHART AREA ══════════════ */}
      <div style={{
        background: "#12151e", border: "1px solid #272d40", borderRadius: 14,
        padding: "20px 16px 12px", marginBottom: 24
      }}>
        {/* VIEW: FUNNEL */}
        {activeView === "funnel" && (
          <div>
            <div style={{ fontSize: 13, fontWeight: 600, color: "#8b92a8", marginBottom: 4, paddingLeft: 8 }}>
              Completion Rate (%) — Mỗi bar = % user base đã vượt qua level
            </div>
            <ResponsiveContainer width="100%" height={380}>
              <BarChart data={filteredData} margin={{ top: 10, right: 20, left: 10, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e2233" vertical={false} />
                <XAxis
                  dataKey="level" tick={{ fill: "#555c72", fontSize: 11 }}
                  label={{ value: "Level Number", position: "insideBottom", offset: -10, fill: "#555c72", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <YAxis
                  tick={{ fill: "#555c72", fontSize: 11 }} domain={[0, 100]}
                  label={{ value: "Completion Rate (%)", angle: -90, position: "insideLeft", offset: 5, fill: "#555c72", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <Tooltip content={<FunnelTooltip />} cursor={{ fill: "rgba(124,108,240,0.06)" }} />

                {/* Cliff zone annotations */}
                {cliffZones.map((cz, i) =>
                  cz.level >= levelRange[0] && cz.level <= levelRange[1] && (
                    <ReferenceLine key={i} x={cz.level} stroke="#f8717140" strokeDasharray="4 4"
                      label={{ value: `${cz.label}: ${cz.desc}`, position: "top", fill: "#f87171", fontSize: 10, fontWeight: 600 }}
                    />
                  )
                )}

                <Bar dataKey="completionRate" radius={[3, 3, 0, 0]} maxBarSize={20}>
                  {filteredData.map((d, i) => (
                    <Cell
                      key={i}
                      fill={highlightSevere ? getBarColor(d.severity) : "#60a5fa"}
                      fillOpacity={highlightSevere ? getBarOpacity(d.severity) : 0.65}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* VIEW: DROP RATE */}
        {activeView === "drop" && (
          <div>
            <div style={{ fontSize: 13, fontWeight: 600, color: "#8b92a8", marginBottom: 4, paddingLeft: 8 }}>
              Drop Rate (%) — Mỗi bar = lượng user bị mất giữa level N-1 → N. <span style={{ color: "#f87171" }}>Spike = Problem level</span>
            </div>
            <ResponsiveContainer width="100%" height={380}>
              <BarChart data={filteredData} margin={{ top: 10, right: 20, left: 10, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e2233" vertical={false} />
                <XAxis
                  dataKey="level" tick={{ fill: "#555c72", fontSize: 11 }}
                  label={{ value: "Level Number", position: "insideBottom", offset: -10, fill: "#555c72", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <YAxis
                  tick={{ fill: "#555c72", fontSize: 11 }}
                  label={{ value: "Drop Rate (%)", angle: -90, position: "insideLeft", offset: 5, fill: "#555c72", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <Tooltip content={<DropTooltip />} cursor={{ fill: "rgba(124,108,240,0.06)" }} />

                {/* Average reference line */}
                {showAvgLine && (
                  <ReferenceLine y={avgDropRate} stroke="#fbbf24" strokeDasharray="6 4" strokeWidth={1.5}
                    label={{ value: `AVG: ${avgDropRate}%`, position: "right", fill: "#fbbf24", fontSize: 10, fontWeight: 600 }}
                  />
                )}
                {showAvgLine && (
                  <ReferenceLine y={avgDropRate * 2} stroke="#f8717180" strokeDasharray="4 4" strokeWidth={1}
                    label={{ value: `2×AVG: ${(avgDropRate * 2).toFixed(1)}%`, position: "right", fill: "#f87171", fontSize: 10 }}
                  />
                )}

                {/* Cliff annotations */}
                {cliffZones.map((cz, i) =>
                  cz.level >= levelRange[0] && cz.level <= levelRange[1] && (
                    <ReferenceLine key={i} x={cz.level} stroke="#f8717140" strokeDasharray="4 4"
                      label={{ value: cz.label, position: "top", fill: "#f87171", fontSize: 10, fontWeight: 600 }}
                    />
                  )
                )}

                <Bar dataKey="dropRate" radius={[3, 3, 0, 0]} maxBarSize={20}>
                  {filteredData.map((d, i) => (
                    <Cell
                      key={i}
                      fill={highlightSevere ? getBarColor(d.severity) : "#60a5fa"}
                      fillOpacity={highlightSevere ? getBarOpacity(d.severity) : 0.7}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* VIEW: OVERLAY */}
        {activeView === "overlay" && (
          <div>
            <div style={{ fontSize: 13, fontWeight: 600, color: "#8b92a8", marginBottom: 4, paddingLeft: 8 }}>
              Overlay: <span style={{ color: "#f87171" }}>Drop Rate (bar)</span> + <span style={{ color: "#fbbf24" }}>Avg Attempts (line)</span> — Correlation check
            </div>
            <ResponsiveContainer width="100%" height={380}>
              <ComposedChart data={filteredData} margin={{ top: 10, right: 50, left: 10, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e2233" vertical={false} />
                <XAxis
                  dataKey="level" tick={{ fill: "#555c72", fontSize: 11 }}
                  label={{ value: "Level Number", position: "insideBottom", offset: -10, fill: "#555c72", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <YAxis
                  yAxisId="left" tick={{ fill: "#555c72", fontSize: 11 }}
                  label={{ value: "Drop Rate (%)", angle: -90, position: "insideLeft", offset: 5, fill: "#f87171", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <YAxis
                  yAxisId="right" orientation="right" tick={{ fill: "#555c72", fontSize: 11 }}
                  label={{ value: "Avg Attempts", angle: 90, position: "insideRight", offset: 10, fill: "#fbbf24", fontSize: 11 }}
                  tickLine={false} axisLine={{ stroke: "#272d40" }}
                />
                <Tooltip content={<FunnelTooltip />} cursor={{ fill: "rgba(124,108,240,0.06)" }} />

                <Bar yAxisId="left" dataKey="dropRate" radius={[3, 3, 0, 0]} maxBarSize={18}>
                  {filteredData.map((d, i) => (
                    <Cell key={i} fill={getBarColor(d.severity)} fillOpacity={getBarOpacity(d.severity)} />
                  ))}
                </Bar>
                <Line
                  yAxisId="right" type="monotone" dataKey="avgAttempts"
                  stroke="#fbbf24" strokeWidth={2.5} dot={false}
                  strokeDasharray="" activeDot={{ r: 5, fill: "#fbbf24", stroke: "#0b0d13", strokeWidth: 2 }}
                />
                <Line
                  yAxisId="left" type="monotone" dataKey="completionRate"
                  stroke="#a78bfa" strokeWidth={1.5} strokeDasharray="6 3" dot={false}
                  opacity={0.5}
                />
              </ComposedChart>
            </ResponsiveContainer>
            <div style={{ display: "flex", gap: 20, justifyContent: "center", marginTop: 8 }}>
              <span style={{ fontSize: 11, color: "#f87171" }}>■ Drop Rate (bar, left axis)</span>
              <span style={{ fontSize: 11, color: "#fbbf24" }}>━ Avg Attempts (line, right axis)</span>
              <span style={{ fontSize: 11, color: "#a78bfa", opacity: 0.6 }}>┅ Completion Rate (dashed, ref)</span>
            </div>
          </div>
        )}
      </div>

      {/* ══════════════ CLIFF ANALYSIS CARDS ══════════════ */}
      <div style={{ marginBottom: 24 }}>
        <h3 style={{ fontSize: 15, fontWeight: 700, color: "#f87171", marginBottom: 14 }}>
          Cliff Zones Detected — Root Cause Hypothesis
        </h3>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 14 }}>
          {cliffZones.map((cz, i) => {
            const d = LEVEL_DATA.find(l => l.level === cz.level);
            return (
              <div key={i} style={{
                background: "#181c28", border: "1px solid #272d40", borderRadius: 12,
                padding: "18px 20px", borderTop: "3px solid #f87171"
              }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                  <span style={{ fontWeight: 700, fontSize: 16, color: "#f87171" }}>{cz.label}</span>
                  <span style={{ fontSize: 11, color: "#555c72" }}>Level {cz.level}</span>
                </div>
                <div style={{ fontSize: 12, color: "#fbbf24", marginBottom: 10, fontWeight: 500 }}>{cz.desc}</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 12px", fontSize: 12, marginBottom: 12 }}>
                  <div style={{ color: "#8b92a8" }}>Drop Rate:</div>
                  <div style={{ color: "#f87171", fontWeight: 700 }}>-{d?.dropRate}%</div>
                  <div style={{ color: "#8b92a8" }}>Players Lost:</div>
                  <div style={{ color: "#e4e7f0" }}>~{Math.round(TOTAL_USERS * (d?.dropRate || 0) / 100).toLocaleString()}</div>
                  <div style={{ color: "#8b92a8" }}>Avg Attempts:</div>
                  <div style={{ color: d?.avgAttempts > 4 ? "#fbbf24" : "#e4e7f0" }}>{d?.avgAttempts}x</div>
                  <div style={{ color: "#8b92a8" }}>Completion:</div>
                  <div style={{ color: "#e4e7f0" }}>{d?.completionRate}%</div>
                </div>
                <div style={{
                  background: "#f8717112", border: "1px solid #f8717130", borderRadius: 8,
                  padding: "10px 12px", fontSize: 11, color: "#f8a4a4", lineHeight: 1.6
                }}>
                  <strong>Root Cause:</strong> {i === 0
                    ? "Difficulty Spike — New color introduced quá đột ngột. User chưa adapt kịp mechanic mới → frustration → quit"
                    : i === 1
                    ? "Board Size Jump — 10×10 → 12×12 tăng complexity exponentially. Moves không tăng tương ứng → mathematically harder"
                    : "Compound Difficulty — 7 colors + fewer moves = double nerf. Chỉ hardcore players survive → massive casual drop"
                  }
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* ══════════════ PROBLEM LEVELS TABLE (C6 preview) ══════════════ */}
      <div style={{
        background: "#12151e", border: "1px solid #272d40", borderRadius: 14,
        padding: 20, marginBottom: 24
      }}>
        <h3 style={{ fontSize: 15, fontWeight: 700, color: "#e4e7f0", marginBottom: 14 }}>
          Top Problem Levels — Sort by Difficulty Index <span style={{ fontSize: 12, color: "#555c72", fontWeight: 400 }}>(exportable cho GD team)</span>
        </h3>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #272d40" }}>
                {["#", "Level", "Completion %", "Drop Rate", "Avg Attempts", "Avg Score", "Days", "Difficulty Idx", "Severity", "Suggested Action"].map(h => (
                  <th key={h} style={{ padding: "8px 10px", textAlign: "left", color: "#555c72", fontSize: 10, textTransform: "uppercase", letterSpacing: ".5px", fontWeight: 600, whiteSpace: "nowrap" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...LEVEL_DATA]
                .filter(d => d.severity !== "normal")
                .sort((a, b) => b.difficultyIndex - a.difficultyIndex)
                .slice(0, 10)
                .map((d, i) => {
                  const sevColor = d.severity === "severe" ? "#f87171" : "#fbbf24";
                  const action = d.severity === "severe"
                    ? (d.avgAttempts > 4 ? "Nerf difficulty" : "Redesign UX")
                    : "Monitor / Tune";
                  return (
                    <tr key={i} style={{ borderBottom: "1px solid #1e2233" }}>
                      <td style={{ padding: "8px 10px", color: "#555c72" }}>{i + 1}</td>
                      <td style={{ padding: "8px 10px", fontWeight: 700, color: sevColor }}>Level {d.level}</td>
                      <td style={{ padding: "8px 10px", color: "#8b92a8" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <div style={{ width: 60, height: 6, background: "#272d40", borderRadius: 3, overflow: "hidden" }}>
                            <div style={{ width: `${d.completionRate}%`, height: "100%", background: "#60a5fa", borderRadius: 3 }} />
                          </div>
                          {d.completionRate}%
                        </div>
                      </td>
                      <td style={{ padding: "8px 10px", color: sevColor, fontWeight: 600 }}>-{d.dropRate}%</td>
                      <td style={{ padding: "8px 10px", color: d.avgAttempts > 4 ? "#fbbf24" : "#8b92a8" }}>{d.avgAttempts}x</td>
                      <td style={{ padding: "8px 10px", color: "#8b92a8" }}>{d.avgBestScore}</td>
                      <td style={{ padding: "8px 10px", color: "#8b92a8" }}>{d.avgDaysToComplete}d</td>
                      <td style={{ padding: "8px 10px" }}>
                        <span style={{
                          background: `${sevColor}18`, color: sevColor, padding: "2px 8px",
                          borderRadius: 4, fontWeight: 600, fontSize: 11
                        }}>{d.difficultyIndex}</span>
                      </td>
                      <td style={{ padding: "8px 10px" }}>
                        <span style={{
                          fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 4,
                          background: d.severity === "severe" ? "#f8717118" : "#fbbf2418",
                          color: sevColor, textTransform: "uppercase"
                        }}>{d.severity}</span>
                      </td>
                      <td style={{ padding: "8px 10px", fontSize: 11, color: "#8b92a8" }}>{action}</td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Footer */}
      <div style={{ textAlign: "center", color: "#555c72", fontSize: 11, marginTop: 32, paddingTop: 16, borderTop: "1px solid #272d40" }}>
        Flood-It — C1 Level Completion Funnel Demo • Simulated data based on casual puzzle game patterns • mart_level_funnel
      </div>
    </div>
  );
}
