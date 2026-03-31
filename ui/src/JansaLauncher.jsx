import { useState, useRef, useEffect } from "react";

const STEPS = [
  {
    num: 1,
    title: "Export GED AxeoBIM",
    desc: "Sélectionnez le fichier d'export GED (.xlsx) depuis AxeoBIM.",
    field: "ged",
    accept: ".xlsx",
    icon: "📊",
    hint: "Fichier de type : 17CO_Tranche_2_du_XX_mars_2026.xlsx",
  },
  {
    num: 2,
    title: "Tableau de Suivi (GrandFichier)",
    desc: "Sélectionnez le GrandFichier à mettre à jour (.xlsx).",
    field: "gf",
    accept: ".xlsx",
    icon: "📋",
    hint: "Fichier de type : P17-T2-VISA-Tableau_de_suivi.xlsx",
  },
  {
    num: 3,
    title: "Rapports PDF",
    desc: "Sélectionnez les rapports des approbateurs (optionnel).",
    field: "reports",
    accept: ".pdf",
    multiple: true,
    optional: true,
    icon: "📄",
    hint: "Rapports de visa ARCHI, BET, SOCOTEC, etc.",
    disabled: true,
    disabledMsg: "Disponible dans une prochaine version",
  },
  {
    num: 4,
    title: "Extrait SAS",
    desc: "Sélectionnez le fichier SAS conformité (optionnel).",
    field: "sas",
    accept: ".xlsx,.csv",
    optional: true,
    icon: "🔒",
    hint: "Fichier de conformité SAS (.xlsx ou .csv)",
    disabled: true,
    disabledMsg: "Disponible dans une prochaine version",
  },
];

const OUTPUT_FILES = [
  {
    name: "updated_grandfichier.xlsx",
    icon: "📋",
    desc: "GrandFichier mis à jour — Tous les statuts, dates et observations des approbateurs sont renseignés depuis le GED. Colonnes VISA GLOBAL et DATE CONTRACTUELLE calculées automatiquement. Mise en forme avec code couleur par statut.",
  },
  {
    name: "evidence_export.csv",
    icon: "🔍",
    desc: "Traçabilité complète — Chaque modification effectuée est enregistrée : ancienne valeur, nouvelle valeur, source, raison. Permet l'audit de toutes les écritures du pipeline.",
  },
  {
    name: "match_summary.csv",
    icon: "📈",
    desc: "Résumé de correspondance — Nombre de lignes GF mises en correspondance avec le GED, non trouvées, ou ignorées (onglets OLD). Indicateur de qualité du matching.",
  },
  {
    name: "anomaly_log.json",
    icon: "⚠️",
    desc: "Journal des anomalies — Documents sans colonne GF, missions non mappées, conflits de statut. Tout ce qui n'a pas pu être résolu automatiquement.",
  },
  {
    name: "orphan_ged_documents.xlsx",
    icon: "📎",
    desc: "Documents orphelins GED — Documents présents dans le GED mais absents du GrandFichier, dont le MOEX est encore en attente. À traiter manuellement ou à ajouter au GF.",
  },
  {
    name: "orphan_summary.xlsx",
    icon: "📝",
    desc: "Résumé des orphelins — Liste condensée : NUMERO, EMETTEUR, DATE DE RECEPTION. Vue rapide pour le suivi MOEX des documents non encore indexés.",
  },
];

// Parse the "RUN COMPLETE" summary block into key stats
function parseStats(logs) {
  const stats = {};
  for (const line of logs) {
    const m = line.match(/^\s{2}(.+?):\s+([0-9,\s]+)$/);
    if (m) stats[m[1].trim()] = m[2].trim();
  }
  return stats;
}

export default function JansaLauncher() {
  const [files, setFiles] = useState({});
  const [activeStep, setActiveStep] = useState(0);
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);
  const [error, setError] = useState(null);
  const [log, setLog] = useState([]);
  const [outputDir, setOutputDir] = useState("");
  const logRef = useRef(null);
  const esRef = useRef(null);

  // Auto-scroll log to bottom
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [log]);

  const handleFile = (field, e) => {
    const f = e.target.files;
    if (!f || f.length === 0) return;
    setFiles((prev) => ({
      ...prev,
      [field]: field === "reports" ? Array.from(f) : f[0],
    }));
  };

  const clearFile = (field) => {
    setFiles((prev) => {
      const next = { ...prev };
      delete next[field];
      return next;
    });
  };

  const canRun = !!(files.ged && files.gf);

  const handleRun = async () => {
    setRunning(true);
    setDone(false);
    setError(null);
    setLog([]);
    setOutputDir("");

    try {
      // 1. Upload files and start the pipeline
      const form = new FormData();
      form.append("ged", files.ged);
      form.append("gf", files.gf);

      const res = await fetch("/api/run", { method: "POST", body: form });
      if (!res.ok) {
        const err = await res.text();
        throw new Error(`Serveur : ${res.status} — ${err}`);
      }
      const { run_id } = await res.json();

      // 2. Open SSE stream for real-time logs
      const es = new EventSource(`/api/stream/${run_id}`);
      esRef.current = es;

      es.onmessage = (event) => {
        const data = JSON.parse(event.data);

        if (data.line) {
          setLog((prev) => [...prev, data.line]);
        }

        if (data.done) {
          es.close();
          setRunning(false);
          if (data.success) {
            setOutputDir(data.output_dir);
            setDone(true);
          } else {
            setError("Le pipeline s'est terminé avec des erreurs. Consultez les logs ci-dessus.");
          }
        }
      };

      es.onerror = () => {
        es.close();
        setRunning(false);
        setError("Connexion au serveur perdue. Vérifiez que api_server.py est lancé sur le port 8000.");
      };
    } catch (err) {
      setRunning(false);
      setError(err.message);
    }
  };

  const reset = () => {
    if (esRef.current) esRef.current.close();
    setFiles({});
    setActiveStep(0);
    setRunning(false);
    setDone(false);
    setError(null);
    setLog([]);
    setOutputDir("");
  };

  const stats = done ? parseStats(log) : {};

  return (
    <div style={{
      minHeight: "100vh",
      background: "linear-gradient(135deg, #0c1220 0%, #1a2744 50%, #0f1b2d 100%)",
      fontFamily: "'Segoe UI', system-ui, -apple-system, sans-serif",
      color: "#e2e8f0",
      padding: "0",
    }}>
      {/* Header */}
      <div style={{
        background: "rgba(15, 23, 42, 0.8)",
        borderBottom: "1px solid rgba(148, 163, 184, 0.1)",
        padding: "20px 32px",
        display: "flex",
        alignItems: "center",
        gap: "16px",
        backdropFilter: "blur(12px)",
      }}>
        <div style={{
          width: 44, height: 44,
          background: "linear-gradient(135deg, #f59e0b, #d97706)",
          borderRadius: "10px",
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: "22px", fontWeight: 800, color: "#1a2744",
          letterSpacing: "-1px",
          boxShadow: "0 2px 12px rgba(245, 158, 11, 0.3)",
        }}>J</div>
        <div>
          <div style={{ fontSize: "18px", fontWeight: 700, letterSpacing: "-0.3px", color: "#f8fafc" }}>
            JANSA GED MAJ
          </div>
          <div style={{ fontSize: "12px", color: "#94a3b8", letterSpacing: "0.5px" }}>
            P17&CO Tranche 2 — Mise à jour du GrandFichier
          </div>
        </div>
        <div style={{ marginLeft: "auto", fontSize: "11px", color: "#64748b", background: "rgba(100,116,139,0.15)", padding: "4px 10px", borderRadius: "6px" }}>
          v1.0.0
        </div>
      </div>

      <div style={{ maxWidth: 920, margin: "0 auto", padding: "32px 24px" }}>

        {/* Stepper */}
        <div style={{ display: "flex", gap: "12px", marginBottom: "32px" }}>
          {STEPS.map((s, i) => {
            const isActive = activeStep === i;
            const hasFile = !!files[s.field];
            const isDisabled = !!s.disabled;
            return (
              <button
                key={s.field}
                onClick={() => !isDisabled && setActiveStep(i)}
                style={{
                  flex: 1,
                  background: isActive
                    ? "rgba(245, 158, 11, 0.12)"
                    : hasFile ? "rgba(169, 208, 142, 0.1)"
                    : isDisabled ? "rgba(100,116,139,0.05)"
                    : "rgba(30, 41, 59, 0.5)",
                  border: isActive
                    ? "1px solid rgba(245, 158, 11, 0.4)"
                    : hasFile ? "1px solid rgba(169, 208, 142, 0.3)"
                    : "1px solid rgba(148, 163, 184, 0.1)",
                  borderRadius: "10px",
                  padding: "14px 12px",
                  cursor: isDisabled ? "default" : "pointer",
                  textAlign: "center",
                  transition: "all 0.2s",
                  opacity: isDisabled ? 0.45 : 1,
                }}
              >
                <div style={{ fontSize: "20px", marginBottom: "4px" }}>{s.icon}</div>
                <div style={{
                  fontSize: "10px", fontWeight: 700,
                  color: isActive ? "#f59e0b" : hasFile ? "#a9d08e" : "#94a3b8",
                  textTransform: "uppercase", letterSpacing: "0.8px",
                }}>
                  Étape {s.num}
                </div>
                <div style={{ fontSize: "12px", color: isActive ? "#f8fafc" : "#cbd5e1", fontWeight: 500, marginTop: "2px" }}>
                  {s.title.length > 20 ? s.title.slice(0, 18) + "…" : s.title}
                </div>
                {hasFile && <div style={{ fontSize: "10px", color: "#a9d08e", marginTop: "4px" }}>✓ Sélectionné</div>}
                {isDisabled && <div style={{ fontSize: "9px", color: "#64748b", marginTop: "4px", fontStyle: "italic" }}>Bientôt</div>}
              </button>
            );
          })}
        </div>

        {/* Active step detail */}
        {!done && (
          <div style={{
            background: "rgba(30, 41, 59, 0.6)",
            border: "1px solid rgba(148, 163, 184, 0.1)",
            borderRadius: "14px",
            padding: "28px",
            marginBottom: "24px",
            backdropFilter: "blur(8px)",
          }}>
            {(() => {
              const s = STEPS[activeStep];
              const hasFile = files[s.field];
              return (
                <div>
                  <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "16px" }}>
                    <span style={{ fontSize: "28px" }}>{s.icon}</span>
                    <div>
                      <div style={{ fontSize: "16px", fontWeight: 700, color: "#f8fafc" }}>
                        Étape {s.num} — {s.title}
                      </div>
                      <div style={{ fontSize: "13px", color: "#94a3b8", marginTop: "2px" }}>{s.desc}</div>
                    </div>
                    {s.optional && (
                      <span style={{
                        marginLeft: "auto", fontSize: "10px", color: "#64748b",
                        background: "rgba(100,116,139,0.15)", padding: "3px 8px",
                        borderRadius: "4px", fontWeight: 600,
                      }}>OPTIONNEL</span>
                    )}
                  </div>

                  {s.disabled ? (
                    <div style={{
                      padding: "24px", textAlign: "center", color: "#64748b", fontSize: "13px",
                      border: "1px dashed rgba(100,116,139,0.2)", borderRadius: "10px",
                    }}>
                      🔒 {s.disabledMsg}
                    </div>
                  ) : hasFile ? (
                    <div style={{
                      display: "flex", alignItems: "center", gap: "12px", padding: "16px",
                      background: "rgba(169, 208, 142, 0.08)",
                      border: "1px solid rgba(169, 208, 142, 0.2)", borderRadius: "10px",
                    }}>
                      <div style={{ fontSize: "24px" }}>✅</div>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontSize: "13px", fontWeight: 600, color: "#a9d08e" }}>
                          {Array.isArray(hasFile) ? `${hasFile.length} fichier(s) sélectionné(s)` : hasFile.name}
                        </div>
                        {!Array.isArray(hasFile) && (
                          <div style={{ fontSize: "11px", color: "#64748b", marginTop: "2px" }}>
                            {(hasFile.size / 1024 / 1024).toFixed(1)} Mo
                          </div>
                        )}
                      </div>
                      <button
                        onClick={() => clearFile(s.field)}
                        style={{
                          background: "rgba(239, 68, 68, 0.1)", border: "1px solid rgba(239, 68, 68, 0.2)",
                          borderRadius: "6px", padding: "6px 12px", color: "#f87171",
                          fontSize: "11px", cursor: "pointer", fontWeight: 600,
                        }}
                      >
                        Retirer
                      </button>
                    </div>
                  ) : (
                    <label style={{
                      display: "flex", flexDirection: "column", alignItems: "center", gap: "8px",
                      padding: "32px", border: "2px dashed rgba(245, 158, 11, 0.25)",
                      borderRadius: "10px", cursor: "pointer", transition: "all 0.2s",
                      background: "rgba(245, 158, 11, 0.03)",
                    }}>
                      <div style={{ fontSize: "32px", opacity: 0.6 }}>📂</div>
                      <div style={{ fontSize: "13px", color: "#f59e0b", fontWeight: 600 }}>
                        Cliquer pour sélectionner
                      </div>
                      <div style={{ fontSize: "11px", color: "#64748b" }}>{s.hint}</div>
                      <input
                        type="file" accept={s.accept} multiple={!!s.multiple}
                        onChange={(e) => handleFile(s.field, e)}
                        style={{ display: "none" }}
                      />
                    </label>
                  )}
                </div>
              );
            })()}
          </div>
        )}

        {/* Error banner */}
        {error && (
          <div style={{
            background: "rgba(239, 68, 68, 0.08)",
            border: "1px solid rgba(239, 68, 68, 0.25)",
            borderRadius: "10px",
            padding: "16px 20px",
            marginBottom: "20px",
            fontSize: "13px",
            color: "#fca5a5",
          }}>
            ❌ {error}
          </div>
        )}

        {/* Run button */}
        {!done && !running && (
          <button
            onClick={handleRun}
            disabled={!canRun}
            style={{
              width: "100%", padding: "16px", fontSize: "15px", fontWeight: 700,
              color: canRun ? "#1a2744" : "#64748b",
              background: canRun ? "linear-gradient(135deg, #f59e0b, #d97706)" : "rgba(100,116,139,0.1)",
              border: canRun ? "none" : "1px solid rgba(100,116,139,0.15)",
              borderRadius: "10px", cursor: canRun ? "pointer" : "default",
              letterSpacing: "0.3px", transition: "all 0.2s",
              boxShadow: canRun ? "0 4px 20px rgba(245, 158, 11, 0.3)" : "none",
              marginBottom: "24px",
            }}
          >
            {canRun ? "▶  Lancer la mise à jour" : "Sélectionnez le GED et le GrandFichier pour continuer"}
          </button>
        )}

        {/* Live log console */}
        {log.length > 0 && (
          <div
            ref={logRef}
            style={{
              background: "rgba(15, 23, 42, 0.9)",
              border: "1px solid rgba(148, 163, 184, 0.1)",
              borderRadius: "10px",
              padding: "20px",
              marginBottom: "24px",
              fontFamily: "'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace",
              fontSize: "12px",
              maxHeight: "320px",
              overflowY: "auto",
            }}
          >
            {log.map((l, i) => {
              const isSuccess = l.includes("RUN COMPLETE") || l.startsWith("✓");
              const isStep = l.includes("[INFO]") && l.includes("—");
              const isWarning = l.includes("[WARNING]") || l.includes("WARN");
              const isError = l.includes("[ERROR]") || l.includes("ERROR");
              const isSep = l.startsWith("===");
              const isStat = /^\s{2}[A-Za-z]/.test(l) && l.includes(":");
              return (
                <div key={i} style={{
                  padding: "2px 0",
                  color: isError ? "#f87171"
                    : isSuccess ? "#a9d08e"
                    : isWarning ? "#fbbf24"
                    : isSep ? "#475569"
                    : isStat ? "#94a3b8"
                    : isStep ? "#f59e0b"
                    : "#e2e8f0",
                  fontWeight: isSuccess || isSep ? 700 : 400,
                  fontSize: isStat ? "11px" : "12px",
                  animation: "fadeIn 0.2s ease",
                }}>
                  {l}
                </div>
              );
            })}
            {running && (
              <div style={{ color: "#f59e0b", marginTop: "8px", animation: "pulse 1s infinite" }}>
                ⏳ Traitement en cours...
              </div>
            )}
          </div>
        )}

        {/* Results */}
        {done && (
          <div>
            <div style={{
              background: "rgba(169, 208, 142, 0.08)",
              border: "1px solid rgba(169, 208, 142, 0.2)",
              borderRadius: "12px",
              padding: "20px 24px",
              marginBottom: "24px",
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "12px" }}>
                <div style={{ fontSize: "28px" }}>✅</div>
                <div>
                  <div style={{ fontSize: "16px", fontWeight: 700, color: "#a9d08e" }}>
                    Mise à jour terminée avec succès
                  </div>
                  <div style={{ fontSize: "12px", color: "#64748b", marginTop: "2px", fontFamily: "monospace" }}>
                    {outputDir}/
                  </div>
                </div>
              </div>
              {/* Key stats */}
              {Object.keys(stats).length > 0 && (
                <div style={{ display: "flex", flexWrap: "wrap", gap: "10px", marginTop: "8px" }}>
                  {[
                    ["GF rows matched", stats["GF rows matched to GED"]],
                    ["Fields updated", stats["Fields updated"]],
                    ["Orphan GED docs", stats["Orphan GED documents"]],
                    ["Anomalies", stats["Total anomalies"]],
                  ].filter(([, v]) => v).map(([label, value]) => (
                    <div key={label} style={{
                      background: "rgba(15,23,42,0.5)",
                      border: "1px solid rgba(148,163,184,0.1)",
                      borderRadius: "8px",
                      padding: "8px 14px",
                      textAlign: "center",
                    }}>
                      <div style={{ fontSize: "16px", fontWeight: 700, color: "#f8fafc" }}>{value}</div>
                      <div style={{ fontSize: "10px", color: "#64748b", marginTop: "2px" }}>{label}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div style={{ fontSize: "14px", fontWeight: 700, color: "#f8fafc", marginBottom: "14px" }}>
              📦 Fichiers générés
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "10px", marginBottom: "28px" }}>
              {OUTPUT_FILES.map((f) => (
                <div key={f.name} style={{
                  background: "rgba(30, 41, 59, 0.6)",
                  border: "1px solid rgba(148, 163, 184, 0.1)",
                  borderRadius: "10px",
                  padding: "16px",
                  display: "flex",
                  gap: "12px",
                  alignItems: "flex-start",
                }}>
                  <div style={{ fontSize: "20px", marginTop: "2px" }}>{f.icon}</div>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: "13px", fontWeight: 700, color: "#f59e0b", fontFamily: "monospace" }}>
                      {outputDir ? `${outputDir}/` : ""}{f.name}
                    </div>
                    <div style={{ fontSize: "12px", color: "#94a3b8", marginTop: "4px", lineHeight: "1.5" }}>
                      {f.desc}
                    </div>
                  </div>
                </div>
              ))}
            </div>

            <button
              onClick={reset}
              style={{
                width: "100%", padding: "14px", fontSize: "14px", fontWeight: 600,
                color: "#f8fafc", background: "rgba(100,116,139,0.15)",
                border: "1px solid rgba(148, 163, 184, 0.15)",
                borderRadius: "10px", cursor: "pointer",
              }}
            >
              ↩ Nouvelle mise à jour
            </button>
          </div>
        )}

        {/* Footer */}
        <div style={{
          marginTop: "40px", paddingTop: "20px",
          borderTop: "1px solid rgba(148, 163, 184, 0.08)",
          display: "flex", justifyContent: "space-between",
          fontSize: "11px", color: "#475569",
        }}>
          <span>JANSA VISASIST — MOEX P17&CO Tranche 2</span>
          <span>GEMO</span>
        </div>
      </div>

      <style>{`
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(3px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: rgba(148,163,184,0.2); border-radius: 3px; }
      `}</style>
    </div>
  );
}
