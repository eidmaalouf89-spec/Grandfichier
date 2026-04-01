export default function Sidebar({ runs, activeRunId, onSelectRun, onNewRun }) {
  return (
    <div style={{
      width: 220,
      minWidth: 220,
      background: 'var(--color-bg-secondary)',
      borderRight: '0.5px solid var(--color-border)',
      display: 'flex',
      flexDirection: 'column',
      height: '100vh',
    }}>
      {/* Header */}
      <div style={{
        padding: '16px',
        borderBottom: '0.5px solid var(--color-border)',
      }}>
        <div style={{ fontSize: 13, fontWeight: 500 }}>JANSA GF Updater</div>
        <div style={{ fontSize: 11, color: 'var(--color-text-secondary)', marginTop: 2 }}>
          Historique des runs
        </div>
      </div>

      {/* Runs list */}
      <div style={{ flex: 1, overflowY: 'auto', padding: 8 }}>
        {runs.length === 0 && (
          <div style={{ padding: '12px 10px', fontSize: 12, color: 'var(--color-text-muted)' }}>
            Aucun run pour cette session.
          </div>
        )}
        {runs.map(run => (
          <div
            key={run.run_id}
            onClick={() => onSelectRun(run.run_id)}
            style={{
              padding: '8px 10px',
              borderRadius: 'var(--radius-md)',
              cursor: 'pointer',
              marginBottom: 4,
              border: run.run_id === activeRunId
                ? '0.5px solid var(--color-border-strong)'
                : '0.5px solid transparent',
              background: run.run_id === activeRunId ? 'var(--color-bg)' : 'transparent',
            }}
          >
            <div style={{ fontSize: 11, color: 'var(--color-text-secondary)' }}>
              {formatTimestamp(run.timestamp)}
            </div>
            <div style={{
              fontSize: 11,
              fontWeight: 500,
              marginTop: 2,
              color: !run.done
                ? 'var(--color-blue)'
                : run.success
                  ? 'var(--color-green)'
                  : 'var(--color-red)',
            }}>
              {!run.done
                ? '⟳ En cours...'
                : run.success
                  ? `✓ Succès — ${run.mode}`
                  : '✗ Erreur'}
            </div>
          </div>
        ))}
      </div>

      {/* New run button */}
      <div
        onClick={onNewRun}
        style={{
          margin: 12,
          padding: '8px 12px',
          background: 'var(--color-bg)',
          border: '0.5px solid var(--color-border-strong)',
          borderRadius: 'var(--radius-md)',
          fontSize: 13,
          fontWeight: 500,
          cursor: 'pointer',
          textAlign: 'center',
          userSelect: 'none',
        }}
      >
        + Nouveau run
      </div>
    </div>
  )
}

function formatTimestamp(ts) {
  if (!ts || ts.length < 15) return ts
  const d = ts.slice(0, 8)
  const t = ts.slice(9, 15)
  return `${d.slice(6)}/${d.slice(4,6)}/${d.slice(0,4)} — ${t.slice(0,2)}:${t.slice(2,4)}`
}
