import { useState, useEffect } from 'react'
import Sidebar from './components/Sidebar'
import UploadPanel from './components/UploadPanel'
import ProgressPanel from './components/ProgressPanel'
import ResultsPanel from './components/ResultsPanel'

export default function App() {
  const [runs, setRuns] = useState([])        // liste depuis GET /api/runs
  const [activeRunId, setActiveRunId] = useState(null)
  const [activePanel, setActivePanel] = useState('upload')  // 'upload' | 'progress' | 'results'

  // Charger la liste des runs au démarrage et après chaque nouveau run
  const refreshRuns = async () => {
    try {
      const res = await fetch('/api/runs')
      const data = await res.json()
      setRuns(data.runs || [])
    } catch (e) {
      // Silently ignore if API not started yet
    }
  }

  useEffect(() => {
    refreshRuns()
  }, [])

  // Sélectionner un run depuis la sidebar
  const handleSelectRun = (runId) => {
    setActiveRunId(runId)
    const run = runs.find(r => r.run_id === runId)
    if (!run) return
    if (run.done) {
      setActivePanel('results')
    } else {
      setActivePanel('progress')
    }
  }

  // Lancement d'un nouveau run
  const handleRunStarted = (runId) => {
    setActiveRunId(runId)
    setActivePanel('progress')
    refreshRuns()
  }

  // Run terminé → refresh liste + passer aux résultats
  const handleRunComplete = () => {
    refreshRuns()
    setActivePanel('results')
  }

  const activeRun = runs.find(r => r.run_id === activeRunId) || null

  return (
    <div style={{
      display: 'flex',
      height: '100vh',
      background: 'var(--color-bg)',
    }}>
      <Sidebar
        runs={runs}
        activeRunId={activeRunId}
        onSelectRun={handleSelectRun}
        onNewRun={() => { setActiveRunId(null); setActivePanel('upload') }}
      />

      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        {/* Topbar */}
        <div style={{
          padding: '14px 24px',
          borderBottom: '0.5px solid var(--color-border)',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          background: 'var(--color-bg)',
        }}>
          <span style={{ fontSize: 15, fontWeight: 500 }}>
            {activeRun
              ? `Run du ${formatTimestamp(activeRun.timestamp)}`
              : 'Nouveau run'}
          </span>
          {activeRun && (
            <span style={{
              fontSize: 11,
              padding: '3px 8px',
              borderRadius: 'var(--radius-sm)',
              background: 'var(--color-green-light)',
              color: 'var(--color-green)',
              fontWeight: 500,
            }}>
              {activeRun.mode}
            </span>
          )}
        </div>

        {/* Steps nav */}
        <div style={{
          display: 'flex',
          borderBottom: '0.5px solid var(--color-border)',
          padding: '0 24px',
          background: 'var(--color-bg)',
        }}>
          {['upload', 'progress', 'results'].map((panel, i) => {
            const labels = ['Fichiers', 'Exécution', 'Résultats']
            const isActive = activePanel === panel
            const isDone = activeRun && (
              (panel === 'upload') ||
              (panel === 'progress' && activeRun.done) ||
              (panel === 'results' && activeRun.done && activeRun.success)
            )
            return (
              <div
                key={panel}
                onClick={() => activeRun || panel === 'upload' ? setActivePanel(panel) : null}
                style={{
                  padding: '10px 16px',
                  fontSize: 12,
                  fontWeight: isActive ? 500 : 400,
                  color: isDone && !isActive ? 'var(--color-green)' : isActive ? 'var(--color-text)' : 'var(--color-text-secondary)',
                  borderBottom: isActive ? '2px solid var(--color-blue)' : '2px solid transparent',
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  userSelect: 'none',
                }}
              >
                <span style={{
                  width: 6, height: 6, borderRadius: '50%',
                  background: isDone && !isActive ? 'var(--color-green)' : 'currentColor',
                  flexShrink: 0,
                }} />
                {labels[i]}
              </div>
            )
          })}
        </div>

        {/* Panel content */}
        <div style={{ flex: 1, overflowY: 'auto', padding: 24 }}>
          {activePanel === 'upload' && (
            <UploadPanel onRunStarted={handleRunStarted} />
          )}
          {activePanel === 'progress' && activeRunId && (
            <ProgressPanel
              runId={activeRunId}
              onComplete={handleRunComplete}
            />
          )}
          {activePanel === 'results' && activeRunId && (
            <ResultsPanel runId={activeRunId} />
          )}
        </div>
      </div>
    </div>
  )
}

function formatTimestamp(ts) {
  // ts format: "20260401_091422"
  if (!ts || ts.length < 15) return ts
  const d = ts.slice(0, 8)
  const t = ts.slice(9, 15)
  return `${d.slice(6)}/${d.slice(4,6)}/${d.slice(0,4)} — ${t.slice(0,2)}:${t.slice(2,4)}`
}
