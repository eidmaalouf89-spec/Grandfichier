import { useState } from 'react'

const BET_ZONES = [
  { key: 'bet_lesommer', label: 'AMO HQE — Le Sommer', hint: 'PDFs — dossier ou sélection multiple' },
  { key: 'bet_avls',     label: 'Acousticien — AVLS',   hint: 'PDFs — dossier ou sélection multiple' },
  { key: 'bet_terrell',  label: 'BET Structure — Terrell', hint: 'PDFs — fiches FExx' },
  { key: 'bet_socotec',  label: 'Bureau de contrôle — SOCOTEC', hint: 'PDFs — rapports BC' },
]

export default function UploadPanel({ onRunStarted }) {
  const [gedFile, setGedFile] = useState(null)
  const [gfFile, setGfFile] = useState(null)
  const [includeBet, setIncludeBet] = useState(false)
  const [betFiles, setBetFiles] = useState({ bet_lesommer: [], bet_avls: [], bet_terrell: [], bet_socotec: [] })
  const [launching, setLaunching] = useState(false)
  const [error, setError] = useState(null)

  const canLaunch = gedFile && gfFile && !launching

  const handleBetFiles = (key, files) => {
    setBetFiles(prev => ({ ...prev, [key]: Array.from(files) }))
  }

  const handleLaunch = async () => {
    if (!canLaunch) return
    setLaunching(true)
    setError(null)
    try {
      const formData = new FormData()
      formData.append('ged', gedFile)
      formData.append('gf', gfFile)
      if (includeBet) {
        for (const [key, files] of Object.entries(betFiles)) {
          for (const f of files) {
            formData.append(key, f)
          }
        }
      }
      const res = await fetch('/api/run', { method: 'POST', body: formData })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      onRunStarted(data.run_id)
    } catch (e) {
      setError(`Erreur lors du lancement : ${e.message}`)
      setLaunching(false)
    }
  }

  return (
    <div style={{ maxWidth: 640 }}>

      {/* Bannière d'avertissement */}
      <div style={{
        display: 'flex',
        alignItems: 'flex-start',
        gap: 10,
        padding: '12px 14px',
        borderRadius: 'var(--radius-md)',
        background: 'var(--color-amber-light)',
        border: `0.5px solid var(--color-amber-border)`,
        marginBottom: 24,
      }}>
        <span style={{ fontSize: 14, color: 'var(--color-amber)', flexShrink: 0, marginTop: 1 }}>⚠</span>
        <p style={{ fontSize: 12, color: 'var(--color-amber-dark)', lineHeight: 1.6 }}>
          <strong style={{ fontWeight: 500, color: '#412402' }}>Avant de lancer le pipeline —</strong>{' '}
          assurez-vous que votre tableau de suivi est mis à jour pour le SAS et les derniers indices afin d'éviter les oublis.
        </p>
      </div>

      {/* Section Fichiers source */}
      <div style={{ marginBottom: 6, fontSize: 13, fontWeight: 500 }}>Fichiers source</div>
      <div style={{ marginBottom: 16, fontSize: 12, color: 'var(--color-text-secondary)' }}>
        Sélectionnez le dump GED et le GrandFichier courant.
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 24 }}>
        <UploadZone
          label="Export GED AxeoBIM"
          hint=".xlsx — dump complet"
          accept=".xlsx"
          file={gedFile}
          onChange={setGedFile}
        />
        <UploadZone
          label="GrandFichier courant"
          hint=".xlsx — tableau de suivi"
          accept=".xlsx"
          file={gfFile}
          onChange={setGfFile}
        />
      </div>

      {/* Toggle BET */}
      <div
        onClick={() => setIncludeBet(v => !v)}
        style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: includeBet ? 14 : 0, cursor: 'pointer', userSelect: 'none' }}
      >
        <div style={{
          width: 32, height: 18, borderRadius: 9,
          background: includeBet ? 'var(--color-green-border)' : 'var(--color-border-strong)',
          position: 'relative', flexShrink: 0, transition: 'background 0.15s',
        }}>
          <div style={{
            width: 14, height: 14, borderRadius: '50%', background: 'white',
            position: 'absolute', top: 2,
            right: includeBet ? 2 : undefined,
            left: includeBet ? undefined : 2,
            transition: 'left 0.15s, right 0.15s',
          }} />
        </div>
        <span style={{ fontSize: 13, fontWeight: 500 }}>
          Inclure la passe BET (rapports PDF)
        </span>
      </div>

      {/* Zones BET */}
      {includeBet && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 8 }}>
          {BET_ZONES.map(zone => (
            <BetZone
              key={zone.key}
              label={zone.label}
              hint={zone.hint}
              files={betFiles[zone.key]}
              onChange={files => handleBetFiles(zone.key, files)}
            />
          ))}
        </div>
      )}

      {/* Error */}
      {error && (
        <div style={{
          marginTop: 12,
          padding: '10px 14px',
          background: 'var(--color-red-light)',
          border: '0.5px solid #D85A30',
          borderRadius: 'var(--radius-md)',
          fontSize: 12,
          color: 'var(--color-red)',
        }}>
          {error}
        </div>
      )}

      {/* Launch */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'flex-end',
        gap: 12, marginTop: 24, paddingTop: 16,
        borderTop: '0.5px solid var(--color-border)',
      }}>
        <span style={{ fontSize: 12, color: 'var(--color-text-secondary)' }}>
          2 fichiers requis, passe BET optionnelle
        </span>
        <button
          onClick={handleLaunch}
          disabled={!canLaunch}
          style={{
            padding: '8px 20px',
            background: canLaunch ? 'var(--color-blue)' : 'var(--color-bg-secondary)',
            color: canLaunch ? 'white' : 'var(--color-text-secondary)',
            border: canLaunch ? 'none' : '0.5px solid var(--color-border)',
            borderRadius: 'var(--radius-md)',
            fontSize: 13,
            fontWeight: 500,
            cursor: canLaunch ? 'pointer' : 'default',
            transition: 'background 0.15s',
          }}
        >
          {launching ? 'Lancement...' : 'Lancer le pipeline →'}
        </button>
      </div>
    </div>
  )
}

function UploadZone({ label, hint, accept, file, onChange }) {
  const filled = !!file
  return (
    <label style={{
      border: `0.5px ${filled ? 'solid' : 'dashed'} ${filled ? 'var(--color-green-border)' : 'var(--color-border-strong)'}`,
      borderRadius: 'var(--radius-md)',
      padding: 16,
      textAlign: 'center',
      cursor: 'pointer',
      background: filled ? 'var(--color-green-light)' : 'var(--color-bg-secondary)',
      display: 'block',
      transition: 'border-color 0.15s, background 0.15s',
    }}>
      <input
        type="file"
        accept={accept}
        style={{ display: 'none' }}
        onChange={e => e.target.files[0] && onChange(e.target.files[0])}
      />
      <div style={{ fontSize: 18, marginBottom: 6, color: filled ? 'var(--color-green)' : 'var(--color-text-secondary)' }}>
        {filled ? '✓' : '↑'}
      </div>
      <div style={{ fontSize: 12, fontWeight: 500 }}>{label}</div>
      <div style={{ fontSize: 11, color: 'var(--color-text-secondary)', marginTop: 2 }}>{hint}</div>
      {filled && (
        <div style={{ fontSize: 11, color: 'var(--color-green)', marginTop: 4, fontWeight: 500 }}>
          {file.name}
        </div>
      )}
    </label>
  )
}

function BetZone({ label, hint, files, onChange }) {
  const filled = files.length > 0
  return (
    <label style={{
      border: `0.5px ${filled ? 'solid' : 'dashed'} ${filled ? 'var(--color-green-border)' : 'var(--color-border-strong)'}`,
      borderRadius: 'var(--radius-md)',
      padding: 12,
      cursor: 'pointer',
      background: filled ? 'var(--color-green-light)' : 'var(--color-bg-secondary)',
      display: 'block',
      transition: 'border-color 0.15s, background 0.15s',
    }}>
      <input
        type="file"
        accept=".pdf"
        multiple
        style={{ display: 'none' }}
        onChange={e => onChange(e.target.files)}
      />
      <div style={{ fontSize: 12, fontWeight: 500 }}>{label}</div>
      <div style={{ fontSize: 11, color: filled ? 'var(--color-green)' : 'var(--color-text-secondary)', marginTop: 2 }}>
        {filled ? `${files.length} PDF${files.length > 1 ? 's' : ''} sélectionné${files.length > 1 ? 's' : ''}` : hint}
      </div>
    </label>
  )
}
