import { useState, useEffect } from 'react'

export default function ResultsPanel({ runId }) {
  const [status, setStatus] = useState(null)

  useEffect(() => {
    fetch(`/api/run/${runId}/status`)
      .then(r => r.json())
      .then(setStatus)
      .catch(() => {})
  }, [runId])

  const handleDownload = async (type) => {
    window.location.href = `/api/run/${runId}/download/${type}`
  }

  if (!status) {
    return <div style={{ fontSize: 13, color: 'var(--color-text-secondary)' }}>Chargement...</div>
  }

  if (!status.success) {
    return (
      <div style={{ maxWidth: 560 }}>
        <div style={{
          padding: '14px 16px',
          background: 'var(--color-red-light)',
          border: '0.5px solid #D85A30',
          borderRadius: 'var(--radius-md)',
          fontSize: 13,
          color: 'var(--color-red)',
          marginBottom: 16,
        }}>
          Le pipeline s'est terminé avec des erreurs. Le GrandFichier a quand même été produit — téléchargez-le ci-dessous. Le ZIP de debug contient la trace complète.
        </div>
        <DownloadRow
          type="grandfichier"
          iconLabel="GF"
          iconColor={{ bg: 'var(--color-green-light)', color: 'var(--color-green)' }}
          name="updated_grandfichier.xlsx"
          desc="GrandFichier mis à jour — produit malgré l'erreur"
          onDownload={handleDownload}
        />
        <DownloadRow
          type="debug_zip"
          iconLabel="ZIP"
          iconColor={{ bg: 'var(--color-blue-light)', color: 'var(--color-blue)' }}
          name={`Pour_EID_RUN_${status.output_dir?.replace('output/run_', '') || ''}.zip`}
          desc="Tous les inputs + outputs — pour débogage MOEX"
          onDownload={handleDownload}
        />
      </div>
    )
  }

  return (
    <div style={{ maxWidth: 640 }}>

      {/* Métriques */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
        gap: 10,
        marginBottom: 24,
      }}>
        <MetricCard
          label="Statut"
          value="Succès"
          valueColor="var(--color-green)"
          sub={status.mode}
        />
        <MetricCard
          label="Logs générés"
          value={status.log_lines}
          sub="lignes de log"
        />
        <MetricCard
          label="Mode"
          value={status.has_bet ? 'GED + BET' : 'GED only'}
          sub="passe exécutée"
        />
      </div>

      <div style={{ fontSize: 13, fontWeight: 500, marginBottom: 10 }}>
        Fichiers à télécharger
      </div>

      <DownloadRow
        type="grandfichier"
        iconLabel="GF"
        iconColor={{ bg: 'var(--color-green-light)', color: 'var(--color-green)' }}
        name="updated_grandfichier.xlsx"
        desc="GrandFichier mis à jour — à remettre en circulation"
        onDownload={handleDownload}
      />

      <DownloadRow
        type="debug_zip"
        iconLabel="ZIP"
        iconColor={{ bg: 'var(--color-blue-light)', color: 'var(--color-blue)' }}
        name={`Pour_EID_RUN_${status.output_dir?.replace('output/run_', '') || ''}.zip`}
        desc="Tous les inputs + outputs — pour débogage MOEX"
        onDownload={handleDownload}
      />
    </div>
  )
}

function MetricCard({ label, value, valueColor, sub }) {
  return (
    <div style={{
      background: 'var(--color-bg-secondary)',
      borderRadius: 'var(--radius-md)',
      padding: '12px 14px',
    }}>
      <div style={{ fontSize: 11, color: 'var(--color-text-secondary)', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 500, color: valueColor || 'var(--color-text)' }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: 'var(--color-text-secondary)', marginTop: 2 }}>{sub}</div>}
    </div>
  )
}

function DownloadRow({ type, iconLabel, iconColor, name, desc, onDownload }) {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: 10,
      padding: '10px 14px',
      border: '0.5px solid var(--color-border)',
      borderRadius: 'var(--radius-md)',
      marginBottom: 8,
      background: 'var(--color-bg)',
    }}>
      <div style={{
        width: 32, height: 32,
        borderRadius: 6,
        background: iconColor.bg,
        color: iconColor.color,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 11, fontWeight: 500,
        flexShrink: 0,
      }}>
        {iconLabel}
      </div>
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 13, fontWeight: 500 }}>{name}</div>
        <div style={{ fontSize: 11, color: 'var(--color-text-secondary)', marginTop: 1 }}>{desc}</div>
      </div>
      <button
        onClick={() => onDownload(type)}
        style={{
          padding: '6px 14px',
          background: 'var(--color-bg-secondary)',
          border: '0.5px solid var(--color-border-strong)',
          borderRadius: 'var(--radius-md)',
          fontSize: 12,
          fontWeight: 500,
          cursor: 'pointer',
          color: 'var(--color-text)',
          whiteSpace: 'nowrap',
        }}
      >
        ↓ Télécharger
      </button>
    </div>
  )
}
