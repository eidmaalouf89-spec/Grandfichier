import { useEffect, useState, useRef } from 'react'

const STEPS = [
  { id: 1, name: 'Step 1 — Lecture GED',          desc: 'Ingestion du dump AxeoBIM',               keyword: 'ged_ingest' },
  { id: 2, name: 'Step 2 — Lecture GrandFichier',  desc: 'Chargement des feuilles LOT',             keyword: 'grandfichier_reader' },
  { id: 3, name: 'Step 3 — Matching GF-master',    desc: 'Appariement NUMERO + INDICE',             keyword: 'matcher' },
  { id: 4, name: 'Step 4 — Consolidation',         desc: 'Merge engine + résolution conflits',      keyword: 'merge_engine' },
  { id: 5, name: 'Step 5 — Écriture GF',           desc: 'Mise à jour cellules DATE/N°/STATUT',    keyword: 'grandfichier_writer' },
  { id: 6, name: 'Step 6 — Parsers BET',           desc: 'Le Sommer · AVLS · Terrell · SOCOTEC',   keyword: 'bet_ingest' },
  { id: 7, name: 'Step 7 — Feuilles RAPPORT_*',    desc: 'Écriture historique BET dans le GF',     keyword: 'bet_gf_writer' },
  { id: 8, name: 'Step 8 — BET Backfill',          desc: 'Report avis BET → colonnes LOT',         keyword: 'bet_backfill' },
]

// Détecte la step courante depuis un log line
function detectStep(line) {
  const l = line.toLowerCase()
  if (l.includes('bet_backfill') || l.includes('step 8') || l.includes('backfill')) return 8
  if (l.includes('bet_gf_writer') || l.includes('step 7') || l.includes('rapport_')) return 7
  if (l.includes('bet_ingest') || l.includes('step 6') || l.includes('lesommer') || l.includes('avls') || l.includes('terrell') || l.includes('socotec')) return 6
  if (l.includes('grandfichier_writer') || l.includes('step 5') || l.includes('écriture') || l.includes('writing')) return 5
  if (l.includes('merge_engine') || l.includes('step 4') || l.includes('consolidation') || l.includes('merge')) return 4
  if (l.includes('matcher') || l.includes('step 3') || l.includes('matching')) return 3
  if (l.includes('grandfichier_reader') || l.includes('step 2') || l.includes('feuille') || l.includes('sheet')) return 2
  if (l.includes('ged_ingest') || l.includes('step 1') || l.includes('axeobim') || l.includes('dump')) return 1
  return null
}

export default function ProgressPanel({ runId, onComplete }) {
  const [currentStep, setCurrentStep] = useState(0)
  const [stepTimings, setStepTimings] = useState({})
  const [done, setDone] = useState(false)
  const [success, setSuccess] = useState(false)
  const [startTime] = useState(Date.now())
  const [elapsed, setElapsed] = useState(0)
  const esRef = useRef(null)

  useEffect(() => {
    const timer = setInterval(() => setElapsed(Math.round((Date.now() - startTime) / 1000)), 1000)
    return () => clearInterval(timer)
  }, [startTime])

  useEffect(() => {
    const es = new EventSource(`/api/stream/${runId}`)
    esRef.current = es

    es.onmessage = (event) => {
      const data = JSON.parse(event.data)

      if (data.line) {
        const step = detectStep(data.line)
        if (step) {
          setCurrentStep(prev => {
            if (step > prev) {
              setStepTimings(t => ({ ...t, [step]: { start: Date.now() } }))
              return step
            }
            return prev
          })
        }
      }

      if (data.done) {
        setDone(true)
        setSuccess(data.success)
        if (data.success) setCurrentStep(8)
        es.close()
        onComplete()
      }
    }

    es.onerror = () => {
      es.close()
    }

    return () => es.close()
  }, [runId])

  const progress = done ? 100 : Math.round((currentStep / 8) * 100)

  return (
    <div style={{ maxWidth: 560 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 }}>
        <div style={{ fontSize: 15, fontWeight: 500 }}>
          {done ? (success ? 'Pipeline terminé avec succès' : 'Pipeline terminé avec erreurs') : 'Exécution en cours'}
        </div>
        <div style={{ fontSize: 12, color: 'var(--color-text-secondary)' }}>
          {elapsed}s
        </div>
      </div>

      {/* Barre de progression */}
      <div style={{
        height: 4,
        background: 'var(--color-bg-secondary)',
        borderRadius: 2,
        overflow: 'hidden',
        marginBottom: 24,
      }}>
        <div style={{
          height: '100%',
          width: `${progress}%`,
          background: done && !success ? '#D85A30' : 'var(--color-blue)',
          borderRadius: 2,
          transition: 'width 0.4s ease',
        }} />
      </div>

      {/* Steps */}
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {STEPS.map((step, i) => {
          const isDone = currentStep > step.id || (done && success)
          const isActive = !done && currentStep === step.id
          const isPending = !isDone && !isActive
          const isError = done && !success && currentStep === step.id

          return (
            <div
              key={step.id}
              style={{
                display: 'flex',
                alignItems: 'flex-start',
                gap: 12,
                padding: '10px 0',
                position: 'relative',
              }}
            >
              {/* Connector line */}
              {i < STEPS.length - 1 && (
                <div style={{
                  position: 'absolute',
                  left: 11,
                  top: 34,
                  bottom: 0,
                  width: 1,
                  background: 'var(--color-border)',
                }} />
              )}

              {/* Circle */}
              <div style={{
                width: 24, height: 24, borderRadius: '50%',
                flexShrink: 0,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 11, fontWeight: 500,
                position: 'relative', zIndex: 1,
                background: isError ? 'var(--color-red-light)'
                  : isDone ? 'var(--color-green-light)'
                  : isActive ? 'var(--color-blue-light)'
                  : 'var(--color-bg-secondary)',
                border: isError ? '1.5px solid #D85A30'
                  : isDone ? `1.5px solid var(--color-green-border)`
                  : isActive ? `1.5px solid var(--color-blue)`
                  : '0.5px solid var(--color-border)',
                color: isError ? 'var(--color-red)'
                  : isDone ? 'var(--color-green)'
                  : isActive ? 'var(--color-blue)'
                  : 'var(--color-text-secondary)',
              }}>
                {isDone ? '✓' : step.id}
              </div>

              {/* Info */}
              <div style={{ flex: 1 }}>
                <div style={{
                  fontSize: 13, fontWeight: 500,
                  color: isPending ? 'var(--color-text-secondary)' : 'var(--color-text)',
                }}>
                  {step.name}
                </div>
                <div style={{ fontSize: 12, color: 'var(--color-text-secondary)', marginTop: 2 }}>
                  {step.desc}
                </div>
                {isActive && (
                  <div style={{ fontSize: 11, color: 'var(--color-blue)', marginTop: 2 }}>
                    En cours...
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
