import { useState } from 'react'
import { api } from '../api/client'

export function Publish({ openDelivery }: { openDelivery: (id: string) => void }) {
  const [eventType, setEventType] = useState('order.created')
  const [payload, setPayload] = useState('{\n  "orderId": "o-123",\n  "total": 42.5\n}')
  const [key, setKey] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState<{ eventId: string; jobIds: string[] } | null>(null)

  const publish = async (event: React.FormEvent) => {
    event.preventDefault(); setError(''); setResult(null)
    let parsed: unknown
    try { parsed = JSON.parse(payload) }
    catch { setError('Payload must be valid JSON.'); return }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) { setError('Payload must be a JSON object.'); return }
    setSaving(true)
    try { setResult(await api.publish(eventType.trim(), parsed, key.trim())) }
    catch (e) { setError((e as Error).message) }
    finally { setSaving(false) }
  }

  return <div className="page-stack"><section className="page-heading"><div><p className="eyebrow">SEND AN EVENT</p><h1>Publish</h1><p>One job is created for every enabled endpoint subscribed to this event type.</p></div></section>
    <section className="panel form-panel"><form className="publish-form" onSubmit={e => void publish(e)}><label>Event type<input value={eventType} onChange={e => setEventType(e.target.value)} placeholder="order.created" required /></label><label>JSON payload<textarea className="code-input" rows={12} value={payload} onChange={e => setPayload(e.target.value)} spellCheck={false} required /></label><label>Idempotency key <span className="hint">Optional</span><input value={key} onChange={e => setKey(e.target.value)} placeholder="order-o-123-created" /></label><div className="form-actions"><button className="button primary" disabled={saving}>{saving ? 'Publishing…' : 'Publish event'}</button></div></form>{error && <p className="message error" role="alert">{error}</p>}</section>
    {result && <section className="panel result-panel" role="status"><div className="result-icon">✓</div><div><h2>Event accepted</h2><p className="mono">{result.eventId}</p><p>{result.jobIds.length === 0 ? 'No enabled endpoints matched this event type.' : `${result.jobIds.length} delivery job${result.jobIds.length === 1 ? '' : 's'} created.`}</p><div className="result-links">{result.jobIds.map(id => <button key={id} className="text-button" onClick={() => openDelivery(id)}>View delivery {id.slice(0, 8)} →</button>)}</div></div></section>}
  </div>
}
