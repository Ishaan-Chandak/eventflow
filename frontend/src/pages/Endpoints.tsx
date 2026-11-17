import { useCallback, useEffect, useState } from 'react'
import { api } from '../api/client'
import type { Endpoint } from '../types'

const blank = { name: '', url: 'http://mock-receiver:8082/webhook', eventTypes: 'order.created', enabled: true }

export function Endpoints() {
  const [items, setItems] = useState<Endpoint[]>([])
  const [form, setForm] = useState(blank)
  const [editing, setEditing] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const load = useCallback(async () => {
    try { setItems(await api.endpoints()); setError('') }
    catch (e) { setError((e as Error).message) }
    finally { setLoading(false) }
  }, [])
  useEffect(() => { void load() }, [load])

  const save = async (event: React.FormEvent) => {
    event.preventDefault(); setSaving(true); setError(''); setNotice('')
    const body = { name: form.name.trim(), url: form.url.trim(), eventTypes: form.eventTypes.split(',').map(x => x.trim()).filter(Boolean), enabled: form.enabled }
    if (!body.name || !body.url || body.eventTypes.length === 0) { setError('Name, URL and at least one event type are required.'); setSaving(false); return }
    try {
      if (editing) await api.updateEndpoint(editing, body)
      else await api.createEndpoint(body)
      setNotice(editing ? 'Endpoint updated.' : 'Endpoint created.')
      setEditing(null); setForm(blank); await load()
    } catch (e) { setError((e as Error).message) }
    finally { setSaving(false) }
  }
  const edit = (item: Endpoint) => { setEditing(item.id); setForm({ name: item.name, url: item.url, eventTypes: item.eventTypes.join(', '), enabled: item.enabled }); setNotice(''); window.scrollTo({ top: 0, behavior: 'smooth' }) }
  const toggle = async (item: Endpoint) => { try { await api.updateEndpoint(item.id, { enabled: !item.enabled }); await load() } catch (e) { setError((e as Error).message) } }

  return <div className="page-stack"><section className="page-heading"><div><p className="eyebrow">DESTINATIONS</p><h1>Endpoints</h1><p>Subscribe local receivers to the event types they handle.</p></div></section>
    <section className="panel form-panel"><div className="panel-heading"><div><h2>{editing ? 'Edit endpoint' : 'Register endpoint'}</h2><p>Only allowlisted local hosts can be used in this demo.</p></div>{editing && <button className="button subtle" onClick={() => { setEditing(null); setForm(blank) }}>Cancel edit</button>}</div>
      <form onSubmit={e => void save(e)} className="form-grid"><label>Name<input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} placeholder="Order service" required /></label><label>URL<input value={form.url} onChange={e => setForm({ ...form, url: e.target.value })} required /></label><label className="wide">Event types <span className="hint">Comma separated</span><input value={form.eventTypes} onChange={e => setForm({ ...form, eventTypes: e.target.value })} placeholder="order.created, order.updated" required /></label><label className="checkbox-row"><input type="checkbox" checked={form.enabled} onChange={e => setForm({ ...form, enabled: e.target.checked })} /> Enabled</label><div className="form-actions"><button className="button primary" disabled={saving}>{saving ? 'Saving…' : editing ? 'Save changes' : 'Create endpoint'}</button></div></form>
      {error && <p className="message error" role="alert">{error}</p>}{notice && <p className="message success" role="status">{notice}</p>}
    </section>
    <section className="panel"><div className="panel-heading"><div><h2>Registered endpoints</h2><p>{items.length} shown</p></div><button className="button subtle" onClick={() => void load()}>Refresh</button></div>{loading ? <p className="empty">Loading endpoints…</p> : items.length === 0 ? <p className="empty">No endpoints yet. Register one above.</p> : <div className="endpoint-list">{items.map(item => <article className="endpoint-item" key={item.id}><div><div className="endpoint-title"><h3>{item.name}</h3><span className={`badge ${item.enabled ? 'badge-succeeded' : 'badge-dead'}`}>{item.enabled ? 'Enabled' : 'Disabled'}</span></div><p className="mono">{item.url}</p><div className="chips">{item.eventTypes.map(type => <span key={type} className="chip">{type}</span>)}</div></div><div className="row-actions"><button className="button subtle" onClick={() => edit(item)}>Edit</button><button className="button subtle" onClick={() => void toggle(item)}>{item.enabled ? 'Disable' : 'Enable'}</button></div></article>)}</div>}</section>
  </div>
}
