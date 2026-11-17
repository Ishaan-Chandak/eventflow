import { useCallback, useEffect, useState } from 'react'
import { api } from '../api/client'
import { StatusBadge } from '../components/StatusBadge'
import type { DeliveryPage, DeliveryStatus } from '../types'

const statuses: DeliveryStatus[] = ['QUEUED', 'IN_PROGRESS', 'RETRY_SCHEDULED', 'SUCCEEDED', 'DEAD']
const labels: Record<DeliveryStatus, string> = { QUEUED: 'Queued', IN_PROGRESS: 'In progress', RETRY_SCHEDULED: 'Retry scheduled', SUCCEEDED: 'Succeeded', DEAD: 'Dead' }
const emptySummary = Object.fromEntries(statuses.map(status => [status, 0])) as Record<DeliveryStatus, number>

export function Dashboard({ openDelivery }: { openDelivery: (id: string) => void }) {
  const [data, setData] = useState<DeliveryPage | null>(null)
  const [status, setStatus] = useState<DeliveryStatus | ''>('')
  const [page, setPage] = useState(0)
  const [error, setError] = useState('')
  const [receiver, setReceiver] = useState<number | 'timeout' | null>(null)
  const [controlError, setControlError] = useState('')

  const load = useCallback(async () => {
    try { setData(await api.deliveries(status, page)); setError('') }
    catch (e) { setError((e as Error).message) }
  }, [status, page])

  useEffect(() => {
    void load()
    const timer = window.setInterval(() => void load(), 4000)
    return () => window.clearInterval(timer)
  }, [load])
  useEffect(() => { void api.receiverStatus().then(x => setReceiver(x.status)).catch(() => {}) }, [])

  const changeReceiver = async (next: number | 'timeout') => {
    try { await api.setReceiverStatus(next); setReceiver(next); setControlError('') }
    catch (e) { setControlError((e as Error).message) }
  }

  return <div className="page-stack">
    <section className="page-heading"><div><p className="eyebrow">OPERATIONS OVERVIEW</p><h1>Deliveries</h1><p>Follow every webhook from queue to receiver.</p></div><span className="live-indicator"><span /> Refreshes every 4s</span></section>
    <div className="status-grid">{statuses.map(item => <button key={item} className={`status-card ${status === item ? 'selected' : ''}`} onClick={() => { setStatus(status === item ? '' : item); setPage(0) }}><span>{labels[item]}</span><strong>{(data?.summary || emptySummary)[item]}</strong></button>)}</div>
    <section className="panel">
      <div className="panel-heading"><div><h2>Recent deliveries</h2><p>{status ? `${labels[status]} jobs` : 'All delivery jobs'}, newest first</p></div><button className="button subtle" onClick={() => void load()}>Refresh</button></div>
      <div className="filter-row"><label htmlFor="status-filter">Status</label><select id="status-filter" value={status} onChange={e => { setStatus(e.target.value as DeliveryStatus | ''); setPage(0) }}><option value="">All statuses</option>{statuses.map(s => <option key={s} value={s}>{labels[s]}</option>)}</select><span>{data?.total ?? 0} total</span></div>
      {error && <p className="message error" role="alert">{error}</p>}
      {!data && !error && <p className="empty">Loading deliveries…</p>}
      {data && data.items.length === 0 && <p className="empty">No deliveries in this view yet. Publish an event to begin.</p>}
      {data && data.items.length > 0 && <div className="table-scroll"><table><thead><tr><th>Event</th><th>Endpoint</th><th>Status</th><th>Attempts</th><th>Created</th><th></th></tr></thead><tbody>{data.items.map(job => <tr key={job.id}><td><strong>{job.eventType}</strong><small>{job.id.slice(0, 8)}</small></td><td>{job.endpointName}</td><td><StatusBadge status={job.status} /></td><td>{job.attemptCount}</td><td>{new Date(job.createdAt).toLocaleString()}</td><td><button className="text-button" onClick={() => openDelivery(job.id)}>View →</button></td></tr>)}</tbody></table></div>}
      <div className="pagination"><button className="button subtle" disabled={page === 0} onClick={() => setPage(page - 1)}>Previous</button><span>Page {page + 1}</span><button className="button subtle" disabled={!data || (page + 1) * data.size >= data.total} onClick={() => setPage(page + 1)}>Next</button></div>
    </section>
    <section className="panel demo-panel"><div><p className="eyebrow">LOCAL DEMO</p><h2>Mock receiver</h2><p>Change the next response to watch retries and dead jobs.</p></div><div className="demo-actions">{([200, 500, 400, 'timeout'] as const).map(code => <button key={code} className={`button ${receiver === code ? 'active' : 'subtle'}`} onClick={() => void changeReceiver(code)}>{code === 'timeout' ? 'Timeout' : `HTTP ${code}`}</button>)}</div>{controlError && <p className="message error" role="alert">{controlError}</p>}</section>
  </div>
}
