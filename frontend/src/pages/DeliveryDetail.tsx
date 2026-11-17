import { useCallback, useEffect, useState } from 'react'
import { api } from '../api/client'
import { StatusBadge } from '../components/StatusBadge'
import type { DeliveryDetail as Detail } from '../types'

const showDate = (value: string | null) => value ? new Date(value).toLocaleString() : '—'

export function DeliveryDetail({ id, back }: { id: string; back: () => void }) {
  const [job, setJob] = useState<Detail | null>(null)
  const [error, setError] = useState('')
  const [retrying, setRetrying] = useState(false)
  const [notice, setNotice] = useState('')
  const load = useCallback(async () => { try { setJob(await api.delivery(id)); setError('') } catch (e) { setError((e as Error).message) } }, [id])
  useEffect(() => { void load(); const timer = window.setInterval(() => void load(), 4000); return () => window.clearInterval(timer) }, [load])
  const retry = async () => { setRetrying(true); setError(''); try { await api.retry(id); setNotice('Retry queued.'); await load() } catch (e) { setError((e as Error).message) } finally { setRetrying(false) } }

  return <div className="page-stack"><button className="back-link" onClick={back}>← Back to deliveries</button><section className="page-heading"><div><p className="eyebrow">DELIVERY DETAIL</p><h1>{job?.eventType || 'Delivery'}</h1><p className="mono">{id}</p></div>{job && <StatusBadge status={job.status} />}</section>
    {error && <p className="message error" role="alert">{error}</p>}{!job && !error && <p className="empty">Loading delivery…</p>}
    {job && <><section className="panel"><div className="panel-heading"><div><h2>Summary</h2><p>Current state and destination</p></div>{job.status === 'DEAD' && <button className="button primary" disabled={retrying} onClick={() => void retry()}>{retrying ? 'Queuing…' : 'Retry delivery'}</button>}</div>{notice && <p className="message success" role="status">{notice}</p>}<dl className="detail-grid"><div><dt>Endpoint</dt><dd>{job.endpointName}</dd></div><div><dt>URL</dt><dd className="mono">{job.endpointUrl}</dd></div><div><dt>Attempts</dt><dd>{job.attemptCount} lifetime · {job.cycleAttemptCount}/{job.maxAttempts} this cycle</dd></div><div><dt>Last HTTP status</dt><dd>{job.lastHttpStatus ?? '—'}</dd></div><div><dt>Next attempt</dt><dd>{showDate(job.nextAttemptAt)}</dd></div><div><dt>Last error</dt><dd>{job.lastErrorCode ?? '—'}</dd></div><div><dt>Created</dt><dd>{showDate(job.createdAt)}</dd></div><div><dt>Event ID</dt><dd className="mono">{job.eventId}</dd></div></dl></section>
      <section className="panel"><div className="panel-heading"><div><h2>Attempt history</h2><p>Every outbound request, including recovered unknown outcomes.</p></div></div>{job.attempts.length === 0 ? <p className="empty">No attempts yet.</p> : <div className="table-scroll"><table><thead><tr><th>#</th><th>Outcome</th><th>HTTP</th><th>Duration</th><th>Started</th><th>Error</th></tr></thead><tbody>{job.attempts.map(attempt => <tr key={attempt.id}><td>{attempt.attemptNumber}</td><td>{attempt.outcome ?? 'In progress'}</td><td>{attempt.httpStatus ?? '—'}</td><td>{attempt.durationMs == null ? '—' : `${attempt.durationMs} ms`}</td><td>{showDate(attempt.startedAt)}</td><td>{attempt.errorCode ?? '—'}</td></tr>)}</tbody></table></div>}</section></>}
  </div>
}
