import type { DeliveryDetail, DeliveryPage, DeliveryStatus, Endpoint, EventSummary, Page } from '../types'

const base = '/api/v1'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(base + path, init)
  if (!response.ok) {
    let message = `Request failed (${response.status})`
    try {
      const body = await response.json() as { error?: { message?: string } }
      message = body.error?.message || message
    } catch { /* An empty error body keeps the status message. */ }
    throw new Error(message)
  }
  return response.json() as Promise<T>
}

const json = (body: unknown): RequestInit => ({ headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })

export const api = {
  deliveries: (status: DeliveryStatus | '', page = 0) => request<DeliveryPage>(`/deliveries?page=${page}&size=20${status ? `&status=${status}` : ''}`),
  delivery: (id: string) => request<DeliveryDetail>(`/deliveries/${id}`),
  retry: (id: string) => request<{ id: string; status: string }>(`/deliveries/${id}/retry`, { method: 'POST' }),
  endpoints: () => request<Endpoint[]>('/endpoints?size=100'),
  createEndpoint: (body: Pick<Endpoint, 'name' | 'url' | 'eventTypes' | 'enabled'>) => request<Endpoint>('/endpoints', { method: 'POST', ...json(body) }),
  updateEndpoint: (id: string, body: Partial<Pick<Endpoint, 'name' | 'url' | 'eventTypes' | 'enabled'>>) => request<Endpoint>(`/endpoints/${id}`, { method: 'PATCH', ...json(body) }),
  events: () => request<Page<EventSummary>>('/events?size=20'),
  publish: (eventType: string, payload: unknown, key: string) => request<{ eventId: string; jobIds: string[] }>('/events', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    body: JSON.stringify({ eventType, payload }),
  }),
  receiverStatus: async () => (await fetch('/demo/control')).json() as Promise<{ status: number | 'timeout' }>,
  setReceiverStatus: async (status: number | 'timeout') => {
    const response = await fetch('/demo/control', { method: 'POST', ...json({ status }) })
    if (!response.ok) throw new Error('Could not change demo receiver')
  },
}
