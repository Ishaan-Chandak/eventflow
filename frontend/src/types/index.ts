export type DeliveryStatus = 'QUEUED' | 'IN_PROGRESS' | 'RETRY_SCHEDULED' | 'SUCCEEDED' | 'DEAD'

export interface Endpoint {
  id: string
  name: string
  url: string
  enabled: boolean
  eventTypes: string[]
  createdAt: string
}

export interface Delivery {
  id: string
  eventId: string
  endpointId: string
  endpointName: string
  eventType: string
  status: DeliveryStatus
  attemptCount: number
  nextAttemptAt: string | null
  lastHttpStatus: number | null
  createdAt: string
}

export interface Attempt {
  id: string
  attemptNumber: number
  startedAt: string
  finishedAt: string | null
  outcome: string | null
  httpStatus: number | null
  durationMs: number | null
  errorCode: string | null
}

export interface DeliveryDetail extends Delivery {
  endpointUrl: string
  cycleAttemptCount: number
  maxAttempts: number
  lastErrorCode: string | null
  updatedAt: string
  attempts: Attempt[]
}

export interface Page<T> {
  items: T[]
  total: number
  page: number
  size: number
}

export interface DeliveryPage extends Page<Delivery> {
  summary: Record<DeliveryStatus, number>
}

export interface EventSummary {
  id: string
  eventType: string
  createdAt: string
  jobCount: number
}
