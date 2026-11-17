import type { DeliveryStatus } from '../types'

const labels: Record<DeliveryStatus, string> = {
  QUEUED: 'Queued', IN_PROGRESS: 'In progress', RETRY_SCHEDULED: 'Retry scheduled', SUCCEEDED: 'Succeeded', DEAD: 'Dead',
}

export function StatusBadge({ status }: { status: DeliveryStatus }) {
  return <span className={`badge badge-${status.toLowerCase()}`}>{labels[status]}</span>
}
