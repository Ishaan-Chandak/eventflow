import { useEffect, useState } from 'react'
import { Dashboard } from './pages/Dashboard'
import { DeliveryDetail } from './pages/DeliveryDetail'
import { Endpoints } from './pages/Endpoints'
import { Publish } from './pages/Publish'

type Route = { page: 'dashboard' | 'endpoints' | 'publish' } | { page: 'delivery'; id: string }
function parseRoute(): Route {
  const value = window.location.hash.slice(1)
  if (value.startsWith('/delivery/')) return { page: 'delivery', id: value.slice('/delivery/'.length) }
  if (value === '/endpoints') return { page: 'endpoints' }
  if (value === '/publish') return { page: 'publish' }
  return { page: 'dashboard' }
}

function App() {
  const [route, setRoute] = useState<Route>(parseRoute)
  useEffect(() => { const update = () => setRoute(parseRoute()); window.addEventListener('hashchange', update); return () => window.removeEventListener('hashchange', update) }, [])
  const go = (path: string) => { window.location.hash = path; window.scrollTo(0, 0) }
  const active = route.page === 'delivery' ? 'dashboard' : route.page
  return <div className="app-shell"><aside className="sidebar"><div className="brand"><span className="brand-mark">↗</span><span>EventFlow<small>WEBHOOK DELIVERY</small></span></div><nav aria-label="Main navigation"><button className={active === 'dashboard' ? 'nav-active' : ''} onClick={() => go('/')}><span>◫</span> Deliveries</button><button className={active === 'endpoints' ? 'nav-active' : ''} onClick={() => go('/endpoints')}><span>◎</span> Endpoints</button><button className={active === 'publish' ? 'nav-active' : ''} onClick={() => go('/publish')}><span>↗</span> Publish event</button></nav><div className="sidebar-footer"><span className="online-dot" /> Local demo <small>At least once delivery</small></div></aside><main className="main-content"><div className="topbar"><span>EventFlow / <strong>{route.page === 'delivery' ? 'Delivery detail' : route.page === 'dashboard' ? 'Deliveries' : route.page === 'endpoints' ? 'Endpoints' : 'Publish'}</strong></span><a href="/actuator/health" target="_blank" rel="noreferrer">API health ↗</a></div><div className="content-width">{route.page === 'dashboard' && <Dashboard openDelivery={id => go(`/delivery/${id}`)} />}{route.page === 'endpoints' && <Endpoints />}{route.page === 'publish' && <Publish openDelivery={id => go(`/delivery/${id}`)} />}{route.page === 'delivery' && <DeliveryDetail id={route.id} back={() => go('/')} />}</div></main></div>
}

export default App
