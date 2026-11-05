// roguemargin.com: indexes the trader's public journal + Hyperliquid account data into
// its own SQLite and serves the terminal. Read-only towards the trader; if this dies, trading doesn't notice.
import { Database } from 'bun:sqlite'

const DB = process.env.DASH_DB ?? 'data/dash.db'
const TRADER = process.env.TRADER_URL ?? 'http://127.0.0.1:8790'
const PORT = Number(process.env.PORT ?? 8080)
const HL = process.env.HL_API ?? 'https://api.hyperliquid.xyz'

// This database is only an index of the journal and the chain, so a schema change simply rebuilds it.
const SCHEMA = '2'
const db = new Database(DB, { create: true })
db.exec('pragma journal_mode = wal; create table if not exists meta (k text primary key, v text);')
const getMeta = (k: string) => (db.query('select v from meta where k = ?').get(k) as { v: string } | null)?.v
const setMeta = (k: string, v: string) => db.run('insert or replace into meta values (?, ?)', [k, v])
if (getMeta('schema') !== SCHEMA) {
  db.exec('drop table if exists lanes; drop table if exists equity; drop table if exists fills; drop table if exists positions; drop table if exists decisions; delete from meta;')
  setMeta('schema', SCHEMA)
}
db.exec(`
create table if not exists lanes (id text primary key, name text, model text, feed text, mode text, address text, start_equity real, started_at integer, retired_at integer, treasury real default 0);
create table if not exists equity (lane text, t integer, equity real, primary key (lane, t));
create table if not exists fills (lane text, t integer, coin text, side text, size real, price real, fee real, pnl real, reason text, id text primary key, net real);
create table if not exists positions (lane text, coin text, size real, entry real, mark real, upnl real, leverage real, stop real, target real, liq real, trail real, primary key (lane, coin));
create table if not exists decisions (lane text, t integer, summary text, actions text, latency_ms integer, cost_usd real, id text primary key);
create index if not exists equity_t on equity (t);
create index if not exists fills_t on fills (t);
create index if not exists fills_lane on fills (lane);
create index if not exists decisions_t on decisions (t);
`)
// The chart shows 15-minute points past 24 h, so minute points older than 2 days are dead weight.
const prune = () => db.run('delete from equity where t < ? and t % 900000 != 0', [Date.now() - 2 * 86400_000])

// The trader appends events to its journal; we tail it by sequence number.
type Event =
  | { seq: number; type: 'lane'; lane: string; name: string; model: string; feed: string; mode: string; address?: string; start_equity: number; t: number; retired_at?: number }
  | { seq: number; type: 'equity'; lane: string; t: number; equity: number; treasury?: number; positions: any[] }
  | { seq: number; type: 'fill'; lane: string; t: number; coin: string; side: string; size: number; price: number; fee: number; pnl: number; net?: number; reason: string }
  | { seq: number; type: 'decision'; lane: string; t: number; summary: string; actions: unknown; latency_ms: number; cost_usd: number }

const n = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : null)
const str = (v: unknown, max: number) => (v == null ? null : String(v).slice(0, max))
// Only what the page renders is stored and served.
const publicActions = (xs: unknown) =>
  (Array.isArray(xs) ? xs.slice(0, 12) : []).filter((a) => a && typeof a === 'object').map((a: any) => ({
    coin: str(a.coin, 12), action: str(a.action, 16), result: str(a.result, 160),
    leverage: n(a.leverage), stop_loss: n(a.stop_loss), take_profit: n(a.take_profit), trailing: a.trailing === true || undefined,
  }))

const apply = db.transaction((events: Event[]) => {
  for (const e of events) {
    if (e.type === 'lane')
      db.run('insert or replace into lanes values (?,?,?,?,?,?,?,?,?,?)', [e.lane, e.name, e.model, e.feed, e.mode, e.address ?? null, e.start_equity, e.t, e.retired_at ?? null, 0])
    else if (e.type === 'equity') {
      db.run('insert or replace into equity values (?,?,?)', [e.lane, e.t, e.equity])
      db.run('update lanes set treasury = ? where id = ?', [n(e.treasury) ?? 0, e.lane])
      db.run('delete from positions where lane = ?', [e.lane])
      for (const p of Array.isArray(e.positions) ? e.positions : [])
        db.run('insert or replace into positions values (?,?,?,?,?,?,?,?,?,?,?)', [e.lane, str(p.coin, 12), n(p.size), n(p.entry), n(p.mark), n(p.upnl), n(p.leverage), n(p.stop), n(p.target), n(p.liq), n(p.trail)])
    } else if (e.type === 'fill')
      db.run('insert or ignore into fills values (?,?,?,?,?,?,?,?,?,?,?)', [e.lane, e.t, str(e.coin, 12), str(e.side, 8), n(e.size), n(e.price), n(e.fee), n(e.pnl), str(e.reason, 24), `j${e.seq}`, n(e.net)])
    else if (e.type === 'decision')
      db.run('insert or ignore into decisions values (?,?,?,?,?,?,?)', [e.lane, e.t, str(e.summary, 400) ?? '', JSON.stringify(publicActions(e.actions)), n(e.latency_ms), n(e.cost_usd), `j${e.seq}`])
    setMeta('seq', String(e.seq))
  }
})

async function tailJournal() {
  const after = getMeta('seq') ?? '0'
  const events = (await (await fetch(`${TRADER}/journal?after=${after}`, { signal: AbortSignal.timeout(10_000) })).json()) as Event[]
  if (!Array.isArray(events)) return 0
  if (events.length) apply(events)
  return events.length
}

const info = async (body: object) =>
  (await fetch(`${HL}/info`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(10_000) })).json() as Promise<any>

// Live lanes: truth comes from the chain, not from the trader. Fills by hash, equity + positions from clearinghouseState.
async function indexLive() {
  const live = db.query("select id, address from lanes where mode = 'live' and address is not null and retired_at is null").all() as { id: string; address: string }[]
  if (!live.length) return false
  for (const l of live) {
    const since = Number(getMeta(`fills:${l.id}`) ?? 0)
    const fills = (await info({ type: 'userFillsByTime', user: l.address, startTime: since + 1, aggregateByTime: true })) as any[]
    for (const f of fills)
      // ponytail: live net = closedPnl - this fill's fee; entry fee and funding are on other rows (userFunding)
      db.run('insert or ignore into fills values (?,?,?,?,?,?,?,?,?,?,?)', [l.id, f.time, f.coin, f.side === 'B' ? 'buy' : 'sell', Number(f.sz), Number(f.px), Number(f.fee), Number(f.closedPnl), f.dir, f.hash + ':' + f.tid, Number(f.closedPnl) ? Number(f.closedPnl) - Number(f.fee) : null])
    if (fills.length) setMeta(`fills:${l.id}`, String(fills.reduce((m, f) => Math.max(m, f.time), 0)))
    const s = await info({ type: 'clearinghouseState', user: l.address })
    const t = Math.floor(Date.now() / 60_000) * 60_000
    db.transaction(() => {
      db.run('insert or replace into equity values (?,?,?)', [l.id, t, Number(s.marginSummary.accountValue)])
      db.run('delete from positions where lane = ?', [l.id])
      for (const { position: p } of s.assetPositions) {
        const size = Number(p.szi)
        db.run('insert into positions values (?,?,?,?,?,?,?,?,?,?,?)', [l.id, p.coin, size, Number(p.entryPx), Number(p.positionValue) / Math.abs(size), Number(p.unrealizedPnl), p.leverage.value, null, null, p.liquidationPx ? Number(p.liquidationPx) : null, null])
      }
    })()
  }
  return true
}

// ---- web ----
const curves = (since: number) =>
  db.query('select lane, t, equity from equity where t >= ? and (t > ? or t % 900000 = 0) order by t').all(since, Date.now() - 86400_000)
let tailFrom = Date.now()
let stamp = 0

// fills and decisions only travel when they changed; the page keeps its previous copy otherwise
const version = (table: string) => JSON.stringify(db.query(`select count(*) as n, max(t) as t from ${table}`).get())
let sent = { fills: '', decisions: '' }

function snapshot(full: boolean) {
  const v = { fills: version('fills'), decisions: version('decisions') }
  const changed = { fills: full || v.fills !== sent.fills, decisions: full || v.decisions !== sent.decisions }
  if (!full) sent = v
  const lanes = db.query(`
    select l.*, (select equity from equity e where e.lane = l.id order by t desc limit 1) as equity,
      (select count(*) from fills f where f.lane = l.id) as trades,
      (select coalesce(sum(fee), 0) from fills f where f.lane = l.id) as fees
    from lanes l order by retired_at is not null, equity / start_equity desc`).all()
  return {
    stamp,
    lanes,
    // full history (15-min points past 24h) on connect, then only the newest minutes
    curves: full ? curves(Date.now() - 30 * 86400_000) : undefined,
    tail: full ? undefined : db.query('select lane, t, equity from equity where t >= ? order by t').all(tailFrom),
    positions: db.query('select * from positions').all(),
    fills: changed.fills ? db.query('select * from fills order by t desc limit 200').all() : undefined,
    decisions: changed.decisions ? db.query('select * from decisions order by t desc limit 60').all() : undefined,
  }
}

// The full snapshot is the expensive one (30 days of curves): build it at most once per update or every 5 s.
let full: { stamp: number; at: number; json: string } | undefined
function fullJSON() {
  if (!full || full.stamp !== stamp || Date.now() - full.at > 5_000) full = { stamp, at: Date.now(), json: JSON.stringify(snapshot(true)) }
  return full.json
}

// Live stream: bounded per visitor and in total, and a client that stops reading is dropped instead of buffered.
// per-IP is generous (phone carriers and offices put many visitors behind one address); a refused page polls the cached snapshot
const MAX_CLIENTS = 5000, MAX_PER_IP = 20, MAX_QUEUED = 4
type Client = { c: ReadableStreamDefaultController; ip: string }
const clients = new Set<Client>()
const perIp = new Map<string, number>()
const enc = new TextEncoder()
function drop(x: Client) {
  if (!clients.delete(x)) return
  const left = (perIp.get(x.ip) ?? 1) - 1
  left > 0 ? perIp.set(x.ip, left) : perIp.delete(x.ip)
  try { x.c.close() } catch {}
}

let lastSent = 0
function send(msg: Uint8Array) {
  lastSent = Date.now()
  for (const x of clients) {
    if ((x.c.desiredSize ?? 0) < -MAX_QUEUED) { drop(x); continue }
    try { x.c.enqueue(msg) } catch { drop(x) }
  }
}
function broadcast() {
  stamp = Date.now()
  const msg = enc.encode(`data: ${JSON.stringify(snapshot(false))}\n\n`)
  tailFrom = Math.floor(Date.now() / 60_000) * 60_000 - 60_000
  send(msg)
}
const ping = enc.encode(': ping\n\n') // keeps idle streams and proxies alive, and finds dead clients

// ---- headers ----
const page = await Bun.file(new URL('../public/index.html', import.meta.url)).text()
// The page's one inline script, allowed by hash: nothing injected into the page can run.
const script = page.match(/<script>([\s\S]*?)<\/script>/)?.[1] ?? ''
const scriptHash = new Bun.CryptoHasher('sha256').update(script).digest('base64')
const CSP = [
  "default-src 'none'",
  `script-src 'sha256-${scriptHash}'`,
  "style-src 'unsafe-inline' https://fonts.googleapis.com",
  'font-src https://fonts.gstatic.com',
  "img-src 'self' data:",
  "connect-src 'self'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
].join('; ')
const SECURITY = {
  'strict-transport-security': 'max-age=31536000; includeSubDomains',
  'x-content-type-options': 'nosniff',
  'referrer-policy': 'strict-origin-when-cross-origin',
  'permissions-policy': 'camera=(), microphone=(), geolocation=(), payment=(), usb=()',
  'cross-origin-opener-policy': 'same-origin',
  'x-frame-options': 'DENY',
}
const respond = (body: BodyInit | null, type: string, extra: Record<string, string> = {}, status = 200) =>
  new Response(body, { status, headers: { ...SECURITY, 'content-type': type, ...extra } })
const file = (name: string, type: string) => {
  const f = Bun.file(new URL(`../public/${name}`, import.meta.url))
  return () => respond(f, type, { 'cache-control': 'public, max-age=3600' })
}

Bun.serve({
  port: PORT,
  development: false, // never Bun's debug error page
  idleTimeout: 30, // the live stream opts out below; it gets data every 5 s anyway
  error: (e) => (console.error('http', e), respond('error', 'text/plain', {}, 500)),
  routes: {
    '/': { GET: () => respond(page, 'text/html; charset=utf-8', { 'cache-control': 'no-cache', 'content-security-policy': CSP }) },
    '/api/state': { GET: () => respond(fullJSON(), 'application/json', { 'cache-control': 'no-cache' }) },
    '/api/live': {
      GET: (req, server) => {
        // Tugport's proxy sets x-real-ip to the real client; the container port is not reachable from outside
        const ip = req.headers.get('x-real-ip') ?? server.requestIP(req)?.address ?? 'unknown'
        if (clients.size >= MAX_CLIENTS || (perIp.get(ip) ?? 0) >= MAX_PER_IP)
          return respond('too many live connections, retry later', 'text/plain', { 'retry-after': '30' }, 429)
        server.timeout(req, 0)
        let me: Client
        req.signal.addEventListener('abort', () => drop(me)) // free the slot as soon as the visitor leaves
        return respond(
          new ReadableStream({
            start(c) {
              me = { c, ip }
              clients.add(me)
              perIp.set(ip, (perIp.get(ip) ?? 0) + 1)
              c.enqueue(enc.encode(`data: ${fullJSON()}\n\n`))
            },
            cancel() { drop(me) },
          }),
          'text/event-stream',
          { 'cache-control': 'no-cache', 'x-accel-buffering': 'no' },
        )
      },
    },
    '/og.png': { GET: file('og.png', 'image/png') },
    '/favicon.svg': { GET: file('favicon.svg', 'image/svg+xml') },
  },
  fetch: () => respond('not found', 'text/plain', {}, 404),
})

async function loop() {
  let lastPrune = 0
  for (;;) {
    try {
      const got = await tailJournal()
      const live = await indexLive()
      if (got || live) broadcast() // only when something changed: the trader writes once a minute
      else if (clients.size && Date.now() - lastSent > 25_000) send(ping)
      if (Date.now() - lastPrune > 3600_000) { prune(); lastPrune = Date.now() }
    } catch (e) {
      console.error('index', (e as Error).message)
    }
    await Bun.sleep(5_000)
  }
}
loop()
console.log(`dash on :${PORT}, tailing ${TRADER}`)
