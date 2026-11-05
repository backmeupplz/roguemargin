// roguemargin.com: indexes the trader's public journal + Hyperliquid account data into
// its own SQLite and serves the terminal. Read-only towards the trader; if this dies, trading doesn't notice.
import { Database } from 'bun:sqlite'
import { ok as assert } from 'node:assert'

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
const PAGE = 50 // trades per page
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
    // the newest page of trades travels live; older pages come from /api/fills
    fills: changed.fills ? db.query('select * from fills order by t desc, id desc limit ?').all(PAGE) : undefined,
    fillsTotal: (db.query('select count(*) as n from fills').get() as { n: number }).n,
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

// ---- visitor address ----
// Behind Cloudflare every request arrives from an edge IP; the visitor's own address is in cf-connecting-ip,
// which anyone can forge by hitting the origin directly, so it only counts when the request comes from Cloudflare.
// Ranges from https://www.cloudflare.com/ips (stable; refresh if Cloudflare announces changes).
const CLOUDFLARE = ['173.245.48.0/20', '103.21.244.0/22', '103.22.200.0/22', '103.31.4.0/22', '141.101.64.0/18', '108.162.192.0/18', '190.93.240.0/20', '188.114.96.0/20', '197.234.240.0/22', '198.41.128.0/17', '162.158.0.0/15', '104.16.0.0/13', '104.24.0.0/14', '172.64.0.0/13', '131.0.72.0/22',
  '2400:cb00::/32', '2606:4700::/32', '2803:f800::/32', '2405:b500::/32', '2405:8100::/32', '2a06:98c0::/29', '2c0f:f248::/32']
function ipBits(ip: string): { v: bigint; len: number } | null {
  if (/^\d+\.\d+\.\d+\.\d+$/.test(ip)) {
    const p = ip.split('.').map(Number)
    return p.every((x) => x <= 255) ? { v: p.reduce((n, x) => (n << 8n) + BigInt(x), 0n), len: 32 } : null
  }
  if (!ip.includes(':')) return null
  const [head = '', tail = '', extra] = ip.split('::')
  if (extra !== undefined) return null
  const h = head ? head.split(':') : [], t = tail ? tail.split(':') : []
  const fill = 8 - h.length - t.length
  if (ip.includes('::') ? fill < 1 : fill !== 0) return null
  let v = 0n
  for (const g of [...h, ...Array(fill).fill('0'), ...t]) { if (!/^[0-9a-f]{1,4}$/i.test(g)) return null; v = (v << 16n) + BigInt(parseInt(g, 16)) }
  return { v, len: 128 }
}
const inRange = (ip: string, cidr: string) => {
  const [base = '', bits = ''] = cidr.split('/'), a = ipBits(ip), b = ipBits(base)
  if (!a || !b || a.len !== b.len) return false
  const shift = BigInt(a.len - Number(bits))
  return a.v >> shift === b.v >> shift
}
export function visitor(peer: string, cfHeader: string | null) {
  return cfHeader && ipBits(cfHeader) && CLOUDFLARE.some((r) => inRange(peer, r)) ? cfHeader : peer
}

if (process.argv.includes('--check')) {
  assert(visitor('104.16.5.1', '203.0.113.9') === '203.0.113.9', 'Cloudflare v4 edge: trust its header')
  assert(visitor('2606:4700:10::6816:1', '2001:db8::1') === '2001:db8::1', 'Cloudflare v6 edge: trust its header')
  assert(visitor('8.8.8.8', '203.0.113.9') === '8.8.8.8', 'not Cloudflare: header ignored')
  assert(visitor('104.16.5.1', 'not-an-ip') === '104.16.5.1', 'garbage header ignored')
  assert(visitor('104.24.0.1', null) === '104.24.0.1' && !inRange('104.32.0.1', '104.24.0.0/14') && inRange('2a06:98c7::1', '2a06:98c0::/29'), 'ranges')
  console.log('dash ok')
  process.exit(0)
}

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
    // older trades, one page at a time (?page=0 is the newest); the page number is clamped, nothing else is read from the query
    '/api/fills': {
      GET: (req) => {
        const page = Math.min(100_000, Math.max(0, Math.floor(Number(new URL(req.url).searchParams.get('page')) || 0)))
        const fills = db.query('select * from fills order by t desc, id desc limit ? offset ?').all(PAGE, page * PAGE)
        const total = (db.query('select count(*) as n from fills').get() as { n: number }).n
        return respond(JSON.stringify({ page, size: PAGE, total, fills }), 'application/json', { 'cache-control': 'no-cache' })
      },
    },
    '/api/live': {
      GET: (req, server) => {
        // Tugport's proxy sets x-real-ip to the real client; the container port is not reachable from outside
        const ip = visitor(req.headers.get('x-real-ip') ?? server.requestIP(req)?.address ?? 'unknown', req.headers.get('cf-connecting-ip'))
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
