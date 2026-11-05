// roguemargin.com: indexes the trader's public journal + Hyperliquid account data into
// its own SQLite and serves the terminal. Read-only towards the trader; if this dies, trading doesn't notice.
import { Database } from 'bun:sqlite'

const DB = process.env.DASH_DB ?? 'data/dash.db'
const TRADER = process.env.TRADER_URL ?? 'http://127.0.0.1:8790'
const PORT = Number(process.env.PORT ?? 8080)
const HL = process.env.HL_API ?? 'https://api.hyperliquid.xyz'

const db = new Database(DB, { create: true })
db.exec(`
pragma journal_mode = wal;
create table if not exists lanes (id text primary key, name text, model text, feed text, mode text, address text, start_equity real, started_at integer, retired_at integer);
create table if not exists equity (lane text, t integer, equity real, primary key (lane, t));
create table if not exists fills (lane text, t integer, coin text, side text, size real, price real, fee real, pnl real, reason text, id text primary key);
create table if not exists positions (lane text, coin text, size real, entry real, mark real, upnl real, leverage real, stop real, target real, liq real, primary key (lane, coin));
create table if not exists decisions (lane text, t integer, summary text, actions text, latency_ms integer, cost_usd real, id text primary key);
create table if not exists meta (k text primary key, v text);
`)
// columns added after launch
try { db.exec('alter table lanes add column treasury real default 0') } catch {}
try { db.exec('alter table positions add column trail real') } catch {}
try { db.exec('alter table fills add column net real') } catch {}

const getMeta = (k: string) => (db.query('select v from meta where k = ?').get(k) as { v: string } | null)?.v
const setMeta = (k: string, v: string) => db.run('insert or replace into meta values (?, ?)', [k, v])

// The trader appends events to its journal; we tail it by sequence number.
type Event =
  | { seq: number; type: 'lane'; lane: string; name: string; model: string; feed: string; mode: string; address?: string; start_equity: number; t: number; retired_at?: number }
  | { seq: number; type: 'equity'; lane: string; t: number; equity: number; treasury?: number; positions: any[] }
  | { seq: number; type: 'fill'; lane: string; t: number; coin: string; side: string; size: number; price: number; fee: number; pnl: number; net?: number; reason: string }
  | { seq: number; type: 'decision'; lane: string; t: number; summary: string; actions: unknown; latency_ms: number; cost_usd: number }

const apply = db.transaction((events: Event[]) => {
  for (const e of events) {
    if (e.type === 'lane')
      db.run('insert or replace into lanes values (?,?,?,?,?,?,?,?,?,?)', [e.lane, e.name, e.model, e.feed, e.mode, e.address ?? null, e.start_equity, e.t, e.retired_at ?? null, 0])
    else if (e.type === 'equity') {
      db.run('insert or replace into equity values (?,?,?)', [e.lane, e.t, e.equity])
      db.run('update lanes set treasury = ? where id = ?', [e.treasury ?? 0, e.lane])
      db.run('delete from positions where lane = ?', [e.lane])
      for (const p of e.positions)
        db.run('insert into positions values (?,?,?,?,?,?,?,?,?,?,?)', [e.lane, p.coin, p.size, p.entry, p.mark, p.upnl, p.leverage, p.stop, p.target ?? null, p.liq ?? null, p.trail ?? null])
    } else if (e.type === 'fill')
      db.run('insert or ignore into fills values (?,?,?,?,?,?,?,?,?,?,?)', [e.lane, e.t, e.coin, e.side, e.size, e.price, e.fee, e.pnl, e.reason, `j${e.seq}`, e.net ?? null])
    else if (e.type === 'decision')
      db.run('insert or ignore into decisions values (?,?,?,?,?,?,?)', [e.lane, e.t, e.summary, JSON.stringify(e.actions), e.latency_ms, e.cost_usd, `j${e.seq}`])
    setMeta('seq', String(e.seq))
  }
})

async function tailJournal() {
  const after = getMeta('seq') ?? '0'
  const events = (await (await fetch(`${TRADER}/journal?after=${after}`, { signal: AbortSignal.timeout(10_000) })).json()) as Event[]
  if (events.length) apply(events)
  return events.length
}

const info = async (body: object) =>
  (await fetch(`${HL}/info`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(10_000) })).json() as Promise<any>

// Live lanes: truth comes from the chain, not from the trader. Fills by hash, equity + positions from clearinghouseState.
async function indexLive() {
  const live = db.query("select id, address from lanes where mode = 'live' and address is not null and retired_at is null").all() as { id: string; address: string }[]
  for (const l of live) {
    const since = Number(getMeta(`fills:${l.id}`) ?? 0)
    const fills = (await info({ type: 'userFillsByTime', user: l.address, startTime: since + 1, aggregateByTime: true })) as any[]
    for (const f of fills)
      // ponytail: live net = closedPnl - this fill's fee; entry fee and funding are on other rows (userFunding)
      db.run('insert or ignore into fills values (?,?,?,?,?,?,?,?,?,?,?)', [l.id, f.time, f.coin, f.side === 'B' ? 'buy' : 'sell', Number(f.sz), Number(f.px), Number(f.fee), Number(f.closedPnl), f.dir, f.hash + ':' + f.tid, Number(f.closedPnl) ? Number(f.closedPnl) - Number(f.fee) : null])
    if (fills.length) setMeta(`fills:${l.id}`, String(Math.max(...fills.map((f) => f.time))))
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
}

// ---- web ----
const clients = new Set<ReadableStreamDefaultController>()
let stamp = 0

const curves = (since: number) =>
  db.query('select lane, t, equity from equity where t >= ? and (t > ? or t % 900000 = 0) order by t').all(since, Date.now() - 86400_000)
let tailFrom = Date.now()

function snapshot(full: boolean) {
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
    fills: db.query('select * from fills order by t desc limit 200').all(),
    decisions: db.query('select * from decisions order by t desc limit 60').all(),
  }
}

function broadcast() {
  stamp = Date.now()
  const msg = new TextEncoder().encode(`data: ${JSON.stringify(snapshot(false))}\n\n`)
  tailFrom = Math.floor(Date.now() / 60_000) * 60_000 - 60_000
  for (const c of clients) try { c.enqueue(msg) } catch { clients.delete(c) }
}

const page = Bun.file(new URL('../public/index.html', import.meta.url))
Bun.serve({
  port: PORT,
  idleTimeout: 0,
  routes: {
    '/': () => new Response(page, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' } }),
    '/api/state': () => Response.json(snapshot(true)),
    '/api/live': (req) => {
      let ctl: ReadableStreamDefaultController
      return new Response(
        new ReadableStream({
          start(c) { ctl = c; clients.add(c); c.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(snapshot(true))}\n\n`)) },
          cancel() { clients.delete(ctl) },
        }),
        { headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', 'x-accel-buffering': 'no' } },
      )
    },
    '/og.png': () => new Response(Bun.file(new URL('../public/og.png', import.meta.url))),
    '/favicon.svg': () => new Response(Bun.file(new URL('../public/favicon.svg', import.meta.url)), { headers: { 'content-type': 'image/svg+xml' } }),
  },
  fetch: () => new Response('not found', { status: 404 }),
})

async function loop() {
  for (;;) {
    try {
      const n = await tailJournal()
      await indexLive()
      if (n || clients.size) broadcast()
    } catch (e) {
      console.error('index', (e as Error).message)
    }
    await Bun.sleep(5_000)
  }
}
loop()
console.log(`dash on :${PORT}, tailing ${TRADER}`)
