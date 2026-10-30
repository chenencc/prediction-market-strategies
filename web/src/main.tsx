import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';
type Book = {
    instrument_id: string;
    outcome: string;
    bids: string[][];
    asks: string[][];
    fresh: boolean;
    age_ms: number | null;
    held: string;
    cost: string;
    liquidation_value: string | null;
    batches: number;
    received_ns: string | null;
};
type State = {
    run_id: string;
    source: string;
    slug: string;
    title?: string;
    strategy: string;
    status: string;
    phase: string;
    reason: string;
    error?: string;
    writer_error?: string;
    published_ns: string;
    observed_ns: string;
    started_ns: string;
    books: Book[];
    orders: Record<string, any>[];
    events: Record<string, any>[];
    fills: Record<string, any>[];
    account: Record<string, any>;
    rules?: Record<string, any>;
    cycles?: number;
    session_id?: string;
    window_end_ns?: string;
    event_count: number;
    persisted_event_seq: number;
};
type Point = {
    index: number;
    observed_ns: string | null;
    equity: string | null;
    books: {
        outcome: string;
        fresh: boolean;
        bid: string | null;
        ask: string | null;
    }[];
};
const labels: Record<string, string> = { cooldown: '等待下一轮', draining: '到期前停止开仓', cycle_cooldown: '本轮已平仓，冷却后继续买卖', market_rollover: '停止新开仓，等待下一期 BTC 市场', window_closing: '到期前退出持仓', cycle_closed: '一轮交易完成', initializing: '准备中', running: '运行中', completed: '已结束', failed: '需要检查', stopping: '收尾中', waiting: '等待入场', buying: '买入待确认', holding: '持仓中', selling: '卖出待确认', blocked: '保护暂停', waiting_for_snapshot: '等待完整盘口', stale_book: '盘口已过期，等待新数据', above_buy_threshold: '当前卖价高于买入阈值', buy_threshold_met: '价格满足买入条件', round_trip_complete: '一轮买入与卖出已完成', entry_unfilled: '买入未成交，本轮不重复追单', holding_for_exit: '等待止盈、止损或持有时间到期', hold_time_elapsed: '持有时间到期', take_profit: '达到止盈价格', stop_loss: '达到止损价格', spread_too_wide: '买卖价差超过上限', waiting_for_order_confirmation: '等待订单终态确认', insufficient_depth: '可买深度不足', insufficient_exit_depth: '可卖深度不足', below_market_minimum: '数量低于市场最小订单', cash_or_notional_limit: '资金或金额限制', order_confirmation_timeout: '订单确认超时', residual_below_market_minimum: '剩余仓位小于市场最小订单', exit_attempt_limit: '卖出尝试已达上限', fetching_market_metadata: '正在读取官方市场信息', initialization_failed: '市场连接或初始化失败', market_expired: '市场已到期', decision: '策略决策', fill: '模拟成交', order_event: '订单回报', started: '策略启动', stopped: '策略结束', socket_state: '行情连接变化', protection_halt: '保护暂停' };
const label = (s: string) => labels[s] || s || '—';
const n = (v: any, d = 2) => v === null || v === undefined ? '—' : Number(v).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
const clock = (v: any) => !v ? '—' : new Date(Number(BigInt(v) / 1000000n)).toLocaleTimeString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' });
const stamp = (v: any) => !v ? '—' : new Date(Number(BigInt(v) / 1000000n)).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Asia/Shanghai' });
const short = (s: string) => s?.length > 24 ? s.slice(0, 10) + '…' + s.slice(-8) : s || '—';
async function get<T>(url: string): Promise<T> {
    const r = await fetch(url);
    if (!r.ok)
        throw new Error(`读取失败 ${r.status}`);
    return r.json();
}
function Chart({ points: rawPoints, fills }: {
    points: Point[];
    fills: State['fills'];
}) {
    const points = rawPoints.filter((p): p is Point & {
        observed_ns: string;
    } => typeof p.observed_ns === 'string' && /^\d+$/.test(p.observed_ns));
    const books = points.map(p => p.books.find(b => ['yes', 'up'].includes(b.outcome.toLowerCase())));
    const values = books.flatMap(b => b?.fresh ? [b.bid, b.ask] : []).filter(v => v != null).map(Number);
    if (!values.length)
        return <div className="empty chart-empty">此时段没有有效价格记录</div>;
    const lo = Math.max(0, Math.min(...values) - .01), hi = Math.min(1, Math.max(...values) + .01), span = Math.max(.01, hi - lo);
    const start = BigInt(points[0].observed_ns), end = BigInt(points.at(-1)!.observed_ns), duration = Number(end - start) || 1;
    const x = (time: string) => 48 + Number(BigInt(time) - start) / duration * 800;
    const y = (price: number) => 172 - (price - lo) / span * 140;
    const path = (key: 'bid' | 'ask') => {
        let pen = false;
        return books.map((b, i) => {
            const value = b?.[key];
            if (!b?.fresh || value == null) {
                pen = false;
                return '';
            }
            const command = pen ? 'L' : 'M';
            pen = true;
            return command + x(points[i].observed_ns) + ',' + y(Number(value));
        }).join(' ');
    };
    return <svg className="chart" viewBox="0 0 880 206" role="img" aria-label="主交易方向买一卖一与模拟成交；空白表示没有有效价格">
    {[0, 1, 2, 3].map(i => <g key={i}><line x1="48" x2="848" y1={32 + i * 46.7} y2={32 + i * 46.7} stroke="#26303d"/><text x="4" y={36 + i * 46.7} fill="#8190a2" fontSize="11">{n(hi - i * span / 3, 3)}</text></g>)}
    <path d={path('ask')} stroke="#ed8a91" fill="none" strokeWidth="1.8"/>
    <path d={path('bid')} stroke="#55cdb1" fill="none" strokeWidth="1.8"/>
    {fills.filter(f => BigInt(f.now_ns) >= start && BigInt(f.now_ns) <= end).map(f => <g key={f.client_order_id + f.trade_id}>
      <circle cx={x(f.now_ns)} cy={y(Number(f.price))} r="5" fill={f.side === 'BUY' ? '#73e7c0' : '#ffaeb5'} stroke="#0b1017" strokeWidth="2"><title>{clock(f.now_ns) + ' 模拟 ' + f.side + ' ' + f.quantity + ' @ ' + f.price}</title></circle>
      <text x={x(f.now_ns) + 8} y={y(Number(f.price)) + (f.side === 'BUY' ? -10 : 17)} fill="#dce5ef" fontSize="11">{f.side === 'BUY' ? '买' : '卖'}</text>
    </g>)}
    <text x="48" y="198" fill="#8190a2" fontSize="11">{clock(points[0].observed_ns)}</text><text x="780" y="198" fill="#8190a2" fontSize="11">{clock(points.at(-1)?.observed_ns)}</text>
  </svg>;
}
function Ladder({ book, orders }: {
    book: Book;
    orders: State['orders'];
}) { const max = Math.max(1, ...book.bids.slice(0, 6).map(x => Number(x[1])), ...book.asks.slice(0, 6).map(x => Number(x[1]))); return <div className="ladder"><div className="ladder-title"><strong>{book.outcome.toUpperCase()}</strong><span className={book.fresh ? 'good' : 'warn'}>{book.fresh ? '快照有效' : '快照过期 / 缺失'}</span></div><div className="ladder-row headings"><span>价格</span><span>数量 · shares</span></div>{book.asks.slice(0, 6).reverse().map((a, i) => <div className="ladder-row ask" key={'a' + i} style={{ background: `linear-gradient(to left,#a14c582b ${Number(a[1]) / max * 100}%,transparent 0)` }}><span>{n(a[0], 3)}</span><span>{n(a[1], 1)}</span></div>)}<div className="spread">价差 {book.asks.length && book.bids.length ? n(Number(book.asks[0][0]) - Number(book.bids[0][0]), 3) : '—'} <span>更新 {clock(book.received_ns)}</span></div>{book.bids.slice(0, 6).map((b, i) => <div className="ladder-row bid" key={'b' + i} style={{ background: `linear-gradient(to left,#2588752b ${Number(b[1]) / max * 100}%,transparent 0)` }}><span>{n(b[0], 3)}</span><span>{n(b[1], 1)}</span></div>)}{!book.bids.length && !book.asks.length && <div className="empty">没有完整原生盘口</div>}<div className="ladder-foot">持仓 {n(book.held)} · 挂单 {orders.filter(o => o.instrument_id === book.instrument_id && !['FILLED', 'CANCELED', 'REJECTED', 'EXPIRED', 'DENIED'].includes(o.status)).length}</div></div>; }
function App() {
    const [follow, setFollow] = useState(true);
    const [runs, setRuns] = useState<State[]>([]), [id, setId] = useState(''), [state, setState] = useState<State | null>(null), [history, setHistory] = useState<Point[]>([]), [link, setLink] = useState('connecting'), [tab, setTab] = useState('orders'), [error, setError] = useState(''), [frame, setFrame] = useState<number | null>(null), [tick, setTick] = useState(Date.now());
    useEffect(() => { const update = () => get<State[]>('/api/runs').then(r => { setRuns(r); setId(old => follow ? (r[0]?.run_id || old) : (old || r[0]?.run_id || '')); }).catch(e => setError(e.message)); update(); const t = setInterval(update, 5000); return () => clearInterval(t); }, [follow]);
    useEffect(() => { const t = setInterval(() => setTick(Date.now()), 1000); return () => clearInterval(t); }, []);
    useEffect(() => {
        if (!id)
            return;
        setState(null);
        setHistory([]);
        setFrame(null);
        setError('');
        setLink('connecting');
        let active = true;
        const es = new EventSource(`/api/runs/${id}/stream`);
        es.addEventListener('state', e => {
            if (active) {
                setState(JSON.parse((e as MessageEvent).data));
                setLink('connected');
            }
        });
        es.onerror = () => setLink('disconnected');
        const update = () => get<Point[]>(`/api/runs/${id}/history`).then(h => {
            if (active)
                setHistory(h);
        }).catch(e => {
            if (active)
                setError(e.message);
        });
        update();
        const t = setInterval(update, 5000);
        return () => { active = false; es.close(); clearInterval(t); };
    }, [id]);
    const [historical, setHistorical] = useState<State | null>(null);
    useEffect(() => {
        if (frame == null) {
            setHistorical(null);
            return;
        }
        let active = true;
        get<State>(`/api/runs/${id}/frames/${frame}`).then(s => {
            if (active)
                setHistorical(s);
        }).catch(e => setError(e.message));
        return () => { active = false; };
    }, [id, frame]);
    const s = frame == null ? state : historical, a = s?.account || {}, age = state?.published_ns ? (tick - Number(BigInt(state.published_ns) / 1000000n)) / 1000 : null, stale = !['completed', 'failed'].includes(state?.status || '') && age !== null && age > 3;
    const chartPoints = frame == null ? [...history] : history.slice(0, frame + 1);
    if (frame == null && s?.observed_ns && (!chartPoints.length || !chartPoints.at(-1)?.observed_ns || BigInt(s.observed_ns) > BigInt(chartPoints.at(-1)!.observed_ns!))) {
        chartPoints.push({ index: history.length, observed_ns: s.observed_ns, equity: a.equity, books: s.books.map(b => ({ outcome: b.outcome, fresh: b.fresh, bid: b.bids[0]?.[0] ?? null, ask: b.asks[0]?.[0] ?? null })) });
    }
    const activeRun = state?.status === 'running', feedAge = state?.books?.length ? Math.max(...state.books.map(b => b.received_ns ? (tick - Number(BigInt(b.received_ns) / 1000000n)) : Infinity)) : Infinity;
    return <div className="shell"><header><div className="brand"><span className="logo">◈</span> PAPER <span>OBSERVATORY</span></div><span className="paper-badge">模拟交易 · 无真实订单</span><div className="connection"><i className={link === 'connected' && !stale ? 'dot' : 'dot off'}/>{link === 'connected' ? (stale ? '运行心跳过期' : '界面已连接') : '界面重连中'}<span>{new Date(tick).toLocaleTimeString('zh-CN', { hour12: false, timeZone: 'Asia/Shanghai' })} 上海</span></div></header><aside><div className="nav-title">运行工作区 <span>{runs.length}</span></div><button className="nav-active" onClick={() => { setFollow(true); setFrame(null); }}>◉　{follow ? '正在跟随当前策略' : '跟随当前策略'}</button><div className="nav-title section-gap">最近运行</div>{runs.map(r => <button key={r.run_id} className={'run ' + (id === r.run_id ? 'selected' : '')} onClick={() => { setFollow(false); setId(r.run_id); }}><strong>{r.title || r.slug}</strong><span>{r.source === 'live_public_feed' ? '真实行情 Paper' : r.source === 'saved_real_replay' ? '已保存真实数据回放' : '合成场景验证'}</span><small>{stamp(r.started_ns)} · {label(r.status)}</small></button>)}<div className="aside-foot">LOCAL RESEARCH TERMINAL<br />POLYMARKET / NAUTILUS<br /><span>行情真实与否、成交模拟与否分别标记</span></div></aside><main>{!s ? <div className="empty welcome"><h1>Paper 运行监控</h1><p>{id ? '正在连接运行记录…' : '暂无运行记录。启动一场本机 Paper 后会自动显示。'}</p>{error}</div> : <><div className="page-title"><div><div className="eyebrow">POLYMARKET · {s.strategy === 'continuous-market' ? 'BTC 持续循环买卖 · Paper' : s.strategy === 'single-market' ? '单市场买入 / 卖出' : s.strategy}</div><h1>{s.title || s.slug}</h1><p>{s.source === 'live_public_feed' ? '官方实时行情' : s.source === 'saved_real_replay' ? '已保存真实行情 · 离线回放' : '合成数据 · 撮合验证'}　/　{short(s.run_id)}　/　{frame == null ? '最新状态' : '历史帧 ' + frame}</p></div><span className={'status ' + (s.status === 'failed' ? 'danger' : '')}>{label(s.status)}</span></div>{(s.error || s.writer_error || error || stale) && <div className="alert">{s.error || s.writer_error || error || '超过 3 秒没有运行心跳，当前显示最后已知状态。'}</div>}{s.events.some(e => e.kind === 'socket_state' && e.state?.includes('DISCONNECTED')) && <div className="alert">本轮记录到行情断线；空白区不补造价格。完成模拟买卖不代表行情连接稳定。</div>}<div className="live-strip"><span>界面：{link === 'connected' ? '已连接' : '重连中'}</span><span>策略：{stale ? '心跳过期' : label(state?.status || 'initializing')}</span><span className={activeRun && feedAge < 3000 ? 'good' : 'warn'}>官方行情：{!activeRun ? '未运行，查看记录' : feedAge < 3000 ? '持续更新' : '等待有效盘口'}</span><span>{activeRun && Number.isFinite(feedAge) ? '距最近行情 ' + n(Math.max(0, feedAge) / 1000, 1) + 's' : ''}</span></div><div className="metrics">{[['账户权益', a.equity], ['可用现金', a.cash_free], ['已实现盈亏', a.session_realized_pnl ?? a.realized_pnl], ['未实现盈亏', a.unrealized_pnl], ['累计手续费', a.session_fees ?? a.fees]].map(([k, v]) => <div className="metric" key={k}><span>{k}</span><strong className={k.includes('盈亏') && Number(v) !== 0 ? (Number(v) > 0 ? 'good' : 'bad') : ''}>{n(v)}</strong><small>{a.currency || '—'}{k === '账户权益' ? ' · 原生买盘估值' : ''}</small></div>)}</div><div className="workspace"><div className="left-column"><section><div className="section-head"><h2>{s.books.find(b => ['yes', 'up'].includes(b.outcome.toLowerCase()))?.outcome.toUpperCase() || '盘口'} 价格与执行</h2><span><b className="good">— 买一</b>　<b className="bad">— 卖一</b></span></div><Chart points={chartPoints} fills={s.fills}/><div className="replay"><span>历史回看 · 空白为无有效行情</span><input aria-label="历史回放进度" type="range" min="0" max={Math.max(0, history.length - 1)} value={frame ?? Math.max(0, history.length - 1)} onChange={e => setFrame(Number(e.target.value))}/><button onClick={() => setFrame(null)}>回到最新</button></div></section><section className="decision"><div className="section-head"><h2>策略正在做什么</h2><span className="chip">{label(s.phase)}</span></div><h3>{label(s.reason)}</h3><div className="rules"><span>买入阈值 <b>≤ {n(s.rules?.buy_below, 3)}</b></span><span>每轮数量 <b>{n(s.rules?.quantity)} shares</b></span><span>止盈 / 止损 <b>+{n(s.rules?.take_profit, 3)} / −{n(s.rules?.stop_loss, 3)}</b></span><span>退出计时 <b>{s.rules?.hold_ns ? Number(s.rules.hold_ns) / 1e9 : '—'} 秒</b></span></div><p>{s.rules?.repeat ? '循环买卖；每轮平仓后冷却 5 秒再入场。' : '单轮、限价 IOC。'}计时到期后仍须等到有效盘口才能退出；未成交不计作成交。{s.rules?.repeat ? '这是观察盘口的演示策略，不是盈利策略。' : ''}</p></section><section className="activity"><div className="tabs">{[['orders', '订单'], ['fills', '模拟成交'], ['positions', '持仓'], ['events', '事件时间线']].map(([k, t]) => <button key={k} onClick={() => setTab(k)} className={tab === k ? 'active' : ''}>{t}{k === 'orders' ? ' ' + s.orders.length : k === 'fills' ? ' ' + s.fills.length : ''}</button>)}</div><div className="table-wrap">{tab === 'orders' ? <table><thead><tr><th>订单</th><th>方向</th><th>限价</th><th>数量 / 已成</th><th>状态</th></tr></thead><tbody>{[...s.orders].sort((a, b) => { const x = BigInt(a.ts_init || 0), y = BigInt(b.ts_init || 0); return x === y ? 0 : x > y ? -1 : 1; }).map(o => <tr key={o.client_order_id}><td title={o.client_order_id}>{short(o.client_order_id)}</td><td className={o.side === 'BUY' ? 'good' : 'bad'}>{o.side}</td><td>{n(o.price, 3)}</td><td>{n(o.quantity)} / {n(o.filled_qty)}</td><td>{o.status}</td></tr>)}</tbody></table> : tab === 'fills' ? <table><thead><tr><th>上海时间</th><th>方向</th><th>成交价</th><th>份数</th><th>手续费</th></tr></thead><tbody>{[...s.fills].reverse().map(f => <tr key={f.client_order_id + f.trade_id}><td>{clock(f.now_ns)}</td><td className={f.side === 'BUY' ? 'good' : 'bad'}>{f.side}</td><td>{n(f.price, 3)}</td><td>{n(f.quantity)}</td><td>{n(f.commission, 5)}</td></tr>)}</tbody></table> : tab === 'positions' ? <table><thead><tr><th>结果</th><th>份数</th><th>剩余成本</th><th>买盘估值</th></tr></thead><tbody>{s.books.map(b => <tr key={b.instrument_id}><td>{b.outcome}</td><td>{n(b.held)}</td><td>{n(b.cost)}</td><td>{n(b.liquidation_value)}</td></tr>)}</tbody></table> : <div className="timeline">{[...s.events].reverse().map((e, i) => <div key={i}><time>{clock(e.now_ns)}</time><b>{label(e.kind)}</b><span>{e.reason ? label(e.reason) : (e.event_type || e.state || e.error || '')}{e.price ? ' · ' + e.side + ' ' + e.quantity + ' @ ' + e.price : ''}</span></div>)}</div>}{tab === 'orders' && !s.orders.length && <div className="empty">尚未提交模拟订单，请查看上方策略原因</div>}{tab === 'fills' && !s.fills.length && <div className="empty">尚无模拟成交</div>}</div></section></div><div className="right-column"><section><div className="section-head"><h2>原生双盘口</h2><span>价格 / 份数</span></div>{s.books.map(b => <Ladder key={b.instrument_id} book={b} orders={s.orders}/>)}{!s.books.length && <div className="empty">等待市场连接</div>}</section><section className="health"><h2>数据与模型</h2>{s.rules?.repeat && <><p>已完成循环 <b>{s.cycles ?? 0}</b></p><p>本期到期 <b>{clock(s.window_end_ns)} 上海</b></p><p>资金继承 <b>跨期累计 · 不重置</b></p></>}<p>行情批次 <b>{s.books.reduce((t, b) => t + b.batches, 0)}</b></p><p>最新观测 <b>{clock(s.observed_ns)}</b></p><p>事件已记录 <b>{s.persisted_event_seq ?? 0} / {s.event_count ?? 0}</b></p><p>成交方式 <b>Nautilus 模拟撮合</b></p><p>订单延迟 <b>配置值 100 ms</b></p><small>持仓估值使用原生买盘深度，尚未扣除假设卖出的费用。盘口过期时不补造价格。奖励与链上结算不计入收益。</small></section></div></div></>}</main><footer><span>◈ 本机只读监控</span><span>页面断开不停止 Paper · 原始时间保留 · 金额精度由引擎维护</span><span>v0.1</span></footer></div>;
}
createRoot(document.getElementById('root')!).render(<App />);
