// Pump.fun Paper Sprint: Any Bot Can Win — fresh-coin sniper strategy.
//
// The harness speaks agent-fun/replay/v2: a hello message fixes the caps, fees
// and latency, then one batch per slot carries that slot's events, marks and
// fills, and this bot answers each batch with its orders. Every decision here
// is a pure function of that hello / batch stream — no clock, no randomness,
// no network — so the two grading executions produce byte-identical orders.
//
// Thesis: the fattest moves in a 6-hour sprint happen in a coin's first
// minutes. Buy very young bonding curves (0-120 slots old) whose opening tape
// is buy-dominated, broad-based and stepping up, one fixed 0.08 SOL shot per
// mint, then let winners run on take-profit / trailing-stop exits. A
// guardrail-pacing fallback sends minimum-size 0.01 SOL probe round trips on
// the hottest active coin whenever the filled-coin pace lags, so the
// 20-distinct-mint guardrail is met even on a quiet window.
//
// T3 EV rationale: at a ~15% win rate, TP +25% / SL -15% is -9% EV per trade
// before fees (0.15*25 - 0.85*15), so T4 tightens entry selectivity and
// stretches the payoff (TP +60%, SL -12%) so winners pay for many more losers.
//
// T6 revise2: the dev window shows almost every new coin's opening tape looks
// buy-dominated, so the T4 filters barely cut entries; v2 graded -0.4246 SOL
// (12% win, 651 coins). This variant is one stricter, bigger-payoff step:
// entries need a 0.50 SOL net flow, 6 distinct buyers, +15% runup and a coin
// that survived its first minute (minAge 60 slots, real SOL >= 2.0), and
// winners run to +100% TP with a 30%/20% trail and a 900-slot max hold.

import {
  LAMPORTS_PER_SOL,
  type Batch,
  type Bot,
  type BotState,
  type OrderInput,
} from './vendor/makefun-bot.ts';

// ---------------------------------------------------------------- parameters

export interface StrategyParams {
  // Entry window: age of the curve in slots (current slot - CreateEvent slot).
  minAge: number;
  maxAge: number;
  // Opening-tape filters.
  minTrades: number;
  minBuyShareBps: number; // share of trades that are buys, in bps
  minBuyers: number; // distinct non-creator buyers
  maxBuyerShareBps: number; // max share of buy SOL from a single buyer, in bps
  minNetFlowLamports: bigint; // buys minus sells, in lamports
  minRunupBps: number; // price vs creation price, in bps
  maxRunupBps: number;
  minRealSolLamports: bigint;
  maxRealSolLamports: bigint;
  maxBigSellLamports: bigint; // a single sell this big disqualifies / exits
  skipCreatorSold: boolean;
  // Sizing.
  stakeLamports: bigint; // single fixed first buy; no adds, no pyramiding
  buySlippageBps: number;
  sellSlippageBps: number;
  // Exits (whole position, first trigger wins).
  takeProfitBps: number;
  stopLossBps: number;
  trailArmBps: number; // arm the trailing stop once up this much
  trailBps: number; // exit on this much giveback from the peak
  maxHoldSlots: number;
  fadeNetSolLamports: bigint; // exit when 20-slot net flow is below -this
  exitOnCreatorSell: boolean;
  bigSellExitSolLamports: bigint;
  staleSlots: number; // exit when no trade for this many slots
  // Guardrail pacing fallback (probe round trips).
  paceSecs: number;
  targetCoins: number;
  targetFills: number;
  probeStakeLamports: bigint;
  probeMinRealSolLamports: bigint;
}

export const DEFAULT_PARAMS: StrategyParams = {
  minAge: 60,
  maxAge: 120,
  minTrades: 5,
  minBuyShareBps: 8000,
  minBuyers: 6,
  maxBuyerShareBps: 5000,
  minNetFlowLamports: 500_000_000n, // 0.50 SOL
  minRunupBps: 1500, // +15%
  maxRunupBps: 5000, // +50%
  minRealSolLamports: 2_000_000_000n, // 2.0 SOL
  maxRealSolLamports: 30_000_000_000n, // 30 SOL
  maxBigSellLamports: 500_000_000n, // 0.5 SOL
  skipCreatorSold: true,
  stakeLamports: 80_000_000n, // 0.08 SOL
  buySlippageBps: 2000,
  sellSlippageBps: 10000,
  takeProfitBps: 10000, // +100%
  stopLossBps: 1200, // -12%
  trailArmBps: 3000, // +30%
  trailBps: 2000, // 20% giveback from peak
  maxHoldSlots: 900,
  fadeNetSolLamports: 200_000_000n, // 0.20 SOL
  exitOnCreatorSell: true,
  bigSellExitSolLamports: 500_000_000n, // 0.5 SOL
  staleSlots: 120,
  paceSecs: 21600, // 6 hours of block time
  targetCoins: 25,
  targetFills: 240,
  probeStakeLamports: 10_000_000n, // 0.01 SOL, the guardrail minimum
  probeMinRealSolLamports: 1_500_000_000n, // 1.5 SOL
};

// ---------------------------------------------------------------- per-mint tape

interface TapeTrade {
  slot: number;
  isBuy: boolean;
  sol: bigint;
  user: string;
}

interface Tape {
  mint: string;
  creator: string;
  createdSlot: number | null;
  createVq: bigint; // virtual quote reserves at creation (price numerator)
  createVt: bigint; // virtual token reserves at creation (price denominator)
  complete: boolean;
  trades: number;
  buys: number;
  buySol: bigint;
  sellSol: bigint;
  buyers: Map<string, bigint>; // non-creator buyer -> buy SOL
  creatorSellSlot: number; // slot of the latest creator sell, -1 if none
  bigSellSlot: number; // slot of the latest >= maxBigSell sell, -1 if none
  lastTradeSlot: number;
  recent: TapeTrade[]; // trades kept for the 20-slot fade window
}

const CURVE_FEE_BPS = 125n; // 95 protocol + 30 creator, per leg
const MIN_BUY_LAMPORTS = 10_000_000n; // 0.01 SOL guardrail round-trip minimum
const MAX_ORDERS_PER_BATCH = 16;
const FADE_WINDOW_SLOTS = 20;

function newTape(mint: string, creator: string): Tape {
  return {
    mint,
    creator,
    createdSlot: null,
    createVq: 0n,
    createVt: 0n,
    complete: false,
    trades: 0,
    buys: 0,
    buySol: 0n,
    sellSol: 0n,
    buyers: new Map<string, bigint>(),
    creatorSellSlot: -1,
    bigSellSlot: -1,
    lastTradeSlot: -1,
    recent: [],
  };
}

/** Constant-product quote-out for selling `tokens`, net of curve fees and one tx fee. */
function netOutLamports(
  vq: bigint,
  vt: bigint,
  tokens: bigint,
  txFee: bigint,
): bigint {
  if (tokens <= 0n || vt <= 0n || vq <= 0n) return 0n;
  const quoteOut = (vq * tokens) / (vt + tokens);
  const fee = (quoteOut * CURVE_FEE_BPS) / 10_000n;
  const net = quoteOut - fee - txFee;
  return net > 0n ? net : 0n;
}

// ---------------------------------------------------------------- strategy

export function createStrategy(p: StrategyParams = DEFAULT_PARAMS): Bot {
  // Per-mint tapes, in first-seen order (insertion-ordered Map: deterministic).
  const tapes = new Map<string, Tape>();
  // Mints whose buy filled and that we later sold: never re-enter.
  const exited = new Set<string>();
  // Entry slot per held mint, set when its buy fill arrives.
  const entrySlots = new Map<string, number>();
  // Peak PnL (bps over cost) seen per held mint, for the trailing stop.
  const peakBps = new Map<string, bigint>();
  // Guardrail pacing: own filled buys.
  const filledCoins = new Set<string>();
  let filledCount = 0;
  // Probe fallback: at most one probe at a time.
  const probedMints = new Set<string>();
  let probeMint: string | null = null;
  let probeBuyPending = false;
  // Block-time window for pacing, from the batch stream only.
  let firstBlockTime: number | null = null;
  let lastBlockTime: number | null = null;

  function tapeFor(mint: string, creator: string): Tape {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = newTape(mint, creator);
      tapes.set(mint, t);
    }
    return t;
  }

  function ingest(batch: Batch): void {
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        const t = tapeFor(e.mint, e.creator);
        t.createdSlot = e.slot;
        t.creator = e.creator;
        t.createVq = BigInt(e.virtual_quote_reserves);
        t.createVt = BigInt(e.virtual_token_reserves);
      } else if (e.event === 'TradeEvent') {
        const t = tapeFor(e.mint, e.creator);
        // First sight through a trade: use its reserves as the creation-price proxy.
        if (t.createdSlot === null && t.trades === 0) {
          t.createVq = BigInt(e.virtual_quote_reserves);
          t.createVt = BigInt(e.virtual_token_reserves);
        }
        const sol = BigInt(e.sol_amount);
        t.trades += 1;
        t.lastTradeSlot = e.slot;
        if (e.is_buy) {
          t.buys += 1;
          t.buySol += sol;
          if (e.user !== e.creator) {
            t.buyers.set(e.user, (t.buyers.get(e.user) ?? 0n) + sol);
          }
        } else {
          t.sellSol += sol;
          if (e.user === e.creator) t.creatorSellSlot = e.slot;
          if (sol >= p.maxBigSellLamports) t.bigSellSlot = e.slot;
        }
        t.recent.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user });
      } else {
        // CompleteEvent: the curve is done; the harness closes positions in it.
        const t = tapeFor(e.mint, '');
        t.complete = true;
      }
      if (e.block_time !== null) {
        if (firstBlockTime === null) firstBlockTime = e.block_time;
        lastBlockTime = e.block_time;
      }
    }
  }

  function processFills(batch: Batch): void {
    for (const f of batch.fills) {
      if (f.side === 'buy' && f.status === 'filled') {
        filledCoins.add(f.mint);
        filledCount += 1;
        entrySlots.set(f.mint, f.slot);
        peakBps.delete(f.mint);
        if (f.mint === probeMint) probeBuyPending = false;
      } else if (f.side === 'buy' && f.status !== 'filled') {
        // Rejected or failed probe buy: the probe slot is free again.
        if (f.mint === probeMint) {
          probeMint = null;
          probeBuyPending = false;
        }
      } else if (f.side === 'sell' && f.status === 'filled') {
        entrySlots.delete(f.mint);
        peakBps.delete(f.mint);
        exited.add(f.mint);
        if (f.mint === probeMint) probeMint = null;
      }
    }
  }

  /** Net flow over the last FADE_WINDOW_SLOTS slots, in lamports (buys - sells). */
  function recentNetFlow(t: Tape, slot: number): bigint {
    let net = 0n;
    for (const tr of t.recent) {
      if (tr.slot <= slot - FADE_WINDOW_SLOTS) continue;
      net += tr.isBuy ? tr.sol : -tr.sol;
    }
    return net;
  }

  function qualifies(
    t: Tape,
    slot: number,
    realQuote: bigint,
    vq: bigint,
    vt: bigint,
  ): boolean {
    if (t.complete) return false;
    if (t.createdSlot === null) return false;
    const age = slot - t.createdSlot;
    if (age < p.minAge || age > p.maxAge) return false;
    if (t.trades < p.minTrades) return false;
    // Buy-dominated opening tape (by count).
    if (t.buys * 10_000 < p.minBuyShareBps * t.trades) return false;
    // More than just the creator buying their own coin.
    if (t.buyers.size < p.minBuyers) return false;
    // No single wallet doing most of the buying (bundled dev cluster).
    let topBuyer = 0n;
    for (const sol of t.buyers.values()) if (sol > topBuyer) topBuyer = sol;
    if (t.buySol <= 0n) return false;
    if ((topBuyer * 10_000n) / t.buySol > BigInt(p.maxBuyerShareBps)) return false;
    // Real money arriving, sized to a launch tape.
    if (t.buySol - t.sellSol < p.minNetFlowLamports) return false;
    // Moving up, but not already pumped.
    if (t.createVq <= 0n || t.createVt <= 0n || vq <= 0n || vt <= 0n) return false;
    const runupBps =
      ((vq * t.createVt - t.createVq * vt) * 10_000n) / (t.createVq * vt);
    if (runupBps < BigInt(p.minRunupBps) || runupBps > BigInt(p.maxRunupBps)) return false;
    // Fresh-launch reserve band.
    if (realQuote < p.minRealSolLamports || realQuote > p.maxRealSolLamports) return false;
    // Dump signals kill the entry outright.
    if (t.bigSellSlot >= 0) return false;
    if (p.skipCreatorSold && t.creatorSellSlot >= 0) return false;
    return true;
  }

  function onBatch(state: BotState, batch: Batch): OrderInput[] {
    const slot = batch.slot;
    const txFee = state.txFeeLamports();
    const orders: OrderInput[] = [];
    const selling = new Set<string>();

    ingest(batch);
    processFills(batch);

    // Prune stale tapes (keep held / pending mints and the entry window).
    for (const [mint, t] of tapes) {
      if (state.positions.has(mint)) continue;
      let pendingBuy = false;
      for (const po of state.pending.values()) {
        if (po.order.side === 'buy' && po.order.mint === mint) {
          pendingBuy = true;
          break;
        }
      }
      if (pendingBuy) continue;
      const age = t.createdSlot === null ? slot - t.lastTradeSlot : slot - t.createdSlot;
      if (age > p.maxAge + 650) tapes.delete(mint);
    }
    // Prune the fade window on surviving tapes.
    for (const t of tapes.values()) {
      while (t.recent.length > 0 && t.recent[0].slot <= slot - FADE_WINDOW_SLOTS - 5) {
        t.recent.shift();
      }
    }

    const pendingSell = new Set<string>();
    for (const po of state.pending.values()) {
      if (po.order.side === 'sell') pendingSell.add(po.order.mint);
    }

    // --- Exits first (whole position, first trigger wins) -----------------
    for (const [mint, holding] of state.positions) {
      if (orders.length >= MAX_ORDERS_PER_BATCH) break;
      if (pendingSell.has(mint)) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const t = tapes.get(mint);
      const net = netOutLamports(
        curve.virtual_quote_reserves,
        curve.virtual_token_reserves,
        holding.token_amount,
        txFee,
      );
      const cost = holding.cost_lamports;
      if (cost <= 0n) continue;
      const pnlBps = ((net - cost) * 10_000n) / cost;
      const entrySlot = entrySlots.get(mint);

      // Trailing-stop peak tracking.
      const prevPeak = peakBps.get(mint) ?? pnlBps;
      const peak = pnlBps > prevPeak ? pnlBps : prevPeak;
      peakBps.set(mint, peak);

      let exit = false;
      // Probe positions flip as soon as the buy fills: sell next batch.
      if (mint === probeMint) exit = true;
      // Take profit / stop loss.
      if (pnlBps >= BigInt(p.takeProfitBps)) exit = true;
      if (pnlBps <= -BigInt(p.stopLossBps)) exit = true;
      // Trailing stop, armed once up trailArmBps.
      if (peak >= BigInt(p.trailArmBps) && pnlBps <= peak - BigInt(p.trailBps)) exit = true;
      // Max hold.
      if (entrySlot !== undefined && slot - entrySlot >= p.maxHoldSlots) exit = true;
      if (t !== undefined) {
        // Tape turned: sellers dominate the last 20 slots.
        if (recentNetFlow(t, slot) < -p.fadeNetSolLamports) exit = true;
        // Creator selling while we hold.
        if (
          p.exitOnCreatorSell &&
          entrySlot !== undefined &&
          t.creatorSellSlot >= entrySlot
        ) {
          exit = true;
        }
        // Whale / dump sell since entry.
        if (entrySlot !== undefined && t.bigSellSlot >= entrySlot) exit = true;
        // Dead curve.
        if (slot - t.lastTradeSlot >= p.staleSlots) exit = true;
      }
      if (exit) {
        orders.push({
          side: 'sell',
          mint,
          token_amount: holding.token_amount,
          max_slippage_bps: p.sellSlippageBps,
        });
        selling.add(mint);
      }
    }

    // --- Entries: one fixed shot per mint, never re-enter after exit ------
    const freeSlots =
      state.hello.caps.max_open_positions - state.committedPositionCount();
    const buysAllowed = Math.max(0, freeSlots - orders.filter((o) => o.side === 'buy').length);
    if (buysAllowed > 0 && orders.length < MAX_ORDERS_PER_BATCH) {
      let buysSent = 0;
      for (const [mint, t] of tapes) {
        if (buysSent >= buysAllowed) break;
        if (orders.length >= MAX_ORDERS_PER_BATCH) break;
        if (state.positions.has(mint)) continue;
        if (selling.has(mint)) continue;
        if (exited.has(mint)) continue;
        if (probeMint !== null && mint === probeMint) continue;
        let pendingBuy = false;
        for (const po of state.pending.values()) {
          if (po.order.side === 'buy' && po.order.mint === mint) {
            pendingBuy = true;
            break;
          }
        }
        if (pendingBuy) continue;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (
          !qualifies(
            t,
            slot,
            curve.real_quote_reserves,
            curve.virtual_quote_reserves,
            curve.virtual_token_reserves,
          )
        ) {
          continue;
        }
        // Clip to the 1% real-reserve order cap; skip if below the minimum.
        const cap = state.maxBuyLamports(mint);
        let size = p.stakeLamports < cap ? p.stakeLamports : cap;
        if (size < MIN_BUY_LAMPORTS) continue;
        if (state.freeCashLamports() < size + txFee) continue;
        orders.push({
          side: 'buy',
          mint,
          quote_lamports: size,
          max_slippage_bps: p.buySlippageBps,
        });
        buysSent += 1;
      }
    }

    // --- Guardrail-pacing probe: one minimum-size round trip --------------
    // Fire only when the filled pace lags the prorated target, no probe is in
    // flight, and a position slot is free. Probes lose money by design (fee
    // drag ~4.6% of a 0.01 SOL stake); they are guardrail insurance only.
    const probeInFlight = probeMint !== null;
    if (!probeInFlight && orders.length < MAX_ORDERS_PER_BATCH) {
      let behindPace = false;
      if (firstBlockTime !== null && lastBlockTime !== null) {
        const elapsed = lastBlockTime - firstBlockTime;
        if (elapsed > 0) {
          const expectedCoins = Math.floor((p.targetCoins * elapsed) / p.paceSecs);
          const expectedFills = Math.floor((p.targetFills * elapsed) / p.paceSecs);
          behindPace =
            filledCoins.size < expectedCoins || filledCount < expectedFills;
        }
      }
      const slotsFree =
        state.committedPositionCount() +
          orders.filter((o) => o.side === 'buy').length <
        state.hello.caps.max_open_positions;
      if (behindPace && slotsFree) {
        // Hottest active coin: most recent trade, first-seen order breaks ties.
        let best: Tape | null = null;
        for (const t of tapes.values()) {
          if (t.complete) continue;
          if (t.createdSlot === null) continue;
          const age = slot - t.createdSlot;
          if (age < p.minAge || age > p.maxAge) continue;
          if (t.creatorSellSlot >= 0) continue;
          if (probedMints.has(t.mint)) continue;
          if (filledCoins.has(t.mint)) continue;
          if (exited.has(t.mint)) continue;
          if (state.positions.has(t.mint)) continue;
          if (selling.has(t.mint)) continue;
          let pendingBuy = false;
          for (const po of state.pending.values()) {
            if (po.order.side === 'buy' && po.order.mint === t.mint) {
              pendingBuy = true;
              break;
            }
          }
          if (pendingBuy) continue;
          const curve = state.curves.get(t.mint);
          if (curve === undefined || curve.complete) continue;
          if (curve.real_quote_reserves < p.probeMinRealSolLamports) continue;
          if (state.maxBuyLamports(t.mint) < p.probeStakeLamports) continue;
          if (best === null || t.lastTradeSlot > best.lastTradeSlot) best = t;
        }
        if (best !== null && state.freeCashLamports() >= p.probeStakeLamports + txFee) {
          orders.push({
            side: 'buy',
            mint: best.mint,
            quote_lamports: p.probeStakeLamports,
            max_slippage_bps: p.buySlippageBps,
          });
          probedMints.add(best.mint);
          probeMint = best.mint;
          probeBuyPending = true;
        }
      }
    }
    void probeBuyPending;
    void LAMPORTS_PER_SOL;

    return orders;
  }

  return { onBatch };
}
