// First-Trade Sniper: a pump.fun paper-trading bot for the agent-fun/replay/v2 harness.
//
// What the dev window actually shows (measured on the kit's 24h dataset, not assumed):
// - The create slot is where a coin's price jumps: the creator and the first buyers push
//   real SOL in at once. By the time a 2-slot-latency order can fill, the easy pop is gone
//   and the average coin drifts down. Buying every fresh coin and holding it loses, whatever
//   the exit; buying an already-running coin and riding it loses more, because the runs are
//   spiky and the pullbacks shake a trailing stop out before the coin ever completes.
// - The least-bad edge is the very first trade: a launch whose creator put in only a little
//   of their own SOL (a small devBuy means the creator is not about to dump a big bag) and
//   that almost nobody else has bought yet (at most a couple of distinct non-creator buyers),
//   bought the moment its real SOL reserve is big enough for a meaningful order, and sold
//   within a few slots on a small take-profit, a stop loss, or a hard time limit. It is a
//   breakeven-ish grind, not a home-run swing: tiny stakes, fast turnover, and a hard cap on
//   how much any one coin can lose. The guardrail needs 20+ distinct coins, so the filter
//   stays loose enough to cover well over a hundred coins a day.
// - Deterministic by construction: no clock, no randomness, iteration only over
//   insertion-ordered maps fed by the batch stream. Amounts are bigint throughout.

import { BPS, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Oldest age (slots since create) at which a first buy may still be decided. */
  maxAge: number;
  /** SOL to aim for per coin (each reply is still capped at 1% of real SOL). */
  stakeSol: number;
  /** Smallest single buy worth paying the tx fee for (SOL). */
  minBuySol: number;
  /** Real SOL reserve window at decision time (SOL). */
  minRqSol: number;
  maxRqSol: number;
  /** Creator's own buys in the create slot, at most (SOL): a big creator buy is a dump in waiting. */
  maxDevBuySol: number;
  /** Distinct non-creator buyers seen before entry, at most: a launch already crowded is late. */
  maxBuyers: number;
  /** Price now over the create price, at most (x). */
  maxRunup: number;
  /** Skip the coin when the creator sold before entry. */
  skipDevSold: boolean;
  /** Slippage allowed on buys (bps). */
  buySlippageBps: number;
  /** Exit when the position is worth this much over its cost (x). */
  tpX: number;
  /** Exit when it is worth this much under its cost (x). */
  slX: number;
  /** Exit after holding this many slots. */
  maxHold: number;
}

export const DEFAULT_PARAMS: Params = {
  maxAge: 3,
  stakeSol: 0.02,
  minBuySol: 0.005,
  minRqSol: 0.5,
  maxRqSol: 30,
  maxDevBuySol: 0.75,
  maxBuyers: 2,
  maxRunup: 6,
  skipDevSold: true,
  buySlippageBps: 10_000,
  tpX: 0.2,
  slX: 0.12,
  maxHold: 20,
};

const FEE_BPS = 125n;

/** What selling `tokens` against (vq, vt) would pay after curve fees (95 + 30 bps). */
function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * FEE_BPS + BPS - 1n) / BPS;
}

interface Coin {
  createSlot: number;
  creator: Pubkey;
  vq0: bigint;
  vt0: bigint;
  devBuy: number;
  buyers: Set<Pubkey>;
  devSold: boolean;
  done: boolean;
}

interface Held {
  openedSlot: number;
  txPaid: bigint;
}

export function createStrategy(p: Params = DEFAULT_PARAMS) {
  const coins = new Map<Pubkey, Coin>();
  const held = new Map<Pubkey, Held>();
  const selling = new Set<Pubkey>();
  const stake = BigInt(Math.floor(p.stakeSol * 1e9));
  const minBuy = BigInt(Math.floor(p.minBuySol * 1e9));

  const coinOf = (mint: Pubkey): Coin => {
    let c = coins.get(mint);
    if (c === undefined) {
      c = {
        createSlot: -1, creator: '', vq0: 0n, vt0: 1n, devBuy: 0, buyers: new Set(),
        devSold: false, done: false,
      };
      coins.set(mint, c);
    }
    return c;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      for (const e of batch.events) {
        if (e.event === 'CreateEvent') {
          const c = coinOf(e.mint);
          c.createSlot = e.slot;
          c.creator = e.creator;
          c.vq0 = BigInt(e.virtual_quote_reserves);
          c.vt0 = BigInt(e.virtual_token_reserves);
        } else if (e.event === 'TradeEvent') {
          const c = coins.get(e.mint);
          if (c === undefined) continue;
          const sol = Number(e.sol_amount) / 1e9;
          const isDev = e.user === c.creator;
          if (e.is_buy) {
            if (isDev && e.slot === c.createSlot) c.devBuy += sol;
            else if (!isDev) c.buyers.add(e.user);
          } else if (isDev) {
            c.devSold = true;
          }
        } else {
          const c = coins.get(e.mint);
          if (c !== undefined) c.done = true;
        }
      }

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      const tx = state.txFeeLamports();
      const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
      for (const m of batch.marks) marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });

      for (const f of batch.fills) {
        if (f.client_id.startsWith('_')) {
          held.delete(f.mint);
          selling.delete(f.mint);
          const c = coins.get(f.mint);
          if (c !== undefined) c.done = true;
          continue;
        }
        const h = held.get(f.mint);
        if (f.status !== 'rejected' && h !== undefined) h.txPaid += BigInt(f.tx_fee_lamports);
        if (f.side === 'buy' && f.status === 'filled' && h === undefined) {
          held.set(f.mint, { openedSlot: f.slot, txPaid: BigInt(f.tx_fee_lamports) });
        }
      }
      for (const mint of [...selling]) {
        let pend = false;
        for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'sell') pend = true;
        if (!pend) selling.delete(mint);
      }
      for (const mint of [...held.keys()]) {
        if (!state.positions.has(mint) && !selling.has(mint)) {
          let pendBuy = false;
          for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'buy') pendBuy = true;
          if (!pendBuy) held.delete(mint);
        }
      }

      // Exits first: take-profit, stop loss, or the hard time limit.
      for (const [mint, pos] of state.positions) {
        if (orders.length >= cap) break;
        if (selling.has(mint)) continue;
        let h = held.get(mint);
        if (h === undefined) {
          h = { openedSlot: now, txPaid: tx };
          held.set(mint, h);
        }
        let mk = marks.get(mint);
        if (mk === undefined) {
          const cv = state.curves.get(mint);
          if (cv === undefined) continue;
          mk = { vq: cv.virtual_quote_reserves, vt: cv.virtual_token_reserves };
        }
        const value = sellValue(pos.token_amount, mk.vq, mk.vt) - tx;
        const basis = pos.cost_lamports + h.txPaid;
        let exit = false;
        if (basis > 0n) {
          if (value >= (basis * BigInt(Math.floor((1 + p.tpX) * 1000))) / 1000n) exit = true;
          if (value <= (basis * BigInt(Math.floor((1 - p.slX) * 1000))) / 1000n) exit = true;
        }
        if (now - h.openedSlot >= p.maxHold) exit = true;
        if (!exit) continue;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
        selling.add(mint);
        const c = coins.get(mint);
        if (c !== undefined) c.done = true;
      }

      // Entries: one small buy per clean launch, at most.
      let free = state.freeCashLamports() - tx * BigInt(orders.length);
      let open = state.committedPositionCount();
      for (const [mint, c] of coins) {
        if (orders.length >= cap) break;
        if (c.done || c.createSlot < 0) continue;
        const age = now - c.createSlot;
        if (age > p.maxAge) { c.done = true; continue; }
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (state.positions.has(mint)) continue;
        let pend = false;
        for (const po of state.pending.values()) if (po.order.mint === mint) pend = true;
        if (pend || selling.has(mint)) continue;
        if (c.devSold && p.skipDevSold) { c.done = true; continue; }
        const rqSol = Number(curve.real_quote_reserves) / 1e9;
        if (rqSol < p.minRqSol || rqSol > p.maxRqSol) continue;
        if (c.devBuy > p.maxDevBuySol) { c.done = true; continue; }
        if (c.buyers.size > p.maxBuyers) { c.done = true; continue; }
        const price = Number(curve.virtual_quote_reserves) / Number(curve.virtual_token_reserves);
        const createPrice = Number(c.vq0) / Number(c.vt0);
        if (createPrice > 0 && price / createPrice > p.maxRunup) { c.done = true; continue; }
        if (open >= state.hello.caps.max_open_positions) continue;
        const maxBuy = state.maxBuyLamports(mint);
        const amount = stake < maxBuy ? stake : maxBuy;
        if (amount < minBuy) continue;
        if (free < amount + tx) continue;
        orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: p.buySlippageBps });
        free -= amount + tx;
        c.done = true;
        open += 1;
      }
      return orders;
    },
  };
}
