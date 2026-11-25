// Tape Rider: a pump.fun paper-trading bot for the agent-fun/replay/v2 harness.
// Buys launches whose opening tape looks clean and coins that already proved demand,
// then lets winners run under a trailing stop. See strategy.ts for the reasoning.
// Run by the grader as `node /bot/main.ts` with this directory mounted at /bot.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

await runBot(createStrategy(DEFAULT_PARAMS));
