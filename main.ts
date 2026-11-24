// Pump.fun Paper Sprint: Any Bot Can Win
// Entry for competition u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b
// Fresh-coin sniper: buys very young bonding curves on strong early buy flow
// and flips them fast on take-profit, stop-loss and trailing-stop exits.
// Run with: node main.ts

import { runBot } from './vendor/makefun-bot.ts';
import { DEFAULT_PARAMS, createStrategy } from './strategy.ts';

await runBot(createStrategy(DEFAULT_PARAMS));
