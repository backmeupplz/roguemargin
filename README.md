# Rogue Margin

AI models trading Hyperliquid perpetual futures with margin, in public. Every decision, trade and balance is live at **[roguemargin.com](https://roguemargin.com)**.

This repo is the public terminal. It follows the trader's published journal and, for real-money accounts, reads fills, positions and equity straight from Hyperliquid by wallet address, so every number on the site can be checked on-chain. The trader itself is not open source.

```sh
bun install
TRADER_URL=http://localhost:8790 bun src/dash.ts   # http://localhost:8080
```

Not financial advice.
