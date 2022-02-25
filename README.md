# optionskaro

Program written in Go to backtest a few strategies using the historical options data from "NF BNF options data (1min) backup" telegram group.

## Roadmap
- [ ] Ability to import data into a sqlite DB for querying
- [ ] Ability to build strategies as functions which are invoked on events
- [ ] Ability to define custom events that gets triggered when certain conditions are met
- [ ] Ability to map strategies and events so they can run
- [ ] Ability to run backtests using the historical data that's available
- [ ] Find a way to distribute this data in a more consumable format for easy querying later on
- [ ] Build an interface to run the strategy live against a broker implementation
  - [ ] Broker can be backtest / live paper / live testing / live production
  - [ ] We might need a way to version the strategies and also track metrics at each version to find which works best (think of MLOps but for trading)
