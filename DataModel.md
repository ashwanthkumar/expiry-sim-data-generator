## Data Model

We would roughly need 2 tables to hold all the data in our SQLite.

### instrument table
- symbol (indexed)
- expiry (indexed)
- strike (indexed)
- lot_size
- instrument_type (CE/PE/FUT)

### data table
- instrument_id (indexed - foriegn-key)
- timestamp
- open
- high
- low
- close

Example JSON Response output:

```
// This is an instrument
{
  "symbol": "22NIFTY30316000CE",
  "expiry": "2021-03-03",
  "data": [
    ["timestamp", "open", "high", "low", "close"],
    ....
  ]
}
```