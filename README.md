# stockdata

## Daily price robot

Run the price update flow with either of these commands:

```powershell
uv run python -m price_history.price_robot
```

```powershell
uv run price_robot
```

Both commands update per-asset CSV files in `data/prices` and regenerate `data/latest_prices.csv`.
