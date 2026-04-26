# stockdata

## Dashboard

Run the dashboard backend:

```powershell
uv run uvicorn dashboard.main:app --host 127.0.0.1 --port 8000 --reload
```

Run the dashboard frontend from `src/dashboard/frontend`:

```powershell
npm.cmd install
npm.cmd run dev -- --port 5173 --strictPort
```

The dashboard is available at `http://127.0.0.1:5173/?realData=1`.

## Daily price robot

Run the price update flow with either of these commands:

```powershell
uv run python -m price_history.price_robot
```

```powershell
uv run price_robot
```

Both commands update per-asset CSV files in `data/prices` and regenerate `data/latest_prices.csv`.
