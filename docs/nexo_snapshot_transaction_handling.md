# NEXO Snapshot Generation: Exact Transaction-Type Handling

This document specifies how `src/blockchain_reader/cex/nexo_snapshots.py` processes NEXO rows during snapshot generation.

- Input folder: `data/blockchain/transactions/cex/nexo/*.csv`
- Output: `data/blockchain/snapshots/cex/nexo/nexo_raw_snapshots.csv`
- Repayment review output: `data/blockchain/snapshots/cex/nexo/nexo_liquidation_only_review.csv`

The behavior below reflects the intended handling for the current supported NEXO `Type` values, documented on 2026-04-03.

## 1. Global processing flow

1. Load every CSV file in the NEXO transaction folder, concatenate them in filename order, and retain source-file order as a deterministic tie-break for identical timestamps.
1. Parse `Date / Time (UTC)` using day-first formats (`%d/%m/%Y %H:%M:%S`, `%d/%m/%Y %H:%M`, `%d/%m/%Y`, then day-first fallback).
1. Sort rows in ascending datetime order.
1. Build synthetic repayment swaps by pairing `Manual Sell Order` + `Manual Repayment`:
1. match window `±6 hours`
1. USD-equivalent tolerance:
1. default `<= $0.05`
1. `USDC` manual sells `<= $0.60`
1. for `Manual Repayment`, matching uses `USD Equivalent + Fee` when `Fee Currency` is `USD`
1. tie-break: nearest time, then lowest USD delta
1. Normalize each row into a single explicit action: `skip`, `reward`, `swap`, `receive`, or `send`.
1. Apply accounting in `CryptoTracker`:
1. `reward` -> `apply_reward_with_allocations(...)`
1. `swap` -> `_process_swap(...)`
1. `receive` -> `asset.receive(...)`
1. `send` -> `asset.send(...)`
1. Write snapshots only for touched coins; if a coin is updated multiple times on the same day, retain the latest daily state.
1. Write unmatched `Exchange Liquidation` rows to the review CSV for explicit approval.
1. Sort the final CSV and normalize dates to midnight (`YYYY-MM-DD 00:00:00`).

## 2. Type-driven normalization contract

### 2.1 Ignore rules

A row is skipped immediately when:

- `Type` (lowercased) is `transfer in`, `transfer out`, `locking term deposit`, or `unlocking term deposit`.

### 2.2 Amount and symbol parsing

- Symbols are parsed via `sanitize_symbol(...)`.
- Empty symbols and `-` are treated as missing symbols.
- Amounts are parsed as `Decimal`; parse failures become `0`.
- There is no generic row-shape fallback for unknown `Type` values.
- Every known NEXO `Type` is mapped explicitly.
- Unsupported `Type` values raise an error instead of being inferred from input/output signs.

### 2.3 Cashback special handling

If `Type == cashback`:

- action = `reward`
- allocations = `[(None, 0.5), ("xUSD", 0.5)]`

Accounting effect for reward value `V` (EUR):

- payout coin quantity: `+Q`
- payout coin principal: `+V - 0.5V = +0.5V`
- xUSD principal: `-0.5V`

### 2.4 Exchange Cashback special handling

If `Type == exchange cashback`:

- action = `reward`
- allocations = `[(None, 1.0)]`

Accounting effect for reward value `V` (EUR):

- payout coin quantity: `+Q`
- payout coin principal: `+V - 1.0V = 0`

### 2.5 Interest-family special handling

Interest-family types:

- `interest`
- `fixed term interest`
- `interest additional`

Handling logic:

- If interest is paid in coin X and details match patterns such as `approved / X Interest Earned`, allocate 100% of the interest to coin X (quantity increases; principal does not increase).
- If interest is paid in NEXO while details indicate a different underlying asset (for example `approved / 0.0001201 ETH`), allocate 75% to the underlying coin and 25% to NEXO.
- Allocation rule: `[(source_coin, 0.75), (reward_token, 0.25)]`

Accounting effect for reward value `V` (EUR):

- reward coin quantity: `+Q`
- reward coin principal: `+V - 0.25V = +0.75V`
- inferred source-coin principal (ETH in the example): `-0.75V`

If the amount is negative and the coin is one of `USD` / `xUSD` / `USDX`:

- Treat all three as a single USD bucket (reflecting NEXO naming differences over time).
- Quantity decreases, principal stays unchanged.

### 2.8 Card-cycle special handling

Card-related rows are normalized as follows:

- `credit card withdrawal credit` -> always `skip`
- `manual sell order` + `manual repayment` -> paired when within `±6 hours`; USD-equivalent delta uses:
  - default tolerance `<= $0.05`
  - `USDC` sell tolerance `<= $0.60`
  - for matching only, `manual repayment` uses `USD Equivalent + Fee` when a positive USD fee is present
  - paired rows are applied as one synthetic `swap`
  - out leg from `manual sell order`
  - in leg from `manual repayment` (debt stable canonicalized to `USD`)
  - principal follows standard `swap` math
- unpaired `manual sell order` and unpaired `manual repayment` -> `skip`
- `exchange credit` with `Details` containing `Nexo Card Loan Withdrawal` -> `skip` (deduplicates a card-debt leg already represented by `nexo card purchase`)
- rejected `nexo card purchase` -> `skip`
- approved `nexo card purchase` enters debit-mode companion handling when there is a same-timestamp match on EUR amount with both:
  - `credit card fiatx exchange to withdraw`
  - `withdraw exchanged`
  - in that case the purchase row itself is `skip`
  - the economic effect is carried only by the `credit card fiatx exchange to withdraw` send of `EURX`
- approved `nexo card purchase` -> forced `send` using negative-leg extraction from the input debt token (no EUR inflow is recorded)
- rejected `nexo card refund` -> `skip`
- approved `nexo card refund` with `EUR -> EUR` legs -> temporary `skip`
  - note: these rows appear to represent fiat being returned outside the tracked crypto/debt buckets
  - in your ledger they can later re-enter the portfolio through `deposit to exchange` / `exchange deposited on`
  - counting them as `USD` would inflate debt repayment and counting them as holdings directly would risk double-counting
- approved `nexo card refund` -> dedicated `receive` into inferred debt token:
  - quantity from `USD Equivalent` (with stable-leg fallback)
  - token inferred from linked merchant purchase when available; otherwise stable-token fallback
  - principal anchored through principal-override logic
- rejected `nexo card cashback reversal` -> `skip`
- approved `nexo card cashback reversal` in an EUR refund redeposit bundle -> `skip`
  - bundle detector: same timestamp contains both:
    - `deposit to exchange` with `EUR -> EURX`
    - `exchange deposited on` with `EUR -> EURX`
  - rationale: the EUR-side redeposit already captures the net refund that re-enters the portfolio as `EURX`
- approved `nexo card cashback reversal` -> dedicated `send` from inferred debt token:
  - quantity from `USD Equivalent` (with stable-leg fallback)
  - principal override on debt token
  - principal addition to `NEXO` with zero quantity change
- `exchange liquidation` -> skipped in production during the repayment-unification phase:
  - rows matched to a synthetic manual pair are deduped
  - unmatched rows are exported to `nexo_liquidation_only_review.csv` for approval
- `exchange deposited on` -> forced `receive` of `EURX` using the absolute `EUR` input amount
  - the `Output Amount` field is ignored for quantity sizing
  - this preserves top-ups even when NEXO exports `0 EURX`
- `credit card fiatx exchange to withdraw` -> forced `send` of the input asset (for example `EURX`); the output `EUR` leg is ignored for holdings
- `exchange to withdraw` -> forced `send` of the input asset (for example `EURX`); the output `EUR` leg is ignored for holdings
- `loan withdrawal` -> forced `send` of the debt token only:
  - input debt token quantity increases the negative USD debt balance
  - the received asset quantity is expected to be represented by a separate `top up crypto` row
- `deposit over repayment` -> forced `send` of the positive input debt token
  - NEXO emits these rows when an over-repayment is returned or compensated
  - the paired `manual repayment` amount already includes the excess, so treating this as a receive would double-count the debt reduction
- `withdraw exchanged` -> always `skip` (withdrawal settlement leg)
- `nexo card transaction fee` -> forced `send` using negative-leg extraction (same helper used for withdrawals)

## 3. Generic accounting math (non-reward actions)

### 3.1 `receive`

For each inflow leg `(coin, q)`:

- quantity `+= q`
- principal `+= q * price_eur(coin, date)`

### 3.2 `send`

For each outflow leg `(coin, q)`:

- quantity `-= q`
- principal `-= q * price_eur(coin, date)`

### 3.3 `swap`

Given `ins` and `outs`:

1. Compute EUR value for each in-leg and out-leg using tx-date prices.
1. `total_in_value = sum(in_leg_values)`.
1. Reduce each out-coin principal by its proportional share of `total_in_value`:
   - `share_out_i = out_leg_value_i / total_out_value`
   - `principal_reduction_out_i = total_in_value * share_out_i`
1. Update quantities:
   - add all in quantities
   - subtract all out quantities

Fallback when `total_out_value == 0`:

- apply equal weighting across out legs.

### 3.4 Price source and fallback

`get_crypto_price(...)` (EUR valuation):

- use price on or before the date, if available
- else use oldest known price
- else use `0`

### 3.5 Principal overrides and principal additions

Certain card flows apply explicit principal mechanics instead of the default price-based `receive`/`send` principal calculation:

- `principal_overrides` replaces default principal movement for specific tokens on a row.
- `principal_additions` applies extra principal movement to a token without changing that token's quantity.

Current card applications:

- approved `nexo card purchase`: debt-token principal is overridden using the EUR notional.
- approved `nexo card refund`: debt-token principal is overridden using the EUR notional.
- approved `nexo card cashback reversal`: debt-token principal is overridden and an additional principal increment is applied to `NEXO` without quantity increase.

## 4. Exact per-`Type` handling

| Type (exact text) | Exact handling path(s) |
|---|---|
| Assimilation | `receive:positive_input_credit` |
| Bonus | `reward:free_input_reward` |
| Cashback | `reward:cashback_50_50` |
| Credit Card Fiatx Exchange To Withdraw | `send:withdrawal_rule` |
| Credit Card Withdrawal Credit | `skip:card_rule` |
| Deposit Over Repayment | `send:over_repayment_return_rule` |
| Deposit To Exchange | `skip:explicit_rule` |
| Dividend | `reward:free_input_reward` |
| Exchange | `swap:input_output_exchange` |
| Exchange Cashback | `reward:exchange_cashback_free` |
| Exchange Credit | `skip:card_loan_withdrawal_rule` |
| Exchange Deposited On | `receive:deposit_credit_rule` (quantity sourced from absolute EUR input amount) |
| Exchange Liquidation | `skip:repayment_unification_phase` (unmatched rows exported to review CSV) |
| Exchange To Withdraw | `send:withdrawal_rule` |
| Fixed Term Interest | `reward:interest_75_25` |
| Interest | `reward:interest_75_25`, `send:negative_stable_interest`, or `skip:empty_interest_row` |
| Interest Additional | `send:negative_stable_interest` |
| Loan Withdrawal | `send:loan_withdrawal_debt_only` |
| Locking Term Deposit | `skip:ignore_term_deposit_type` |
| Manual Repayment | `pair:synthetic_swap_when_matched`, else `skip` |
| Manual Sell Order | `pair:synthetic_swap_when_matched`, else `skip` |
| Nexo Card Cashback Reversal | `send:cashback_reversal_rule`, `skip:eur_refund_redeposit_rule`, or `skip:rejected_rule` |
| Nexo Card Purchase | `send:card_purchase_rule`, `skip:debit_mode_companion_rule`, or `skip:rejected_rule` |
| Nexo Card Refund | `receive:card_refund_rule`, `skip:eur_mode_refund_rule`, or `skip:rejected_rule` |
| Nexo Card Transaction Fee | `send:card_fee_rule` |
| Referral Bonus | `reward:free_input_reward` |
| Top up Crypto | `receive:positive_input_credit` |
| Transfer In | `skip:ignore_internal_wallet_transfer` |
| Transfer Out | `skip:ignore_internal_wallet_transfer` |
| Unlocking Term Deposit | `skip:ignore_term_deposit_type` |
| Withdraw Exchanged | `skip:withdrawal_settlement_rule` |
| Withdrawal | `send:withdrawal_special_case` |

## 5. Important exact edge behaviors

- `Type` names are hard-mapped for:
  - ignore set (`locking term deposit`, `unlocking term deposit`)
  - card-cycle overrides (`credit card withdrawal credit`, `nexo card purchase`, `exchange liquidation`, `nexo card transaction fee`)
  - `cashback`
  - `exchange cashback`
  - interest-family (`interest`, `fixed term interest`, `interest additional`)
- Manual repayment-cycle matching is global (pairing pass before row-level normalization), not a single-row mapping rule.
- Receive-like, send-like, reward-like, and swap-like rows are all routed through explicit `Type` handlers.
- Internal wallet transfer detection is detail-regex based and independent of `Type`.
- Rows with zero/effectively empty movement are silently skipped.
- Snapshots are day-level; intra-day updates keep only the final state per date+coin.
