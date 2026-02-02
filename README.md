# Financial Migration Validator

This project is a **simple but practical system to evaluate the quality and risk of financial datasets before a migration**.

Financial migrations usually fail not because the code crashes, but because the data is subtly wrong:
missing identifiers, duplicated records, broken references, inconsistent currencies, or extreme values that look valid but are not.

The goal of this project is to **surface those problems early**, in a clear and inspectable way, so a migration decision can be made with context instead of assumptions.

This is not a full migration tool.  
It is a **pre-migration evaluation layer**.

---

## What the system does

Given a financial dataset, the system:

1. Loads the data through a **format-specific adapter**
2. Normalizes it into a **small internal schema**
3. Runs a set of **data quality validations**
4. Optionally performs **anomaly detection on transaction amounts**
5. Produces:
   - a concise CLI report
   - an optional JSON report
   - a migration risk level (LOW / MEDIUM / HIGH)

The output is designed to be readable by humans and usable in automation.

---

## Why this matters

In real systems, migrations often happen under pressure:
deadlines, parallel systems, partial knowledge of legacy data.

A tool like this helps answer basic but critical questions:

- Are there duplicated identifiers?
- Are transactions referencing accounts that do not exist?
- Are there silent currency mismatches?
- Are there dates that make no sense?
- Are there values that deserve manual inspection?

Even a simple validator can prevent costly mistakes.

---

## Supported data sources

The system uses adapters so that validation logic stays the same while input formats change.

Currently supported:

### Synthetic / mock datasets

Used for stress testing and validation logic:

- duplicated IDs
- missing references
- future-dated transactions
- zero or extreme values

### Kaggle accounting dataset

A realistic but structured accounting export, used to test behavior on real-world data.

### Canadian government general ledger data

Receiver General of Canada public accounting transactions:

- bilingual schema
- missing or weak identifiers
- debit / credit sign logic
- very large transaction amounts

Each dataset highlights different failure modes.

---

## High-level architecture

Raw dataset
↓
Format adapter
↓
Canonical internal schema
↓
Validation rules
↓
Anomaly detection (optional)
↓
Risk assessment + report

Adapters deal with format-specific complexity.  
Validation rules are format-agnostic.

---

## Internal schema (simplified)

The system works with three core tables:

- **accounts**
  - account_id
  - currency
  - metadata

- **transactions**
  - transaction_id
  - account_id
  - amount
  - currency
  - date

- **vendors** (optional)

Adapters are responsible for mapping raw data into this schema.

---

## Validation rules

The current validation layer checks for:

### Structural and referential issues

- Duplicate account IDs
- Duplicate transaction IDs
- Transactions referencing non-existent accounts

### Temporal issues

- Transactions dated in the future
- Unparseable or invalid dates

### Monetary issues

- Zero-amount transactions
- Currency mismatches between accounts and transactions

### Anomaly detection

- Modified Z-score (MAD-based) outlier detection
- Deterministic “Top N largest absolute amounts” for manual review

The anomaly output is intentionally inspectable, not just a score.

---

## Example CLI output

Cleaning Stats

Rows in: 14500
Rows out: 14500
Bad dates: 0
Bad amounts: 0
Bad credit/debit codes: 0
Rows bucketed to UNKNOWN account: 14343
Fallback transaction IDs used: 14352

Migration Summary

Accounts processed: 70
Transactions processed: 14500

Issues detected:

Top 50 largest absolute transaction amounts (50)
example: transaction_id=gov_row_8105, amount=-46614033473.18
example: transaction_id=gov_row_3639, amount=-38870586006.48

Migration risk level: MEDIUM

The report favors **clarity over completeness**:
counts first, then a small number of concrete examples.

---

## JSON output

The same evaluation can be exported as JSON for automation or further analysis.

py -m src.main
--format gov_canada_gl
--input data/real_datasets/receiver_general_accounting_transactions_canada.csv
--currency CAD
--top-outliers 50
--json out/report.json

The JSON includes:

- record counts
- cleaning metrics
- validation issues with examples
- anomaly details
- final risk level

---

## Exit codes

The program exits with a code suitable for scripts or CI:

| Code | Meaning                                 |
| ---- | --------------------------------------- |
| 0    | LOW risk – safe to migrate              |
| 2    | MEDIUM risk – manual review recommended |
| 5    | HIGH risk – migration should be blocked |

---

## Testing approach

The test suite focuses on **evaluation correctness**, not just code coverage.

It includes:

- deterministic mock datasets with known expected outcomes
- independent verification of anomaly detection results
- regression tests against real datasets

py -m pytest -q

---

## Limitations and future work

This is intentionally a **small system**.

Current limitations:

- rules are generic, not domain-specific
- no historical baselines
- no cross-period consistency checks
- no learning-based anomaly detection

Potential improvements:

- account-level anomaly detection
- schema versioning
- dataset profiling
- integration with ingestion pipelines

---

## Summary

This project is a focused attempt to treat financial migrations as a **data quality and risk problem**, not just a data movement problem.

It is simple, but grounded in real datasets and real failure modes.
