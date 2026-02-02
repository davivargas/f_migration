# Next-Day Migration Validator

A lightweight prototype that simulates a **“next-day” finance data migration** by ingesting,
validating, and analyzing accounting exports.

The focus is correctness, speed, and clear risk signaling, not polish or scale.

---

## Why this exists

Fast finance migrations are risky. When accounting data is moved quickly, issues like missing
references, currency mismatches, malformed records, and anomalous values can silently break
downstream systems.

This project explores the **failure modes that appear when migrating financial data under
tight timelines**, inspired by AI-native accounting and ERP platforms that prioritize rapid
go-live.

---

## What it does

The validator ingests CSV exports and performs:

- **Schema validation**
  - Missing required fields
  - Invalid or malformed values
- **Referential integrity checks**
  - Transactions referencing non-existent accounts
  - Duplicate identifiers
- **Financial sanity checks**
  - Currency mismatches
  - Zero or unusually large transaction amounts
  - Transactions dated in the future
- **Basic anomaly detection**
  - Simple statistical outlier flagging (non-destructive)

All findings are summarized in a **clear migration report** that highlights potential risks.
