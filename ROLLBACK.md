# EMERGENCY ROLLBACK GUIDE

Last updated: 2026-01-02

If something breaks in live trading after recent fixes:

Rollback to previous version:
git checkout v1.0-pre-audit

Return to current version:
git checkout main

What changed recently:
- Retry logic for API calls
- Atomic JSON writes
- Duplicate order protection
- Safer order verification
- Reduced API polling

If unsure:
Stop the bot (Ctrl+C) and check Kite manually.