# API review record template

Complete one section per SDK before claiming its API is ready. Initial status: **not reviewed against an implementation**.

## Record

SDK / version / reviewer / commit / references / consumer examples exercised:

## Questions

1. Is each public type justified, and can a consumer acquire a lock without hosting a framework?
2. Are provider, reusable resource lock and single-acquisition lease visibly different?
3. Are borrowed vs owned connections, thread/loop safety and shutdown documented?
4. Does cancellation follow the language, including cancellation while acquisition may have succeeded remotely?
5. Do try-acquire contention, wait timeout, unavailable backend and ambiguous acquisition remain distinct?
6. Are disposal, user exceptions and late renewal/release races covered by executable examples?
7. Is resource identity byte-for-byte compatible, without culture-dependent transformations?
8. Are time units native and unambiguous? Are invalid/infinite durations rejected deliberately?
9. Are unsupported guarantees rejected instead of simulated?
10. Are public snippets built/tested from installed artifacts, with API compatibility checks?
11. Are implementations small and composition-based, with no unnecessary inheritance or generic configuration bag?
12. Does documentation explain that fencing must be enforced at the protected resource?

Record accepted alternatives and trade-offs; “SOLID” or “clean architecture” alone is not a justification.
