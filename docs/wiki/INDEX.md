# Knowledge Base

> What we've learned about Boston's 311 system — for contributors, AI assistants, and open source users.
> Each file is self-contained. Start here, read what's relevant.

## Reference (stable knowledge, rarely changes)

- [Open311 API Reference](open311-api-reference.md) — endpoints, rate limits, auth, extensions, field inventory
- [Service Code Mapping](service-code-mapping.md) — scraper slug→code table, input.* discovery, /services.json gaps
- [Data Quality Issues](data-quality-issues.md) — 14 systemic problems with Boston's 311 open data feed
- [Dev Setup & Deployment Gotchas](dev-setup.md) — dev-server env-loading trap, pipeline-deploy skip trap, local-run-is-prod-write warning

## Research (findings from analysis, evolves)

- [Ticket Routing](ticket-routing.md) — how app buttons map to stored types; litter, trash can, graffiti flows
- [SHARPS Team Analysis](sharps-team-analysis.md) — 6 workers identified from closure notes; shift patterns; syringe count parsing
- [Reporting Patterns](reporting-patterns.md) — peak hour analysis; citizen vs sweep theory (disproved); reporting time vs activity time
- [Other Ticket Black Hole](other-ticket-analysis.md) — 142k invisible "General Request" tickets; misrouted rodent/waste reports

## Open Questions

- Where does `input.Litter` route? Probably Street Cleaning catch-all — needs NLP on descriptions to confirm
- Where does `input.Overflowing Trash Can` route? Probably Empty Litter Basket — unconfirmed
- Graffiti: two departments (Property Mgmt vs PWD) — different areas? response times?
- Parks "Ground Maintenance" (3,902/yr) as separate litter silo with worse response times
- ~700 rodent reports trapped in "Other" — do they have structured form fields? (routing failure signal)
- Constituent Call hour patterns vs Citizens Connect App patterns
- Build sweep classifier for the few ambiguous tight-radius clusters
