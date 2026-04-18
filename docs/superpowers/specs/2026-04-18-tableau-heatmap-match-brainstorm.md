# Tableau Heatmap Match — Brainstorming (in progress)

**Status:** paused mid-brainstorm, resume next session.
**Date started:** 2026-04-18

## Problem

Our heatmap on urbanhazardmaps.com looks meaningfully different from Andy Brand's
Tableau Public sharps heatmap, on the same Boston 311 sharps dataset. We've tried
nine render presets (including three "tableau-*" presets in `HeatMap.tsx`) and
none of them match. We can't figure out why. Tableau also makes it easier to see
hot spots shift over time, which we want.

- Tableau reference: https://public.tableau.com/app/profile/andy.brand/viz/Boston311NeedleData/HeatMapDashboard
- Our implementation: `frontend/src/components/HeatMap.tsx`, using `leaflet.heat`
  with params in `getHeatParams()` / `createHeatLayer()`.

## End goal

Figure out what Tableau is doing differently — either match their rendering or
add it as an explicit option in our render-method dropdown so users can choose.

## Context already in the codebase

- 9 existing `HeatMethod` presets, three named "tableau", "tableau-wide",
  "tableau-soft" — all using `leaflet.heat`'s `radius`, `blur`, `maxZoom`,
  `minOpacity` knobs. None match.
- Slider now scales radius exponentially 0.04x–0.2x (committed `ce3bb5e`).
- Zoom-adaptive scaling is in place (`getHeatParams` scales by zoom level).

## Open questions for next session

Answer in any order; **Q1 and Q2 are the blockers** — everything downstream
depends on them.

1. **What specifically looks different about Tableau's heatmap vs. ours?**
   Pick the closest match (or describe):
   - **A.** Shape — their blobs are smoother / rounder / more organic; ours look patchy or clumped
   - **B.** Spread — their hotspots fade gradually over many blocks; ours cut off too sharply at the edges
   - **C.** Color/intensity — their gradient or peak color is different; hotspot reds vs. cooler ramps
   - **D.** Concentration — same data points look denser / more concentrated in theirs; ours look diffuse
   - **E.** Motion / time behavior — their patterns animate smoothly between periods; ours pop / shift jarringly
   - **F.** All of the above — it's a general vibe difference I can't put a finger on

2. **Data source check:** is Tableau's viz definitely reading the same `boston.gov` CKAN sharps feed, or could Andy have pre-filtered / aggregated differently (e.g., binned to block centroids, dedup'd on same-location tickets, only shown closed tickets, etc.)?

3. **Preferred outcome shape:**
   - **A.** Replace our default render method with a new "Tableau-match" preset
   - **B.** Add Tableau-match as an additional dropdown option and leave existing defaults alone
   - **C.** Make the existing "tableau" preset actually match, deprecate the other two

4. **Investigation bandwidth:** OK with me running headless-browser side-by-side comparisons at matched time periods/zoom levels (takes real time and iteration), or do you want a lighter-touch first pass (Brian compares manually, sends me a screenshot + description)?

5. **Time-over-time UX is called out as a separate Tableau advantage.** Is that
   in scope for this work, or a future phase? (It interacts with the time
   scrubber we already have — may or may not want to touch.)

## Investigation approach candidates (to pick after Q1)

Hold for next session, but rough options on the table:

- **Reverse-engineer Tableau's renderer.** Tableau Public renders via canvas; we can scrape the rendered tiles / inspect the viz config JSON. More effort, higher signal.
- **Empirical parameter sweep.** Render our map with same data + time window at many `radius`/`blur`/`minOpacity`/gradient combinations, compare visually to Tableau screenshots at matched zoom. Less effort, might miss if Tableau is doing something structurally different (e.g., KDE vs gaussian, log intensity scaling, pre-binning).
- **Switch libraries.** `leaflet.heat` is fixed-kernel gaussian. Alternatives like `Leaflet.SimpleHeat`, `deck.gl HeatmapLayer`, or d3-contour-based hexbin might be closer to what Tableau actually does. Biggest effort, could be the real answer.

## Resume instructions

When you pick this up next session:
1. Read this file.
2. Start by asking Q1 (what specifically looks different), then Q2 (data source
   check). Everything else is downstream of those.
3. Once Q1/Q2 answered, propose 2-3 approaches scoped to the answer, pick one,
   and transition to `writing-plans` skill.
