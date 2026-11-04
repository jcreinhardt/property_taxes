# Overlap and disagreements across the slide-design sources

Sources: GP = Goldsmith-Pinkham beamer-tips; SJ = Schwabish JEP 2014; SB = Schwabish *Better Presentations*;
AL = Alley assertion-evidence; TU = Tufte *Cognitive Style of PowerPoint*; DO = Doumont *Trees, Maps, and Theorems*;
KB = Butts template; MT = metropolis theme/manual; CT = current `presentation.tex`.
✓ = states the rule; (✓) = implied or partial; ✗ = contradicts; – = silent.

| Rule | GP | SJ | SB | AL | TU | DO | KB | MT | CT |
|---|---|---|---|---|---|---|---|---|---|
| One message per slide; make one point well | ✓ | – | ✓ | ✓ | ✗ (density) | ✓ | – | (✓ standout) | ✗ |
| Title is a full-sentence message, ≤ 2 lines | – | – | (✓ "active title") | ✓ 28pt | (✓ sentences) | ✓ ≤12 words | ✗ phrases | – | (✓) partial |
| Evidence is a figure/diagram, not bullets; minimise text | (✓) | ✓ | ✓ | ✓ | ✓ | ✓ | – | – | ✗ |
| Few colours: 1 base + 1–2 accents, consistent, colour-blind safe | ✓ 2–3, Okabe–Ito | ✓ ColorBrewer | ✓ consistent | – | – | ✓ 1 accent | ✗ 9 + 2 | ✓ 3 roles, Tol | (✓) |
| Large type; never shrink to fit; enlarge figure fonts | ✓ | (✓) | ✓ billboard | ✓ 18–28pt | ✗ (low-res) | ✓ | – | (✓ heavier Fira) | ✗ 10pt |
| Sans-serif face for projection | ✓ Lato | ✗ avoid Arial/Calibri | ✓ | ✓ Calibri bold | – | ✓ | ✗ Libertine | ✓ Fira | ✓ Fira |
| Reduce clutter: gridlines, legends, 3-D, logos, decoration | (✓) | ✓ | ✓ | ✓ logos | ✓ Phluff | ✓ | – | ✓ "minimize noise" | – |
| Integrate labels/legend into the graph; direct labels | ✓ | ✓ | ✓ | – | ✓ | ✓ | – | – | – |
| Bars start at zero; no pie charts | – | ✓ | – | – | – | ✓ | – | – | – |
| Back-up/appendix slides with links instead of busy slides | ✓ | – | – | – | – | – | ✓ | ✓ appendix | (✓) one link |
| Build/reveal sequentially (`\only`, `\onslide`, layering) | ✓ with care | – | ✓ layering | (✓ only if useful) | ✗ fragments | (✓ say then show) | ✓ cellcolor<k> | – | ✗ none |
| Blank space between elements; wide spacing in lists | ✓ wideitemize | – | ✓ | ✓ | – | ✓ | ✓ stretch 1.3 | ✓ | ✗ |
| Lists ≤ 4 items; ≤ 3–5 main points in the talk | – | – | (✓) 5 sections | ✓ 2–4 | ✓ no deep trees | ✓ 3 (max 5) | – | – | ✗ |
| 16:9 aspect ratio | ✓ | – | (✓ know the room) | ✓ templates | – | – | ✓ | – | ✓ |
| Speaker notes / secondary detail off the slide | ✓ \note | – | – | ✓ notes page | ✓ handout | ✓ in speech | – | (✓ bug note) | ✗ |
| Rehearse; practise clicks | ✓ | – | ✓ | ✓ | – | ✓ | – | – | – |
| Roadmap/outline slides | optional | – | ✓ scaffolding | only long talks | ✗ | ✗ avoid "Outline" | ✓ every section | ✓ section pages | ✓ (MT) |
| Frame numbers / footer | ✓ number only | – | – | – | – | ✗ unless segregated | ✓ grey mono | ✓ counter | ✓ |
| Tables: few numbers, highlight cells, drop SEs/R² | ✓ highlight | – | ✓ | (✓ short tables) | ✗ full matrix on paper | (✓ short table) | ✓ highlight | – | – |

## Five rules that recur most often

1. **One message per slide, stated as a sentence title** (AL, DO, SB, GP "one point well", TU "sentences have agency"; CT partly).
2. **Evidence is visual; strip text and bullets** (SJ, SB, AL, TU, DO, GP).
3. **Reduce clutter / maximise signal-to-noise** — gridlines, legends, logos, decoration, 3-D (SJ, SB, AL, TU, DO, MT, GP).
4. **Few, consistent, colour-blind-safe colours** — one base + one or two accents (GP, SJ, SB, DO, MT).
5. **Type large enough for the back of the room, never shrunk to fit; same for figure fonts** (GP, SB, AL, DO, MT manual).

## Disagreements

- **Density.** Tufte wants high-resolution handouts and full data tables; Alley/Doumont/Schwabish/GP want sparse,
  glanceable slides. For a seminar the sparse camp wins; Tufte's residue is "nothing important in small type, show comparisons side by side".
- **Bullets.** Alley bans them; Tufte and Doumont discourage; Goldsmith-Pinkham, Butts, Schwabish keep short lists
  (GP with dashes and wide spacing; SB without markers).
- **Typeface.** Alley: bold Calibri/Arial. Schwabish: avoid Arial/Calibri/Times. GP: Lato. MT: Fira Sans (Light!). KB: serif Libertine.
  Consensus: a humanist sans at a non-light weight (Lato, Fira Sans Book/Regular, Source Sans).
- **Background.** GP off-white or black (auditorium); SB light grey; MT near-white `black!2` or dark; AL only "no hot colours".
- **Chrome.** Doumont: no logos, no footers, segregated navigation; Alley: logo first/last slide only; MT title bar +
  progress bar, GP/KB/MT frame numbers. Seminar practice (questions cite slide numbers) favours a small number.
- **Roadmaps.** SB scaffolding and MT/KB section pages vs Doumont ("Outline adds little") and Alley (only for long talks); GP optional.
- **Builds.** GP/SB/KB use overlays and layering; Tufte dislikes fragmenting; Doumont allows "state verbally, develop visually".
- **Point sizes.** Alley's 28/18–24 pt are PowerPoint sizes. Beamer's 16:9 canvas is 160×90 mm (measured: 453.5×255.1 pt);
  a PowerPoint 16:9 slide is 13.33 in = 338.7 mm wide, a factor of 2.12. So Beamer 11pt ≈ 23 pt, `\large` ≈ 30 pt,
  `\footnotesize` ≈ 19 pt, `\scriptsize` ≈ 17 pt in PowerPoint terms: an 11pt body with a `\large` headline matches Alley's
  numbers, and the real violation is line count and word count, not point size. (Corrected 2026-11-04; the first draft
  assumed the 4:3 width of 128 mm.)
- **Tables on slides.** GP/KB show regression tables with highlighted cells; Schwabish says drop SEs/R²; Tufte says put them on paper.
