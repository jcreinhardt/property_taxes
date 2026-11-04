---
name: beamer-slides
description: How to build slides in Beamer for this project -- the house style file `template/ptaxslides.sty` and example deck, slide anatomy (sentence headline + one piece of evidence), type sizes, colours that match the R figures, exporting ggplot figures at slide size so their text is readable, slide tables, equations, overlays/builds, backup slides with links, speaker notes, and a pre-flight checklist. Condensed from Goldsmith-Pinkham's beamer-tips, Schwabish (Better Presentations; JEP 2014), Alley (assertion-evidence), Doumont, Tufte, the metropolis manual and Butts's templates (summaries in `references/`). Use whenever a session writes, edits, restyles, compiles or reviews a .tex slide deck (e.g. `text/presentation.tex`), makes a figure or table meant for slides, or migrates a deck to the house style. For what the talk says and in what order, use `talk-structure` first.
---

# Building slides in Beamer

`talk-structure` decides the storyline and the list of sentence headlines. This skill turns each headline into
a frame. The design sources agree on five things (`references/_overlap.md`): one message per slide stated as a
sentence title; evidence shown visually, not in bullets; nothing on the slide that does not carry the message;
few, consistent, colour-blind-safe colours; type large enough for the back row, figures included.

## Start a deck

1. Copy `template/ptaxslides.sty` next to the deck (for the project deck, into `text/`). Start from
   `template/example.tex`, which has one frame per layout below.
2. Class line: `\documentclass[11pt,aspectratio=169]{beamer}` then `\usepackage{ptaxslides}`. Nothing else is needed.
3. Build with `latexmk -pdf deck.tex` (pdfLaTeX) or `latexmk -lualatex deck.tex`; both work. Two passes are needed
   for `\source`, the link buttons and table highlights; latexmk does that. Build files (`.aux .nav .snm .toc .out
   .fls .fdb_latexmk`) are not covered by `.gitignore`; build in a scratch folder or delete them before committing.
4. The style loads Fira Sans Book with SemiBold for bold, maths in Fira through `mathastext` (so table digits and
   equations match the text), `booktabs`, `dcolumn` (column type `d`), TikZ, `appendixnumberbeamer`, `pgfpages`.

What the style does, and why:

| Setting | Choice | Source |
|---------|--------|--------|
| Canvas | 16:9, 160 x 90 mm, 11pt | Goldsmith-Pinkham, Alley templates |
| Frame title | left-aligned, `\large` SemiBold, wraps to two lines, no bar | Alley, Doumont: headline top-left; Doumont, Tufte: no chrome |
| Footline | frame number only, small grey, bottom right | GP, metropolis; questions cite slide numbers |
| Logos, navigation symbols, progress bar | none | Doumont, Alley, Tufte |
| Typeface | Fira Sans Book, not Light | metropolis manual: Light is too thin for projectors |
| Colours | blue `ptaxblue` #2166AC, red `ptaxred` #B2182B, grey #8C8C8C, reserve green #1B7837 | same as `code/R/theme.R`; ColorBrewer RdBu is colour-blind safe |
| Lists | en-dash markers; `wideitemize` adds 10pt between items | GP: spacing so less fits |
| Background | white | matches the white figure backgrounds the R code saves |

## Slide anatomy

Every frame is **headline + one piece of evidence + optional source line**.

- **Headline.** The claim as a full sentence, at most two lines, about 12 words (Doumont), sentence case, no full
  stop needed. "Out-migration from exposed counties rose 0.3 pp after 2017", not "Migration results". Section
  breaks and the title page are the only frames without a claim.
- **Evidence.** One figure, one equation, one short table, or one diagram. A second piece means a second slide.
  Two to four short items if there must be text, each at most two lines, with no nested sub-bullets (Alley,
  Tufte). Never more than about 40 words of body text.
- **Source line.** `\source{...}` bottom left, for data source, sample, weights and clustering. This replaces
  footnote hacks and keeps notes off the figure.
- **Never shrink to fit.** No `\small`, `\footnotesize`, `\resizebox` on text or `[shrink]` to rescue a full frame.
  Split it, or move the detail to a backup slide and link to it (Goldsmith-Pinkham).

Type sizes in PowerPoint terms (Beamer scales by 2.12 to the same screen): body 11pt projects like 23 pt,
`\large` headline like 30 pt, `\footnotesize` like 19 pt, `\scriptsize` source line like 17 pt. Alley's floor is
18 pt, so nothing smaller than `\scriptsize`, and that only for the source line and buttons.

## Layouts (all in `template/example.tex`)

| Layout | Use for | Code |
|--------|---------|------|
| Headline + figure | most results slides | `\slidefig{file}` (full width, never taller than the space under a two-line title); `\slidefig[0.8]{file}` |
| Figure + 2-3 lines | a figure that needs a legend in words or a number to remember | `columns[T,onlytextwidth]`, `0.58` / `0.38` |
| Step-through figure | building an event study series by series, or adding the post period | `\only<1>{\slidefig{a}}\only<2>{\slidefig{b}}`; every step has identical axes |
| Equation | the estimating equation | one display, coefficient of interest `\cred{}`, the treatment variable `\cblue{}`, then 2-3 lines naming the symbols and the identifying assumption |
| Short table | when the number itself is the message | below |
| Section break | between the blocks of the talk | `\begin{transitionframe}Mechanisms\end{transitionframe}` |
| Backup slide | robustness, details, proofs | `\appendix`, then `[label=...]` frames with `\backlink{...}` |

## Colour

- Two accents, used the same way on slides and in figures. **Red** (`\cred{}`, `\alert{}`) marks the treated, exposed,
  high-tax or post-2017 side and the one number to remember. **Blue** (`\cblue{}`) marks the control, low-tax or
  comparison side and structure. **Grey** (`\cgrey{}`) is context. These are `COL_SIDE` and `COL_PERIOD` in
  `code/R/theme.R`, so "the red line" means the same thing in the figure and in the sentence above it.
- Colour only what the audience should look at. One emphasis per slide; never bold and colour together.
- Check figures in greyscale or a colour-blindness simulator if they use more than the two accents.

## Figures

The main failure in this repository: figures are saved 10-12 inches wide with `base_size` 10 and then shrunk onto
a slide that is 5.8 inches wide, so their text ends up at a third of the slide's body size. Make slide figures at
the size they will be shown, so that 11pt in R is 11pt on the slide:

| Slot on the slide | ggsave width x height (in) |
|-------------------|----------------------------|
| `\slidefig` full width under a two-line headline | 5.8 x 2.5 |
| full width under a one-line headline | 5.8 x 2.8 |
| left column of the figure + text layout (0.58) | 3.4 x 2.5 |
| two figures side by side | 2.8 x 2.4 each |

(Measured on the house style: text width 420 pt = 5.84 in, `\slidefig` height 179 pt = 2.49 in.)

```r
source("code/R/paths.R")                       # loads theme_ptax(), COL_SIDE, COL_PERIOD
g <- g + theme_ptax(base_size = 11) +           # same size as the slide body text
  theme(legend.position = "none")               # label the lines directly instead
ggsave(file.path(out_dir("..."), "fig_slide.pdf"), g, width = 5.8, height = 2.5)
```

- Save slide figures as **PDF** (vector), not PNG; keep the paper version separately if it needs other dimensions.
- **Label series directly** at the line ends or with `annotate()`, not in a legend box (Schwabish, Doumont). Give the
  labels `size = 11 / .pt` so they print at 11pt, and widen the x axis on the right
  (`scale_x_continuous(expand = expansion(add = c(0.3, 2.6)))`) so they fit. Drop the plot title (the headline is the
  title), minor gridlines, and any axis label the headline already states.
- Checked 2026-11-04 on the house style: a 5.8 x 2.5 in export with `base_size = 11` gives axis text at the slide's
  body size; the same plot saved at 11 x 6.5 in, as most repo scripts do, shows its text at about a third of that.
- Event studies: zero line in grey, 2017 reference marked, pre-period and post-period in `COL_PERIOD`, confidence
  bands light. Keep axes fixed across all steps of a build.
- Bars start at zero; no 3-D, no pie charts, no dual y-axes.

## Tables

A slide table shows the numbers that carry the claim and nothing else (Schwabish, Cochrane): the coefficient of
interest, its standard error, rows saying "County, year FE: yes", and the sample size. Three columns at most. No
R², no control coefficients, no stars beyond what the talk needs. The full table is a backup slide.

```latex
\begin{tabular}{l d d d}                  % d = aligned on the decimal point
  \toprule
   & \multicolumn{1}{c}{Out-migration} & \multicolumn{1}{c}{AGI out} & \multicolumn{1}{c}{Own revenue} \\
  \midrule
  Exposure $\times$ post & 0.31 & 0.12 & \cellfrom{a}-0.02 \\
                         & (0.08) & (0.05) & (0.04)\cellto{a} \\
  \bottomrule
\end{tabular}
\highlightcells<2->{a}                     % red box around the cells on the second click
```

Rows can be revealed with `\onslide<2->{...}` (see `references/goldsmith-pinkham-beamer-tips.md`), but a box on the
one cell that matters usually beats a row-by-row build.

## Overlays

- Build only when the order of revelation is the argument: series by series in a figure, pre-period before
  post-period, a highlight after the audience has read the table (Doumont: "state verbally, then develop
  visually"). Never animate a list for its own sake.
- `\setbeamercovered{invisible}` is set, so hidden items take space but are not shown faintly.
- Rehearse every click. Unrehearsed builds are what make a talk feel artificial (Goldsmith-Pinkham).

## Backup slides and links

- Put `\appendix` after the last main slide; `appendixnumberbeamer` then restarts numbering, so the main deck's
  last frame number is the true length.
- Label the main frame and the backup frame, link one way with `\appendixlink{label}{Text}` (bottom right) and back
  with `\backlink{label}`.
- Anything cut from a busy slide goes here: full regression tables, robustness, data construction, proofs.

## Speaker notes

Write `\note[itemize]{...}` after a frame: what to say first, the transition sentence, the one number. Switch the
output with one of three lines in the preamble (`hide notes`, `show only notes`, `show notes on second screen=right`).
To present with notes on the laptop: build `slides.pdf` with hidden notes and `notes.pdf` with only notes, open both in
Skim, and set notes.pdf as the synchronised notes document (View, Presentation options).

## Pre-flight checklist

Before sending or presenting a deck, check every frame:

1. The titles alone, read in order, tell the talk (`talk-structure`).
2. Each title is a sentence of at most two lines.
3. One piece of evidence per slide; at most four text items, none longer than two lines; no nested bullets.
4. No `\small`, `\footnotesize` or `[shrink]` on body text; no frame overflows (`Overfull \vbox` in the log).
5. Every figure was exported at slide size and its smallest text is readable at 50% zoom of the PDF.
6. Red and blue mean the same thing in figures and text throughout.
7. Every number on a slide is discussed; everything else is in the appendix.
8. Main deck length fits the budget table in `talk-structure`; backup slides are linked.
9. Source line on every figure or table built from data, with sample, weights, clustering.
10. No "Thank you" or "Questions?" slide; the last main slide is the bottom line.

## Migrating `text/presentation.tex` (metropolis) to the house style

The current deck uses `\usetheme[progressbar=frametitle]{metropolis}` at 10pt (`references/current-template.md`).
To move it:

1. Class options `11pt,aspectratio=169`; replace `\usetheme{...}` and the colour definitions with
   `\usepackage{ptaxslides}`; keep `natbib`.
2. Delete `\metroset{...}`; turn `alertblock` model boxes into a plain display equation with `\cred{}` on the
   coefficient.
3. Replace `figure` environments and `\footnotetext` source hacks with `\slidefig{}` and `\source{}`; re-export the
   figures at slide size (table above).
4. Rewrite noun-phrase titles ("Results", "Corelogic Data") as claims; split full-text frames.
5. Move the math, literature and robustness frames after `\appendix` and link them; delete the "THANK YOU" frame.

## Known quirks

- Plain frames (title page, section breaks) can log `Overfull \vbox (about 5pt too high)` when the deck also has
  TikZ overlays. It does not change the output (checked pixel by pixel); ignore it on plain frames only.
- `mathastext` sets maths letters in Fira italic. For Computer Modern maths instead, replace the `mathastext` line
  in the style with `\usefonttheme[onlymath]{serif}`; table digits in `d` columns then become serif too.
- `\source`, `\appendixlink`, `\backlink` and `\highlightcells` use `remember picture`: they are misplaced after
  a single compile pass and correct after the second.
- `\highlightcells` marker names must be unique within a frame.

## References

One file per source in `references/`, each with that source's concrete rules and, where requested, its LaTeX
verbatim. `references/_overlap.md` has the rule-by-source table and the disagreements resolved above.

| File | Source | Best for |
|------|--------|----------|
| `goldsmith-pinkham-beamer-tips.md` | Goldsmith-Pinkham, Tips + Tricks with Beamer for Economists | full preamble, dummy frames, table highlighting, row/column reveals, DiD timing diagram, notes, appendix links (verbatim code) |
| `schwabish-jep-visualizing-data.md` | Schwabish, "An Economist's Guide to Visualizing Data", JEP 2014 | figure rules, eight before/after examples |
| `schwabish-better-presentations.md` | Schwabish, Better Presentations (2016) | visualize/unify/focus, layering, presentation worksheet |
| `alley-assertion-evidence.md` | Alley, assertion-evidence, The Craft of Scientific Presentations (2013) | sentence headlines, type sizes, words per minute |
| `doumont-effective-slides.md` | Doumont, Trees, Maps, and Theorems (2009) | the laws of communication, slide checklist, graph rules |
| `tufte-cognitive-style.md` | Tufte, The Cognitive Style of PowerPoint (2006) | the case against bullets and fragmentation |
| `metropolis-theme.md` | metropolis theme manual | if a deck stays on metropolis: options, fonts, pitfalls |
| `butts-beamer-templates.md` | Butts, latex-slides template | alternative house macros: coloured text, `\imageframe`, absolute positioning |
| `current-template.md` | `text/presentation.tex` as of 2026-11-04 | what the existing deck does and where it departs from the rules |
