---
name: notebook
description: Keep the research notebook of this repository (property_taxes) -- a dated, recipe-only entry in `notebook/` at the repo root for every result a session produced that someone may want to find again, listed in `notebook/INDEX.md`. Use it whenever a session ran an estimate, diagnostic, validation check, comparison or dead end whose numbers someone may want later; when the user asks to log, record, archive, write up or "add this session to the notebook"; at the end of an analysis session that produced findings; when a new result replaces an earlier one; and BEFORE answering any question about what was found earlier ("what did we get for ...", "didn't we check ...", "where did the capitalisation estimate end"), by reading INDEX.md first.
---

# The research notebook

Sessions here answer questions with runs on CoreLogic, Data Axle, SOI and Census data: event studies, border RDs,
balance checks, diagnostics, and dead ends. Many scripts and outputs are committed under `code/analysis/` and
`output/analysis/`, and running notes go into `text/data_exploration/dataaxle.md`. Those are long, mixed and hard to
search. The notebook keeps what is worth reading again: the question, the answer with its numbers, and a recipe precise
enough to rebuild the run. Collaborators who don't use Claude Code read it too, so write in plain words.

(Adapted from the notebook skill of `~/PhD/projects/social_security_reform`.)

## What gets an entry, and what doesn't

An entry is one RESULT, not one session. A session can produce none, one, or a few (an estimate and a diagnostic
it led to are two entries if someone could look for either on its own).

- **Entry:** results someone will want to quote or look up later: estimates, diagnostics, validation and balance checks,
  sensitivity runs, comparisons, approaches tried and dropped (say why), syntheses of where a question stands.
- **No entry:** routine steps with no finding of their own (a data pull, a fix, a rerun with identical results).
  The running log `text/data_exploration/dataaxle.md` keeps the step-by-step detail; an entry links to its section
  instead of repeating it.

## Where

```
notebook/                               at the repo root (no per-person folders in this repo)
├── INDEX.md                            one line per entry, newest first -- read this first
└── YYYY-MM-DD-short-slug/              the date of the result, a few words
    ├── README.md                       the entry
    └── <figure>.png                    at most one or two, and only if they carry the result
```

## The entry

Keep it under a page. Use this layout:

```markdown
---
date: 2026-10-21
title: Did the SALT cap capitalise into house prices? Response by lost-subsidy component
question: the question as the user asked it, in one sentence
status: current                      # or: superseded by 2026-11-02-some-slug
code: property_taxes 54c4545 (local main, not pushed)
tags: capitalisation, prices, SOI
---

## Answer
The finding in a few sentences, with the numbers: a small table if there are several. Say what it means and
what it does not show.

## How
- **Outline:** the pipeline in one or two lines, e.g. `SOI county x stub TY2016/18 -> lost value per owner -> event study of log median price | county + state x year`.
- **Choices:** every decision that isn't a default or is easy to get wrong, one line each (weights, samples, FE, clusters, bandwidths, winsorising, assumed rates).
- **Inputs:** every input file, with the commit or cluster job it comes from; flag anything git cannot bring back.
- **Scripts:** the committed scripts that produce it, if any.
- **Check:** the two or three numbers a rebuilt run must reproduce.

## Open
What the result leaves to do, and for whom (if anything).
```

## Why the recipe

Rebuilding a script from a precise description takes minutes; what gets lost are the decisions and the inputs.

- **Choices are what make a result reproducible.** The result depends on small decisions (which counties are balanced,
  return weights vs unweighted, the reference year, how SOI censoring was handled, which tract vintage). Change one
  and the numbers move. List each.
- **Inputs must still exist.** `data/` is gitignored: CoreLogic panels built on Bouchet/SOM, SOI and Census downloads,
  Data Axle extracts live only on the laptop or the clusters. Name them explicitly, with the cluster SQL that built
  them when relevant. A commit hash brings back only what is committed and pushed; flag unpushed commits. Gather with
  `git rev-parse --short HEAD`, `git log --oneline origin/main..HEAD` (unpushed), `git status --porcelain`
  (uncommitted), and `git ls-files --error-unmatch <file>` (tracked or not).
- **The check numbers let a rebuild verify itself.** If rebuilt code gives the same two or three numbers, it was
  rebuilt right.
- **Code that will be run again belongs in `code/`**, not in the notebook.

Never put microdata in an entry: no CoreLogic parcel or deed records, no Data Axle household rows, no owner names
(they are hashed and never exported), as anywhere in the repository. Aggregated numbers and figures are fine.

## Entries are snapshots

Don't rewrite an entry's answer later, even when it turns out wrong: the notebook is a record of what was found
when, and a silently changed number can't be traced. When a later result replaces an entry:

1. write the new entry, and say in it which entry it replaces and what changed;
2. in the old entry, set `status: superseded by <new folder>` and add one line under its title saying so;
3. mark the old line in INDEX.md `(superseded)`.

Fixing a typo or a broken link is fine.

## INDEX.md

One line per entry, newest first, so a reader (or a session) finds the right entry from the index alone:

```markdown
- 2026-10-21 [Migration responses](2026-10-21-migration-nulls/README.md) -- none pre-COVID in SOI flows, Data Axle, enrolment or turnover
```

The text after the dash is the answer in a few words, not the topic.

## Reading the notebook

Before answering a question about earlier results, read `notebook/INDEX.md`, then the entries that match. Quote only
`current` entries as current; if the best match is superseded, follow the link and say that it was superseded.
Give the entry's date and the commits it rests on, because the code may have moved since.

## Writing at the end of a session

1. Decide which results deserve entries (see above); if none, say so and stop.
2. Collect what the recipe needs while it is still at hand: commits, unpushed or untracked inputs, the choices
   made during the session, the numbers.
3. Write each entry, copy in at most one or two key figures, and add the lines to `INDEX.md`.
4. Mark any entry the new results supersede.
5. Tell the user which entries you wrote, and commit them only if they ask.
