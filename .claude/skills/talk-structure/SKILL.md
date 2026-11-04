---
name: talk-structure
description: How to structure an academic economics talk (seminar, job talk, conference slot, brown bag) -- what goes in, in what order, how much time each part gets, one claim per slide with a sentence headline, what to cut, how to end, and how to handle questions. Condensed from Shapiro, Meager, Cochrane, Bellemare, Tabarrok, Peyton Jones, Fatahalian, Motta/Schlag (EUI), Shengwu Li and Winston (summaries in `references/`). Use whenever a session outlines, drafts, restructures, trims or reviews a talk or slide deck, decides what to show from a paper, writes slide headlines, or prepares for questions. For the Beamer implementation (template, colours, fonts, figures, tables, reveals) use the `beamer-slides` skill.
---

# Structuring a talk

A talk is not the paper read aloud. The paper documents; the talk gives a smart non-expert one
idea, enough evidence to believe it, and the wish to read the paper. Ten guides for economists
and scientists agree on this almost unanimously (`references/_overlap.md`); where they disagree
this skill takes a position and says so.

## The five rules everyone agrees on

1. **Cut.** Present a few things well. A 30-minute talk is not a 90-minute talk given three
   times faster (Shapiro). Leave documentation to the paper.
2. **Aim at a smart non-expert.** "Think JEP, not Econometrica" (Tabarrok); the second-year
   student in the room (Li). You cannot make it too simple (Cochrane). Define every term and
   every symbol; explain every figure.
3. **One take-away.** The audience walks out with one qualitative or, better, quantitative
   sentence: "measurement error explains 27% of the equity premium puzzle", not "we estimated
   the consumption CAPM on new data" (Shapiro). Write that sentence before anything else.
4. **Sparse slides, never read.** Slides support the voice; they are not notes. Everything on
   a slide gets talked about; nothing talked about at length is missing from it.
5. **No literature review.** The value added goes on the "this paper" slide in one or two
   lines against the two or three closest papers. A slide of citations is "dull to read,
   worse to hear" (Bellemare).

## Order of an applied talk

The default order, which every empirical source prescribes with small variations:

| # | Block | Share of time | What it must achieve |
|---|-------|---------------|----------------------|
| 1 | Hook | 1-2 slides, about 2 min | "Your audience does not care about your topic. You have 1-2 slides to change their minds" (Shapiro). A fact, an anecdote, a policy question. Not a philosophy of the field. |
| 2 | Question | 1 slide | A counterfactual ("what happens if"), a deep parameter, or a test of a prediction. Motivated by economics, not by a gap in the literature. |
| 3 | This paper + preview | 1-2 slides | What you do, why you can add something (new data, new variation, new measurement), and the headline results in three or four tangible lines. "Assume the audience is about to leave; make sure they walk out with something." |
| 4 | Data | 1-2 slides | Source and level of every variable, so nobody wonders "where did that come from, state or county?" Anticipate the measurement worry here. Never narrate cleaning steps: "no one wants to see your underwear." |
| 5 | Design | 2-4 slides | The estimating equation with every symbol named, the identifying assumption stated as a condition in words ("tax-law changes are exogenous conditional on county and year effects"), and the named target parameter the results will report. Then the one or two real vulnerabilities and what you do about them. Not every criticism, not the models you tried. |
| 6 | Results | the bulk | Main result first, as a figure. Then the mechanism or heterogeneity that makes it believable. Robustness as one bullet list; the tables live in the appendix. |
| 7 | Bottom line | 1 slide | The take-away sentence, its magnitude, what it implies. No "future research", no "Thank you / Questions?" slide. |
| 8 | Appendix | as many as useful | Everything cut from 4-6, reachable by link. Bellemare's "magic appendix trick": 15 main slides, 30 appendix slides. |

Checkpoint before the results (Shapiro): the audience must by then believe the question matters,
know what you measure and how, and know what you will do with it. "Otherwise the talk is already
over, you just don't know it."

**A theory talk** (Li) shortens 1-3 and reaches the model within 3 minutes of a 20-minute talk,
7 of 45, 10 of 75; the model is a concrete special case first, the general one later or never.

## Time and slide budgets

Confirm the real slot first: a "75-minute seminar" is often 45 minutes plus questions (Bellemare).
Then convert the slot into talking time and let that determine the content.

- **Conference slot**: talking time is the slot minus any discussant and Q&A time.
- **Seminar with interruptions**: plan on talking for about two thirds of the slot. This is this
  skill's rule of thumb, not a number from the sources; they only agree that questions eat far more
  time than speakers expect. A 75-minute seminar therefore uses the 45-minute row below, plus a long
  appendix.

| Talking time | Main slides | Hook + question + preview | Design | Results | Bottom line |
|--------------|-------------|---------------------------|--------|---------|-------------|
| 12 min | 8-10 | 2 min | 2 min | 7 min | 1 min |
| 20 min | 12-15 | 2 min | 4 min | 12 min | 2 min |
| 30 min | 18-22 | 3 min | 6 min | 19 min | 2 min |
| 45 min | 25-30 | 4 min | 8 min | 30 min | 3 min |
| 60 min | 32-40 | 5 min | 10 min | 41 min | 4 min |

The rule behind the slide counts: between one and two minutes per main slide, nearer two for
slides with a figure or an equation (Cochrane: "you will not be able to put up one slide per
minute"; Li: two minutes per slide; Bellemare's one per minute is the upper bound). Ending early is
fine; rushing the results is the one unforgivable failure (Cochrane timed a quarter's finance
workshops: not one paper reached its main result in under an hour).

## Build the headlines before the slides

Each slide makes one claim, and its title is that claim as a full sentence, two lines at most
(Fatahalian, Tabarrok, Alley): "Movers respond to the cap: out-migration from exposed counties
rises 0.3 pp", not "Results: migration". Test: read only the titles in order. If that is a
coherent summary of the talk, the structure is right; if a title is a noun phrase, the slide
has no claim and goes or merges. Say why before what: announce where the next block goes and
what question it answers before showing it, so a listener who drifted can re-engage. Surprises
in the narrative are bad (Fatahalian); if you yourself forget what the next slide is while
rehearsing, the narrative is missing, not your memory.

Procedure for a new deck or a restructuring:

1. Write the take-away sentence (rule 3 above).
2. Write the ordered list of slide headlines as plain sentences in a text file before touching
   LaTeX. Count them against the budget table. Cut until they fit.
3. For each headline decide the single piece of evidence that carries it (one figure, one
   equation, one short table, one example). A slide needs no second piece.
4. Only then write frames (`beamer-slides` skill).
5. Rehearse aloud, timed, in front of someone who does not know the paper. Rehearsal is editing:
   the slides that get narrated badly are the ones to cut.

## Decisions on the points where the guides disagree

- **Preview the results: yes.** Shapiro, Bellemare and the EUI guide say so; Cochrane's "look at
  table 1" is the same instinct pushed to the limit and works only for an expert audience and a
  long slot. The preview is three or four tangible lines, not a results slide shown twice.
- **Motivation: short.** One or two slides (Shapiro), about two minutes (Peyton Jones). His
  "motivation 20%" covers what the problem is as well as why it matters, so it spans blocks 1-2
  above. Five minutes of pure motivation (Tabarrok, EUI) is for a 75-minute seminar at most.
- **No outline slide at the start.** It carries no information before the audience knows the
  question (Peyton Jones). Section slides or a progress bar do the job during the talk.
- **Teach, don't pitch.** State what you believe and why; the evidence sells (Li). Meager's point
  survives: the talk is about what the audience thinks and wants, not about your effort.
- **Pace.** Keep moving; pause only on the equation or the figure you want remembered (Li's
  "pause after every equation" and Shapiro's "no pauses" meet here).
- **Scripting.** Script the first two sentences and every transition; speak the rest (Peyton
  Jones, Fatahalian). Memorising the whole talk makes it sound read (Bellemare).

## What to cut, always

- Anything describing effort: data cleaning, download time, what you tried first, how you came
  to the idea ("save it for your memoirs", Cochrane).
- The full regression table. Show the coefficient of interest, with "controls: yes / county FE:
  yes" rows; the rest is an appendix slide.
- The general model when a special case carries the point.
- Robustness, one by one. One list slide, tables in the appendix.
- Policy implications before the result exists (nobody can follow them yet).
- Work you wish you had done. "By the time you stand in front of a room, the amount of work you
  have done is the exact right amount" (Meager).
- Apologies, jokes in the first minute, "I'll go fast through this".

## Questions

- Listen to the whole question, then count to three (Cochrane). Often it is not the question you
  expected.
- Answer in one sentence with your reasoning. A question is not a request to change the paper;
  "I can change it!" signals you had no reason for the choice (Meager).
- "I don't know" and "I need to think about that" are complete answers. Flag an off-the-cuff
  answer as such (Bellemare).
- Try twice to resolve a point; then "let's take this offline" and return to the story. In a
  seminar, protect the results: if questions eat the time, cut slides from the middle, not the
  end.
- Keep a sheet of paper: some questions are edits to the paper.
- Never defensive. "You are not your work, and your work is not you."

## Applying this to the project's decks

The current deck is `text/presentation.tex` (TCJA, SALT cap, migration and local fiscal
capacity). Its storyline follows the order above: hook (the cap removed a $90 billion subsidy),
two questions (migration, fiscal strain), data (SOI exposure and flows, government finances),
exposure difference-in-differences design, results, bottom line. When revising it, start from
the headline list, not the frames, and keep the exposure-by-year event-study figures as the
evidence of record (they are return-weighted, 2017-centred; see `CLAUDE.md`).

## References

One file per source in `references/`, each a numbered list of that author's concrete rules,
with their numbers, and a note on where they contradict the others. `references/_overlap.md`
is the rule-by-source table and the list of disagreements this skill resolved above.

| File | Source | Best for |
|------|--------|----------|
| `shapiro-applied-micro-talk.md` | Shapiro, How to Give an Applied Micro Talk | the order of an applied micro talk, the preview, data and design slides |
| `meager-public-speaking.md` | Meager, Public Speaking for Academic Economists (2017) | delivery, questions, audience focus |
| `cochrane-writing-tips.md` | Cochrane, Writing Tips for PhD Students (2005), §4 and writing rules | getting to the result, slide text, tables |
| `bellemare-talks.md` | Bellemare, 22 Tips for Conference and Seminar Presentations (2014) | slide counts, intro formula, appendix trick, conference talks |
| `tabarrok-great-seminar.md` | Tabarrok, How to Give a Great Seminar | the talk as story, controlling the room |
| `peyton-jones-research-talk.md` | Peyton Jones, How to Give a Great Research Talk | paper vs talk, motivation, no outline |
| `fatahalian-clear-talks.md` | Fatahalian, Tips for Giving Clear Talks | headline-as-claim, audience attention budget |
| `eui-how-to-present.md` | Motta and Schlag, How to Present (EUI, 2000) | seminar norms, timing, ending |
| `shengwu-li-theory-talks.md` | Li, notes on theory talks (2023) | theory talks, teaching not selling |
| `winston-how-to-speak.md` | Winston, How to Speak (MIT, 2018) | opening, repetition, the final slide |
