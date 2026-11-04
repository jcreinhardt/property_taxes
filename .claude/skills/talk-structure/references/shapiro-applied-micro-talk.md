# Shapiro — How to Give an Applied Micro Talk

- Author: Jesse M. Shapiro (Chicago Booth and NBER at the time; now Harvard)
- Title: "How to Give an Applied Micro Talk: Unauthoritative Notes"
- Year: undated slide deck (c. 2010; widely circulated)
- URL used: https://firminayivodji.github.io/Documents/r14.pdf (mirror; the Harvard
  scholar.harvard.edu link returned "Access Denied", brown.edu timed out,
  sangmino.github.io/Documents/r39.pdf is a *different* Shapiro note, "Four Steps to an
  Applied Micro Paper")
- Length: ~45 Beamer slides, 6 pp. in handout form

## Concrete rules (in the order of the talk he prescribes)

1. **Motivation: "Your audience does not care about your topic. You have 1-2 slides to
   change their minds."** Use anecdotes, facts, policy questions.
2. **State a research question** of one of three kinds: a policy/counterfactual ("what
   would happen if..."), an estimate of an important deep parameter, a test of an
   important theoretical prediction — "or, better yet, all three".
3. Questions are motivated by economics, **not by the literature**. Not research
   questions: "apply model X to industry Y", "change assumption Z of model X",
   "re-estimate so-and-so's model on other data".
4. **"This paper" slide:** outline what the paper does and why you have something to add
   ("revisit the consumption CAPM using new high-quality consumption data", NOT "because
   no one has estimated it in a few years").
5. **Preview of findings: "Assume the audience is about to leave. Make sure they walk out
   with something."** Be tangible but terse: just enough method that the results don't
   feel like magic, not so much that it crowds out the findings. Worked example: the right
   level is four lines ("maternity leave reduces women's wages 5%; no effect past fertile
   age; no effect for men; implies ~$0.75 of every $1 paid by the mother"); "incidence
   mostly on wages" is too little; a slide of sample sizes, means and clustering choices is
   too much.
6. **Data slide:** state the source of each variable so that "no one should be wondering:
   where did that come from? state or county level?" Anticipate measurement concerns now
   (sources reliable? concepts match the model?).
7. Claim credit for **novel data, new ways of measuring, new sources of variation** — but
   "no one cares that this dataset took a long time to download".
8. **"No one wants to see your underwear"**: don't narrate data processing steps. Replace
   five "then I collapse/divide/remove" lines with one: "Average wage by state, year and
   gender, excluding outliers (>$100/hour in 2008 dollars)".
9. **Model: be explicit, then more explicit.** Write the estimating equation, define every
   symbol, and state identification as a formal condition
   (E(ε|x, α, δ) = 0, "i.e. law changes are exogenous conditional on fixed effects").
10. **Define your bottom line in the model section** (e.g. β/γ = share of leave costs paid
    by the worker) so the results slide has a named target.
11. Better still: lay out the economic assumptions that justify the econometric ones, and
    connect the model to a well-defined policy or welfare question.
12. **Pause to discuss the most important vulnerabilities** of the approach, why the model
    is a good approximation, and what you'll do to assess sensitivity. **Do not** try to
    anticipate every criticism, talk about other models you tried, or discuss fine points
    no one will think of.
13. Checkpoint before results: the audience must (a) believe in the question, (b)
    understand what you measure and how, (c) understand what you'll do with the data and
    why. "Otherwise ... the talk is already over, you just don't know it."
14. **Slides: every word is precious** (the audience can't skip or browse). Clear and
    sparse; no extraneous detail.
15. **"Make the slides tell the story with your voice."** Don't put anything on a slide
    you don't plan to talk about; space devoted = emphasis intended.
16. **Pacing: no pauses, unless you really want to stress something** (demonstrated with
    a nine-click build).
17. **Leave documentation to the paper.** "Slides cannot be complete — there is not enough
    time. Use your talk to tell your story."
18. **Scaling: "A 30 minute talk is not a 90 minute talk where you talk three times
    faster"** (credit to Matthew Rabin). Choose emphasis and detail for the time you have.
19. **Results: figures wherever possible** — "more honest, more complete, more
    interesting, more persuasive". Tables only to summarise key magnitudes, not to show
    every control or every robustness check (summarise those in bullets).
20. "Always be telling your story."
21. **Have a bottom line: a single qualitative or (ideally) quantitative take-away**
    ("measurement error in consumption data explains 27% of the equity premium puzzle"),
    not a description of what you did ("estimated the consumption CAPM with high-quality
    data").
22. Conclusion checklist: the audience cares about the question, understands how you
    answer it, knows why to believe you, walks out knowing what you learned.
23. **"Practice. A lot. Give talks whenever you can."**

## Applies to economics seminar / job talk?

Yes — written for exactly that (applied micro seminar / job market talk). Prescribes the
standard applied-micro order: motivation → question → this paper → preview → data → model
+ identification → vulnerabilities → results → bottom line. Weighted toward empirical
papers with an estimating equation.

## Conflicts with common advice

- **Previews results** (slide 5) — Cochrane says never preview.
- **"No pauses"** — Shengwu Li and EUI say pause after every equation / between sections.
- No literature-review slide at all; the "this paper" slide carries the value-added.
- Nothing on time budgets in minutes, slide counts, or an outline slide.
