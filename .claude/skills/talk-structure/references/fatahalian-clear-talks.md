# Fatahalian — Tips for Giving Clear Talks

- Author: Kayvon Fatahalian (CMU at the time; now Stanford)
- Title: "Tips for Giving Clear Talks"
- Year: undated (examples cited through 2013; c. 2014)
- URL used: https://www.csie.ntu.edu.tw/~johnson/files/cleartalktips.pdf
- Length: ~60 slides, 13 numbered tips plus wrap-up; examples from SIGGRAPH/HPG talks

## Concrete rules

1. Two good reasons to give a talk: convey to peers "the most important things they
   should know, but probably do not" (not "what you did"), and get feedback ("put smart
   people in the best position to help you"). Bad reasons: reward for acceptance; lab
   meeting obligation.
2. Cost of an unclear talk: **1 hour × 20 people = 20 person-hours**, plus lost feedback,
   collaborations, impact. Benefit of a clear one is non-linear: people read the paper,
   come up afterwards, remember you ("are you looking for a postdoc?").
3. **Tip 1 — Choose your audience and strive for clarity within it.** "You should aim for
   your target audience to understand everything you say (if they won't understand, why
   are you saying it?)". Experts "haven't been thinking about your problem 60 hours a
   week". It is reasonable to target two audiences: experts who get everything and a
   broader audience that gets all but the most technical 20%.
4. **Tip 2 — "The audience prefers not to think (much)."** Finite mental effort: they do
   not want to decode figures or infer things you can just tell them; they *do* want to
   spend effort on limitations, implications, connections to their own work.
5. **Tip 3 — "Every sentence matters."** If you can't justify how a sentence helps the
   listener, take it out.
6. **Tip 4 — The intro exists to tell the listener "Here is the right way to think about
   the problem I am trying to solve."** Not academic completeness; no one cares if
   citations are comprehensive. A related-work slide that says only "others have worked
   on this" has no value for experts (they know) or non-experts (they won't learn it).
7. Best hook: **make them aware of something they didn't know they didn't know** ("You
   might think you know this, but here's a new angle"). Example: Feltman et al. 2012
   opened with "Do you know whether it is more efficient to trace a shadow ray from the
   light or from the surface? It's not so obvious, is it?" Audience reaction: "this person
   has something to say. I should not check email."
8. Discuss related work **inside your framing of the space**. "If done well, your solution
   will seem obvious given this framing. And that's a good thing!"
9. **Tip 5 — Establish inputs, outputs and constraints (goals and assumptions) early.**
   Your contribution is what meets the stated goals under the stated constraints.
10. **Tip 6 — Surprises in the narrative are bad: say where you are going and why before
    you say what you did.** "Why before what" lets listeners compartmentalise (critical
    idea or implementation detail?) and re-engage if lost. In results: "Key questions to
    ask about our approach are..." before showing them.
11. Ideally the audience can always **anticipate** what you'll say next. If, rehearsing,
    *you* keep forgetting what the next slide is, restructure: the narrative isn't there.
12. **Tip 7 — Organise with section slides** ("I'm about to frame the problem", "let's
    talk about our solution", "how well it works", "alternatives we didn't go with", "let
    me wrap up"). They mark shifts, let lost listeners re-engage, give everyone a breath,
    and split the talk into practice units.
13. **Tip 8 — "Always, always, always explain any figure or graph."** Name every visual
    element and highlight colour; walk through the key points; "As you can see..." is
    "verbal eye contact". For a results graph: say what question it answers, describe the
    axes (which must be labelled), then make the one point.
14. **Tip 9 — In the results: "One point per slide! ... (and the point is the title of
    the slide!!!)"** Corollary: show no data on the slide unrelated to the point, which
    usually means **remaking the paper's graphs** for the talk.
15. **Tip 10 — Titles matter.** Read the slide titles alone, in thumbnail view: they
    should be a great summary of the talk ("one point per slide for the whole talk").
16. **Tip 11 — End on a positive note.** Future work framed as "the future is bright /
    this is one part of something bigger", not a list of problems or obvious next steps
    ("Audience: well, that was a bit incremental"). The conclusion is where you contribute
    reflective intellectual thought; his own final SIGGRAPH slide took ~4 hours.
17. **Tip 12 — Speaker notes for transitions only**: put the first half of the sentence
    about slide X in the notes for slide X−1 (click mid-sentence). Don't script figure
    explanations.
18. **Tip 13 — "The audience is always right"** on practice-talk feedback. Don't say "I
    mentioned that here"; if it wasn't understood, it's your fault. Ask: "What could I
    have said to make that point more clear?"
19. Learn the principles before you break them (Dalí painted realistically at 22).
    Lab-meeting talks and conference talks need very different polish; tips 9's corollary
    and 12 are for polished talks.

## Applies to economics seminar / job talk?

Written for computer graphics conference talks (20 min, uninterrupted) but almost all of
it transfers: framing-not-citations intro, why-before-what, section slides, explain every
figure, one point per slide with the point as title, titles-as-summary, end positive.
No guidance on time budget, slide counts, or interruptions.

## Conflicts with common advice

- **Section/outline slides are useful** vs. Peyton Jones's "no outline".
- "End on a positive note" with future work vs. Cochrane ("strike 'I leave x for future
  research'") and Winston (end on contributions).
- Scripted transitions in speaker notes vs. Bellemare/Tabarrok "don't memorise".
