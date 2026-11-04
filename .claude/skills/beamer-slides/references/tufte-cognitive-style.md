# Edward Tufte — *The Cognitive Style of PowerPoint: Pitching Out Corrupts Within*

- Author: Edward R. Tufte. Graphics Press. 1st ed. May 2003 (24 pp.), revised Sept 2003 (28 pp.), 2nd ed. 2006
  (32 pp.; adds "PowerPoint does rocket science", a longer discussion of causes, and an essay on lists; also a chapter
  of *Beautiful Evidence*, 2006).
- URLs used: https://www.edwardtufte.com/tufte/powerpoint (404 now); read instead the Ask E.T./notebook threads
  https://www.edwardtufte.com/notebook/new-edition-of-the-cognitive-style-of-powerpoint/,
  https://www.edwardtufte.com/notebook/powerpoint-does-rocket-science-and-better-techniques-for-technical-reports/,
  https://www.edwardtufte.com/notebook/columbia-accident-investigation-board-the-boeing-powerpoint-slide/;
  secondary summaries https://www.davidmays.org/BookNotes05/TufCogn.html,
  https://qahiccupps.blogspot.com/2018/08/tufte-cognitive-style-of-powerpoint.html,
  https://conversableeconomist.com/2023/06/01/how-powerpoint-and-other-slide-presentations-can-inhibit-thinking/,
  https://www.overleaf.com/articles/implementing-the-teachings-of-edward-tufte/hvtnngcxqnwm. The booklet itself is
  not online; page numbers below are as cited in those threads.

## The argument

PowerPoint is **presenter-oriented**, not content- or audience-oriented. Its default cognitive style: foreshortening of
evidence and thought; **low spatial resolution**; a deeply hierarchical single-path (bullet outline) structure;
breaking narrative and data into arbitrary slide-sized fragments shown in rapid temporal sequence instead of
side-by-side in space; preoccupation with format over content; an attitude of marketing/pitching ("pitching out
corrupts within"). Thin content gets dressed in "PP Phluff" (decoration, logos, clip art, bullets, frames), which
makes talks more boring, which invites more Phluff. Audience boredom is a content failure, not a decoration failure.
The format has crept into paper reports and web pages ("creeping PowerPoint").

## Numbers

1. A typical slide shows ~**40 words**, about **8 seconds** of silent reading (p. 15).
2. Only **40–60 %** of slide area carries unique content; the rest is Phluff, bullets, frames, branding (p. 15).
3. PP printouts carry **2–10 %** of the typographic richness (characters per square inch) of non-fiction bestsellers;
   popular news sites show **10–15×** more information per screen than a set of PP images (table p. 22).
4. "The PP slide format has probably the worst signal/noise ratio of any known method of communication on paper or screen."
5. Median data matrix behind a graphic in *Science*: **> 1,000 numbers**; PP statistical graphics typically show a handful.
6. Templates probably help **10–20 %** of presentations and cause detectable intellectual damage to **80 %**; for
   statistical data the damage "approaches dementia". ~10^10–10^11 slides are produced per year.
7. Splitting 1,855 numbers over 155 slides leaves ~1/155 of the possible pairwise comparisons visible (Mahajan).
8. Gettysburg Address as PowerPoint (Norvig): 6 slides for a 2-minute talk.
9. Boeing/Columbia (Jan 2003, Debris Assessment Team briefing): title "Review of Test Data Indicates Conservatism for
   Tile Penetration"; 5 levels of bullet hierarchy; the key fact — the foam strike was ~**640×** outside the test
   database (1,920 cubic inches vs 3 cubic inch test pieces) — sits in the smallest type near the bottom; the vague
   word "significant(ly)" appears 5 times; executive readers saw only the reassuring headline. CAIB report p. 191:
   "The Board views the endemic use of PowerPoint briefing slides instead of technical papers as an illustration of the
   problematic methods of technical communication at NASA." Both the CAIB (2003) and the Return to Flight Task Group
   (2005) concluded PP is an inappropriate tool for engineering reports, presentations, documentation.

## Prescriptions

1. Never use PowerPoint templates to array words or numbers; never let the tool decide the format or the sequence.
2. Write **sentences with subjects and verbs**, not bullet fragments; bullets convey only sequence, priority or set
   membership and hide causal structure and analytic reasoning ("Sentences have agency").
3. Avoid bullet hierarchies deeper than one level; "What is important is high on the page."
4. Do not read slides aloud; never use PP to format reports or web pages.
5. For serious talks, hand out a **paper technical report**: an 11 × 17 in sheet folded once (4 pages) holds what
   50–100 slides hold; audience reads it (silently, at ~2–3× speaking speed) at the start, then discuss
   ("meetings get ~20 % shorter"). Model: *Science* one-page reports, Toyota A3.
6. Tables: a slide table of 10–20 numbers is a "low-resolution" fragment; show the full data matrix on paper at
   high resolution; put related comparisons **adjacent in space**, not stacked in time.
7. Use PowerPoint only as a **projector** for colour images, graphics, video that cannot be printed, and for
   low-resolution material (large type, single picture); not for evidence.
8. Be skeptical of speakers who rely on PP style; thin slides may hide thin content. Improve content first.

## Conflicts with other sources

- Tufte rejects the one-message-per-slide, large-type, low-density premise that Alley, Doumont and Schwabish share;
  he wants high-resolution handouts and silent reading, they want slides read in a glance.
- Agrees with Alley and Doumont on full sentences instead of bullets and on hiding nothing important in small type.
- His "numbers belong on paper" conflicts with Goldsmith-Pinkham's and Butts' row/column-reveal regression tables.
- His 11 × 17 handout is impractical in a seminar; the usable residue for Beamer: no deep bullet trees, sentence
  titles, important results big and high on the slide, one coherent figure rather than three fragments.
