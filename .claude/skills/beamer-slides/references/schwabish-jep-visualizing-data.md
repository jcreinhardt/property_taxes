# Jonathan A. Schwabish — "An Economist's Guide to Visualizing Data"

- Author: Jonathan A. Schwabish (then CBO; now Urban Institute / PolicyViz).
- Journal of Economic Perspectives 28(1), Winter 2014, pp. 209–234. DOI 10.1257/jep.28.1.209. JEL A11, C80.
- URL used: https://www.aeaweb.org/articles?id=10.1257/jep.28.1.209 (abstract page fetched). The PDF itself
  (https://pubs.aeaweb.org/doi/pdfplus/10.1257/jep.28.1.209) returned 403 / a bot-check page to every fetch
  method allowed here, so the content below is reconstructed from two teaching decks that walk through the paper
  figure by figure — Max Kasy, "Data visualization" (Topics in Econometrics 2019,
  https://maxkasy.github.io/home/files/teaching/TopicsEconometrics2019/DataVisualization-Slides.pdf) and the UIC SURF
  "Data Visualization" deck (https://surf.econ.uic.edu/wp-content/uploads/sites/882/2023/05/Data_Visualization.pdf) —
  plus the AEA Research Highlights interview (https://www.aeaweb.org/research/data-visualization-for-economists-jonathan-schwabish).
  Treat exact wording as paraphrase.

## The argument

"Once upon a time, a picture was worth a thousand words"; with online news, blogs and social media it is worth more.
Economists write for each other and rarely think about the reader; a graph is for the reader (or seminar audience),
not the author. Default software output (Excel, Stata) is the enemy. Three principles organise everything:

## Rules

1. **Show the data.** Present data in graphs, not only tables; pick the simplest graph that tells the story; do
   *not* show all the data — highlight the series the text discusses.
2. **Reduce the clutter.** Remove dark or heavy gridlines, unnecessary tick marks, labels or text, icons and pictures,
   ornamental shading and gradients, and unnecessary dimensions (3-D).
3. **Integrate the text and the graph.** Put title, units, legend and annotations on the graph (top-left), label series
   directly instead of a detached legend; do not narrate in the text what the graph already labels; a graph should be
   self-contained enough to stand alone.
4. Start column/bar charts at zero (length encodings need a zero baseline); dots along a scale need not.
5. Do not use different colours for bars of one series; colour only what you want to emphasise (dark = focus, light/grey = context).
6. Never use 3-D; it distorts and encodes no data.
7. Avoid pie charts (angles/areas are read inaccurately; donuts are worse) — use (horizontal) bars; for two pies use
   paired columns, a stacked bar, or a slope chart.
8. Spaghetti charts (many lines, markers on every point, legend far away and out of order) → small multiples /
   sparkline panels with light/dark contrast, labels at line ends instead of y-axes.
9. Encode like quantities the same way (no diamonds-vs-bars for men vs women); connect pairs with lines; show the
   average as a distinct hollow marker.
10. Rotate to horizontal bars when category labels are long, so labels sit in the chart.
11. Colour: avoid default palettes; build with Adobe Kuler, ColorBrewer 2.0, ColorSchemeDesigner, Colrd; check
    colour-blind rendering (Vischeck/Oracle) and greyscale consistency (ColorBrewer).
12. Fonts: avoid the "boring and overused" Arial, Calibri, Times New Roman; look at Font Squirrel / Google Fonts.
13. Invest time in the details of graphical presentation; consider the audience's needs (a 25-page paper may need to
    become an 800-word blog post, a 3-page brief, or a tweet).

## Before/after examples (the "eight graphs transformed")

| # | Before | After |
|---|---|---|
| 1 | Line chart with heavy grid, legend box, clutter | Lightened grid, direct labels, title integrated |
| 2 | Regression coefficient plot (long-run unemployment vs SNAP caseloads, 4 groups): darkest line is the 0% gridline, duplicated y-axis labels, % signs, cryptic group codes AO/NC/WE/SS | Coefficient line darkest, gridlines lightened, duplicate axes and % signs dropped, repeated titles moved to one common title, groups spelled out |
| 3 | Scatter of countries' revealed comparative advantage vs schooling: every country labelled, haystack of dots | Only the 5 countries discussed (China, Costa Rica, Philippines, Malaysia, Thailand) labelled in full and darkened; rest light |
| 4 | Column chart not starting at zero, a different colour per bar | Zero baseline, rotated to horizontal bars, labels integrated |
| 5 | 3-D column chart with detached legend | 3-D removed, legend integrated, common baseline for group comparison |
| 6 | "Unbalanced" OECD chart: men as diamonds vs women as bars, gradient-shaded columns, heavy grid, vertical labels | Same encoding for both sexes, connecting lines, horizontal country labels in chart, OECD average as hollow circle, title/units/legend top-left |
| 7 | Spaghetti line chart, markers everywhere, legend out of order | Small multiples with one highlighted series each, labels at the line ends |
| 8 | Pie chart (and two pies side by side) | Bar chart with "+" signs showing the parts sum to 100%; paired columns / stacked bar / slope chart for the comparison |

## Tables and slides

The paper is about graphs for papers and talks; it has no separate slide section. Its presentation advice is implicit:
the same three principles apply to any projected graph, with larger type and fewer series.

## Conflicts with other sources

- Tufte prefers dense high-resolution tables/graphics; Schwabish's "do not show all the data" and "highlight few series"
  is a reader-first simplification Tufte would call foreshortening of evidence.
- Doumont's graph summary agrees on everything (bars from zero, dots need not, no pie, no separate legend, light grid).
- Schwabish says avoid Arial/Calibri; Alley recommends Calibri/Arial for projection speed and ubiquity.
