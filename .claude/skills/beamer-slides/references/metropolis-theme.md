# Matthias Vogelgesang — the *metropolis* Beamer theme (formerly "m")

- Author: Matthias Vogelgesang and contributors. v1.2, 2017-01-23 (last release; repo still maintained). CC-BY-SA 4.0.
- URLs used: https://github.com/matze/mtheme (README read via raw file; repo has `source/metropolistheme.dtx`, `doc/`,
  `demo/`, `docker/`, `Makefile`, `NEWS`); manual
  http://mirrors.ctan.org/macros/latex/contrib/beamer-contrib/themes/metropolis/doc/metropolistheme.pdf (read in full,
  incl. implementation); NEWS file (read). On CTAN as `beamertheme-metropolis` (in TeX Live since 2016).
- Design goal: "minimize noise and maximize space for content; the only visual flourish it offers is an (optional)
  progress bar." Blog post: bloerg.net/2014/09/20/a-modern-beamer-theme.html.

## Usage and options

1. `\usetheme[opt=val,...]{metropolis}`; change options mid-deck with `\metroset{opt=val}`.
2. `titleformat` = regular | smallcaps | allsmallcaps | allcaps (default regular); also per element
   `titleformat title/subtitle/section/frame` and `titleformat plain` (standout frames).
3. `sectionpage` = none | simple | progressbar (default progressbar); `subsectionpage` = none (default) | simple | progressbar.
4. `numbering` = none | counter (default) | fraction (frame number bottom-right).
5. `progressbar` = none (default) | head | frametitle | foot.
6. `block` = transparent (default) | fill (light grey block background); `background` = light (default) | dark.
7. `\begin{frame}[standout] ... \end{frame}`: large centred text on inverted background (replaces deprecated `\plain`).
   Put `label=` **before** `standout` in the option list, else "label undefined".
8. `\maketitle` makes a `[plain,noframenumbering]` title frame; section pages are unnumbered.
9. `appendixnumberbeamer` + `\appendix`: metropolis turns off numbering and progress bars in the appendix automatically.
10. `\mreducelistspacing` removes the vertical space before lists.
11. pgfplots: styles `mlineplot`, `mbarplot`, `horizontal mbarplot`, `disable thousands separator`; colour theme
    `pgfplotsthemetol` based on **Paul Tol's** colour-blind-safe palette (TolDarkBlue `#6699CC`, TolLightBlue `#88CCEE`,
    TolLightGreen `#44AA99`, TolDarkGreen `#117733`, TolDarkBrown `#999933`, TolLightBrown `#DDCC77`, TolDarkRed
    `#661100`, TolLightRed `#CC6677`, TolLightPink `#AA4466`, TolDarkPink `#882255`, TolLightPurple `#AA4499`, TolDarkPurple `#332288`).
12. Pandoc: `pandoc -t beamer --latex-engine=xelatex -V theme:metropolis`.

## Fonts

13. Default **Fira Sans** (Light as regular, Regular as bold, Light Italic) and **Fira Mono** via `fontspec`; requires
    **XeLaTeX or LuaLaTeX**; with pdfLaTeX it silently falls back to Computer Modern sans (warning "You need to compile
    with XeLaTeX or LuaLaTeX to use the Fira fonts"). Linux package names "Fira Sans OT" are handled.
14. Change font after loading the theme: `\setsansfont{Ubuntu}` `\setmonofont{Ubuntu Mono}`; for big rooms or weak
    projectors use a heavier weight: `\setsansfont[BoldFont={Fira Sans SemiBold}]{Fira Sans Book}`.
15. Old-style figures in text but lining in maths: `\usefonttheme{professionalfonts}` + `mathspec` +
    `\setsansfont[BoldFont={Fira Sans},Numbers={OldStyle}]{Fira Sans Light}` + `\setmathsfont(Digits)[Numbers={Lining,Proportional}]{Fira Sans Light}`.
16. Font sizes set by the theme: title `\Large`, frametitle `\large`, section title `\Large`, block title `\normalsize`,
    author/date/caption `\small`, page number `\scriptsize`, standout `\Large`.

## Colours

17. Everything derives from three Beamer colours: `normal text` (fg `mDarkTeal #23373b`, bg `black!2`), `alerted text`
    (fg `mLightBrown #EB811B`, orange), `example text` (fg `mLightGreen #14B03D`). Also defined `mDarkBrown #604c38`.
    Frametitle = inverted normal text (dark teal bar, light text). Progress bar = alerted colour.
18. Redefine with `\setbeamercolor{normal text}{fg=...,bg=...}`, `\setbeamercolor{alerted text}{fg=...}`,
    `\setbeamercolor{progress bar}{...}`, `title separator`, `progress bar in head/foot`, `progress bar in section page`.
19. v1.2 added a `highcontrast` colour theme; dark variant via `background=dark`; Ross Churchley's `owl` as an alternative dark theme.

## Known pitfalls (manual §6, NEWS)

20. `allsmallcaps`/`allcaps` use `\MakeLowercase`/`\MakeUppercase`: `\\` breaks, numerals stay full height, maths
    letters get altered (`\mathbb`, `\mathcal`), macros may not expand — use only for plain alphabetic titles.
21. Computer Modern sans (pdfLaTeX) has no small caps, so `smallcaps` options do nothing there.
22. Mixing with another colour theme: load `\useoutertheme`, `\useinnertheme`, `\usefonttheme{metropolis}` separately,
    then `\usecolortheme{seahorse}`; `\usetheme{metropolis}` + `\usecolortheme{...}` gives wrong frametitle colours.
    Metropolis sets no title background, so `whale`-style themes with white title text break.
23. `show notes on second screen` with XeLaTeX: text after the first section slide can turn white — compile with
    LuaLaTeX or add the `\beamer@framenotesbegin` patch from the manual.
24. Standout frames exported from Org mode get labels after the option → use `:BEAMER_opt: label=x,standout`.
25. Pandoc < 1.17.2 cannot create `{.standout}` frames.
26. Old (beta) option names still work as aliases: `usetitleprogressbar`, `noslidenumbers`, `usetotalslideindicator`,
    `nosectionslide`, `darkcolors`, `blockbg`.
27. Beamer 3.37+ issues fixed only in v1.2 — update the CTAN package. Progress bar once produced "Overfull \hbox (0.4pt)".
28. Block fill is light grey; `alertblock` title uses the orange alert colour, so `\alert` inside the title is redundant.

## Conflicts with other sources

- Dark-teal frametitle bar and progress bar are exactly the "chrome" Doumont and Tufte would remove; Butts and
  Goldsmith-Pinkham keep a near-empty header instead.
- Fira Light as the body weight is thin for large rooms; Alley wants bold sans; the manual itself suggests Fira Sans Book + SemiBold for that case.
- Orange `#EB811B` alert on dark teal reads well; against Goldsmith-Pinkham's Okabe–Ito set it is close to his
  vermilion (213,94,0), so both can coexist in figures.
- Section pages with progress bars are a roadmap device Goldsmith-Pinkham leaves optional and Doumont discourages.
