# Kyle Butts — LaTeX slide template (`latex-slides`)

- Author: Kyle Butts. Repo: https://github.com/kylebutts/templates (fetched 2026-11-04 via raw.githubusercontent.com,
  branch master). Folders: `latex-slides/` (`slides.sty` 675 lines, `slides.tex` demo 446 lines, `base.tex` minimal
  39 lines, `math.sty` 205 lines, `references.bib`, `figures/`, `slides.pdf`, `base.pdf`), `latex-article/`,
  `referee-response/`, `thumbnails/`, `render.R`, `README.md`.
- README: "nice typography, minimal aesthetics so the focus is on the content, and a set of helpful commands including
  easily colored words, full-size image frames, table highlighting, multi-column layouts"; customise by defining
  `accent`/`accent2` before `\usepackage{slides}`; "I recommend changing the top slider on https://hslpicker.com".

## What the template contains

1. Document class `[aspectratio=169,t,11pt,table]{beamer}` — 16:9, top-aligned frames, 11pt, `table` option for `\rowcolor`.
2. No Beamer theme: default theme stripped (navigation symbols off, white canvas, 20pt side margins, custom frametitle in a
   light grey box `zinc50`, custom title page, custom footline showing only the frame number in `\scriptsize\ttfamily` grey).
3. Fonts: `\usefonttheme{professionalfonts}`; **Libertine** (serif) + **IBM Plex Mono** (scale 0.88) + `microtype`;
   commented alternative: Roboto sans. Line spacing `\setstretch{1.3}`.
4. Colours: Tailwind **zinc** greys (zinc50…zinc950) for all chrome; two theme colours `accent` (default `#006896`) and
   `accent2` (`#E64173`; `\alert` uses accent2); a named palette navy `#002C55`, raspberry `#940034`, cranberry
   `#B3114B`, orange `#D65616`, purple `#5C4CBF`, blue `#0188AC`, green `#2DB25F`, rose `#FB7185`, yellow `#ffc517`
   with text commands `\navy{}`… and lightened background boxes `\bgNavy{}`… (`bgNavy #b2e3ff`, `bgRaspberry #ff96a9`, …).
5. Hyperlinks coloured `accent2`; buttons white with grey/accent text, monospace.
6. `\bottomleft{}` / `\bottomright{}` (textpos) to park `\beamergotobutton` links at the frame bottom; `appendixnumberbeamer`.
7. `\AtBeginSection` / `\AtBeginSubsection` show a plain, unnumbered TOC frame with the current section highlighted (shaded others at 60 %).
8. Lists: itemize marker `$\rightarrow$`, sub-items squares, small grey markers, tightened left margins; enumerate `1.`.
9. Blocks: `tcolorbox` framed block (zinc50 fill, zinc200 frame, square corners); `zincBlock`, `purpleBlock`, `cranberryBlock`.
10. Maths: `\tcbhighmath[colback=bgColor]{...}` highlight inside equations (default bgRaspberry); `amsmath/amsfonts/amsthm/bm`;
    a fix so `\pause` works inside `align`.
11. Tables: `booktabs`, `adjustbox` (`max width=\textwidth`), `tabularx` L/R/C columns, `dcolumn` `d` type,
    `\note[Notes.]{...}` table-note command, `\tabletitle[Outcome:]{...}`; **`\cellcolor<k>{}`/`\rowcolor<k>{}` with overlay
    specs**; `\marktopleft{id}`/`\markbottomright{id}` draw an accent box around cells (wrap in `\only<k>`).
12. Figures: `\imageframe{file}` full-bleed plain frame (make the figure the frame's aspect ratio); `subfigure`.
13. `transitionframe` (from paulgp): dark accent background, white `\LARGE` centred text, unnumbered plain frame.
14. TikZ page coordinate system `(page cs: x, y)` with x,y in [0,1] for absolute overlays (arrows, call-out boxes with
    fuzzy shadow), plus `\devgrid` to show a 0.1 grid while positioning.
15. `codeblock` listing environment (needs `[fragile]`), biblatex `ext-authoryear-ecomp` with `\citecolor{}` for grey footnote-size citations.
16. Demo usage: title frame `[noframenumbering,plain]`; two-column frames `.5/.5` or `.6/.4` with `[T]`; references
    frame `[allowframebreaks,noframenumbering]` before `\appendix`; appendix frame labelled and linked back with `\beamerreturnbutton{Back}`.

## `slides.tex` preamble (verbatim)

```latex
\documentclass[aspectratio=169,t,11pt,table]{beamer}
\usepackage{slides,math}

% Optionally define `accent`/`accent2` colors for theme customization
% I recommend changing the top slider on this: https://hslpicker.com/#1e9400
\definecolor{accent}{HTML}{940034}
\definecolor{accent2}{HTML}{006896}

\title{What is the Effect of X on Y?}
\date{\today}
\author{Kyle Butts}
\addbibresource{references.bib}
```

## `slides.sty` — key blocks (verbatim excerpts; omitted: bibliography fonts, `codeblock`, `\devgrid` body, coloured blocks)

```latex
% Latex Programming tools ------------------------------------------------------
% \AtBeginEnvironment
% https://tex.stackexchange.com/questions/33576/conditional-typesetting-build
\usepackage{etoolbox}
\usepackage{xparse}

% Remove annoying over-full box warnings
\vfuzz=2pt 
\hfuzz=2pt

% Define colors ----------------------------------------------------------------
\usepackage{xcolor}

% Define `accent`/`accent2` colors for theme customization
% If accent/accent2 are not defined, then use defaults
\providecolor{accent}{HTML}{006896}
\providecolor{accent2}{HTML}{E64173}

% Fonts ------------------------------------------------------------------------
% Beamer Option to use custom fonts
\usefonttheme{professionalfonts}
% T1 removes errors for things like \textbackslash
\usepackage[T1]{fontenc}
\usepackage{inputenc}

% Serif option:
% Libertine for serif text font
\usepackage{libertine}
% Source Code Pro for monospace font
\usepackage[scale=0.88]{plex-mono}
\renewcommand{\ttdefault}{\plexmonofamily}

% Sans-serif option:
% Roboto for serif text font
% \usepackage[sfdefault]{roboto}
% Source Code Pro for monospace font
% \usepackage[scale=0.95]{plex-mono}
% \renewcommand{\ttdefault}{\plexmonofamily}

% Small adjustments to text kerning
\usepackage{microtype}

% Beamer Options ---------------------------------------------------------------
% Background
\setbeamercolor{background canvas}{bg = white}

% Change text margins
% [... frametitle template, options 1/2 ...]
\setbeamerfont{framesubtitle}{size = \small, shape = \itshape}

% Page numbering
% Slightly modified for nicer page numbering
\setbeamercolor{page number in head/foot}{fg=zinc600}
\setbeamerfont{footline}{size=\scriptsize, family=\ttfamily}
\defbeamertemplate{footline}{frame number spacing}{
  \hfill%
  \usebeamercolor[fg]{page number in head/foot}%
  \usebeamerfont{page number in head/foot}%
  \strut\usebeamertemplate*{page number in head/foot}\kern0.6em\vskip1.4pt%
}
[action]
{
  \setbeamertemplate{page number in head/foot}[totalframenumber]%
}
\setbeamertemplate{footline}[frame number spacing]{}

% Appendix numbering
\usepackage{appendixnumberbeamer}
\renewcommand\appendixname{Appendix}

% Button 
\setbeamercolor{button}{bg = white, fg = zinc600!80!accent}
\setbeamerfont{button}{family = \ttfamily}
\setbeamercolor{button border}{fg = black}

% Let's you put \beamerbutton in bottom right corner
\usepackage[absolute,overlay]{textpos}
\newcommand\bottomleft[1]{%
  \begin{textblock*}{\paperwidth}(0pt,{\dimexpr\textheight + 5pt})
    \hspace{8pt}\raggedright #1
  \end{textblock*}
}
\newcommand\bottomright[1]{%
  \begin{textblock*}{\paperwidth}(0pt,\textheight)
    \raggedleft #1\hspace{8pt}
  \end{textblock*}
}

% Remove navigation symbols
\setbeamertemplate{navigation symbols}{}

% Table and Figure captions
\setbeamercolor{caption}{fg = zinc900!70!white}
\setbeamercolor{caption name}{fg=zinc900}
\setbeamerfont{caption name}{shape = \itshape}

% Line spacing
\usepackage{setspace}
% [... caption colours, spacing ...]
\AtBeginDocument{\setlength\belowdisplayskip{\bigskipamount}}
\AtBeginDocument{\setlength\abovedisplayshortskip{\medskipamount}}
\AtBeginDocument{\setlength\belowdisplayshortskip{\bigskipamount}}


% Title page -------------------------------------------------------------------
\setbeamercolor{title}{fg = zinc900}

% Table of Contents with Sections ----------------------------------------------
\setbeamerfont{section in toc}{size = \large, series = \bfseries}
\setbeamerfont{section in toc shaded}{size = \large}
\setbeamercolor{section in toc}{fg = zinc800}
\setbeamertemplate{section in toc shaded}[default][60] % opacity
\setbeamercolor{subsection in toc}{fg = zinc800}
\setbeamertemplate{subsection in toc shaded}[default][60] % opacity

\AtBeginSection[]{
  {
    \hypersetup{hidelinks}
    \setbeamercolor{frametitle}{bg=white}
    \begin{frame}[noframenumbering,plain,c]{}
      \tableofcontents[currentsection,currentsubsection]
    \end{frame}
  }
}
\AtBeginSubsection[]{
% [... TOC, bullets ...]
% Hyperref ---------------------------------------------------------------------
\usepackage{hyperref}
\hypersetup{
  colorlinks = true,
  linkcolor = accent2,
  filecolor = accent2,
  urlcolor = accent2,
  citecolor = accent2,
}

% Kyle's colors ----------------------------------------------------------------
% Zinc from Tailwind Colors
\definecolor{zinc50}{HTML}{fafafa}
\definecolor{zinc100}{HTML}{f4f4f5}
\definecolor{zinc200}{HTML}{e4e4e7}
\definecolor{zinc300}{HTML}{d4d4d8}
\definecolor{green}{HTML}{2DB25F}
\definecolor{rose}{HTML}{FB7185}
\definecolor{yellow}{HTML}{ffc517}

% Easily color text
\newcommand\navy[1]{{\color{navy}#1}}
\newcommand\raspberry[1]{{\color{raspberry}#1}}
\newcommand\cranberry[1]{{\color{cranberry}#1}}
\newcommand\orange[1]{{\color{orange}#1}}
\newcommand\purple[1]{{\color{purple}#1}}
\newcommand\blue[1]{{\color{blue}#1}}
\newcommand\green[1]{{\color{green}#1}}
\newcommand\rose[1]{{\color{rose}#1}}
\newcommand\yellow[1]{{\color{yellow}#1}}

% lightened via: https://uicolors.app/create
\definecolor{bgNavy}{HTML}{b2e3ff}
\definecolor{bgRaspberry}{HTML}{ff96a9}
\definecolor{bgCranberry}{HTML}{fda4d0}
\definecolor{bgOrange}{HTML}{f6b97b}
\definecolor{bgPurple}{HTML}{adb4f4}
\definecolor{bgBlue}{HTML}{cdfbff}
\definecolor{bgGreen}{HTML}{8ee7af}
\definecolor{bgRose}{HTML}{fecdd4}
\definecolor{bgYellow}{HTML}{ffea88}

% Color background of text
\newcommand\bgNavy[1]{\colorbox{bgNavy}{#1}}
\newcommand\bgRaspberry[1]{\colorbox{bgRaspberry}{#1}}
\newcommand\bgCranberry[1]{\colorbox{bgCranberry}{#1}}
\newcommand\bgOrange[1]{\colorbox{bgOrange}{#1}}
\newcommand\bgPurple[1]{\colorbox{bgPurple}{#1}}
\newcommand\bgBlue[1]{\colorbox{bgBlue}{#1}}
\newcommand\bgGreen[1]{\colorbox{bgGreen}{#1}}
\newcommand\bgRose[1]{\colorbox{bgRose}{#1}}
\newcommand\bgYellow[1]{\colorbox{bgYellow}{#1}}

% old colors: have fallen out of grace
% \definecolor{ruby}{HTML}{9a2515}
% \definecolor{alice}{HTML}{107895}
% \definecolor{daisy}{HTML}{EBC944}
% \definecolor{coral}{HTML}{F26D21}
% \definecolor{kelly}{HTML}{829356}
% \newcommand\kelly[1]{{\color{kelly}#1}}
% \newcommand\ruby[1]{{\color{ruby}#1}}
% \newcommand\alice[1]{{\color{alice}#1}}
% \newcommand\daisy[1]{{\color{daisy}#1}}
% \newcommand\coral[1]{{\color{coral}#1}}
% \newcommand\bgKelly[1]{{\colorbox{kelly!80!white}{#1}}}
% \newcommand\bgRuby[1]{{\colorbox{ruby!80!white}{#1}}}
% \newcommand\bgAlice[1]{{\colorbox{alice!80!white}{#1}}}
% \newcommand\bgDaisy[1]{{\colorbox{daisy!80!white}{#1}}}
% \newcommand\bgCoral[1]{{\colorbox{coral!80!white}{#1}}}
% [... tcolorbox/tikz, blocks, listings ...]

% Tables too narrow
% \begin{tabularx}{\linewidth}{cols}
% col-types: X - center, L - left, R -right
% Relative scale: >{\hsize=.8\hsize}X/L/R
\usepackage{tabularx}
\newcolumntype{L}{>{\raggedright\arraybackslash}X}
\newcolumntype{R}{>{\raggedleft\arraybackslash}X}
\newcolumntype{C}{>{\centering\arraybackslash}X}

% Table notes with:
% - \note{...},
% - \note[Source:]{...}, or
% - \note[]{0.7\textwidth}{...}
\RenewDocumentCommand{\note}{O{Notes.} m g}{%
  \IfNoValueTF{#3}{%
    \footnotesize\vspace*{\medskipamount}%
    \noindent\emph{#1} #2
  }{%  
    \parbox{#2}{\footnotesize\vspace*{\medskipamount}%
      \noindent\emph{#1} #3
    }%
  }
}

% Since we don't use `table` environments in slides
\NewDocumentCommand{\tabletitle}{O{Outcome:} m}{%
  {\footnotesize\textsc{#1} #2 \\[\medskipamount]}
}

% Table Highlighting -----------------------------------------------------------

% Create top-left and bottom-right markets in tabular cells with a unique matching id and these commands will outline those cells
\usepackage[beamer,customcolors]{hf-tikz}
\usetikzlibrary{calc}
\usetikzlibrary{fit,shapes.misc}

% To set the hypothesis highlighting boxes red.
\newcommand\marktopleft[1]{%
  \tikz[overlay,remember picture] 
      \node (marker-#1-a) at (0,1.5ex) {};%
% \imageframe{img_name}
% from https://github.com/mattzinc900well/cousteau
\newcommand{\imageframe}[1]{%
    \begin{frame}[plain]
        \begin{tikzpicture}[remember picture, overlay]
            \node[at = (current page.center), xshift = 0cm] (cover) {%
                \includegraphics[keepaspectratio, width=\paperwidth, height=\paperheight]{#1}
            };
        \end{tikzpicture}
    \end{frame}%
}

% subfigures
\usepackage{subfigure}

% Highlight slide --------------------------------------------------------------
% \begin{transitionframe} Text \end{transitionframe}
% from paulgp's beamer tips
\newenvironment{transitionframe}{
    \setbeamercolor{background canvas}{bg=accent!60!black}
    \begin{frame}[noframenumbering,plain,c]\color{white}\LARGE\centering
}{
    \end{frame}
}

% Define custom slide coordinate system ----------------------------------------
% https://tex.stackexchange.com/questions/89588/positioning-relative-to-page-in-tikz
% Defining a new coordinate system for the page:
%
% ┌──────────────────────────────────────────────┐
% │ (0, 0)                                (1, 0) │
```

## Conflicts with other sources

- Serif body font (Libertine) contradicts Alley, Schwabish and metropolis (sans for projection); Butts offers Roboto as the switch.
- Frame titles are phrases ("Components", "Table"), not Alley/Doumont sentence messages; grey title bar is "chrome" Doumont would remove.
- Nine named colours plus two accents exceed Goldsmith-Pinkham's 2–3 and Doumont's one-accent rule, though the demo uses few per slide.
- Section TOC frame on every section is what Goldsmith-Pinkham leaves commented out and Doumont calls a title that adds little.
- Frame numbers in the footline conflict with Doumont (no navigation items unless segregated); Butts' grey monospace number is close to "segregated".
