# Paul Goldsmith-Pinkham — "Tips + Tricks with Beamer for Economists" (beamer-tips)

- Author: Paul Goldsmith-Pinkham (then FRBNY; now Yale SOM). Thanks Casey Wichman for the 16:9 setup.
- Title: Tips + Tricks with Beamer for Economists. Year: ~2017–2019 (repo master as fetched 2026-11-04).
- URL used: https://raw.githubusercontent.com/paulgp/beamer-tips/master/slides.tex (857 lines, read in full).
- Repo contents (https://github.com/paulgp/beamer-tips): `slides.tex`, `slides.pdf`, `some_figures.R` (makes the
  example figures), figures `figure1_overlay.pdf`, `figure2_overlay.pdf`, `figure3_overlay.pdf`,
  `figure_badlabels.pdf`, `figure_goodlabels.pdf`, `figure_bestlabels.pdf`, `colorblind.palettes.pdf`,
  `example_note.pdf`, `how-to-draw-an-owl.pdf`, `too-many-words.pdf`, `unreadable-slides.pdf`, plus build
  by-products (`.aux .log .nav .out .snm .toc .vrb .synctex.gz`).
- He points readers to Schwabish, *Better Presentations*, as the design source.

## Rules (concrete, as stated in the deck)

1. Goal is minimalist design, not boring slides; make one point well per slide rather than overwhelm with text.
2. Use `transitionframe` (coloured background, `\Huge` title) to break sections, instead of a cluttered roadmap header.
3. Use `wideitemize` (itemsep +10pt) instead of `itemize` so you cannot fit too much text.
4. Rule of thumb 1: 45–75 characters per line. Rule of thumb 2: one sentence per line.
5. Use 16:9 (`aspectratio=169`); it looks modern and leaves room for an image beside text. 4:3 makes side-by-side hard.
6. If you want a roadmap, generate it with `\AtBeginSection` (code below); it is commented out in the header by default.
7. Colour: traditional red/blue is bad for colour-blind viewers; crib from a colour-blind-safe palette (Okabe–Ito values used:
   blue 0,114,178; red/vermilion 213,94,0; yellow 240,228,66; green 0,158,115).
8. Use two to three colours throughout: blue everywhere (titles, bullets, links), red to contrast/emphasise, green as backup,
   yellow only as transition-slide colour.
9. Background: an off-white (255,253,218) is easier to read; if you change it, give R/Stata figures the same background
   (or transparent). In large auditoriums use black background with white text (contrast is much higher).
10. Never shrink the font to fit text; if you need a busy slide, make it a backup slide and link to it.
11. Font size matters in graphs too: axis labels that look too big on your laptop are right for the audience
    (bad → good → best label example: enlarge tick/axis labels, then label series directly on the plot).
12. Change the font: the deck uses Lato (`\usepackage[default]{lato}`); other sans options at tug.dk FontCatalogue.
13. Fit figures with `\resizebox{0.7\linewidth}{!}{\includegraphics{...}}` and centre with `center`; make the graphic
    central when possible (0.58 linewidth); iterate with `\only<n>` over versions that keep the same axes.
14. Tables: no `table` float; wrap `tabular` in `\makebox[\linewidth][c]{...}` (use `[l]` to left-align);
    `booktabs` rules; `dcolumn` type `d` aligns on the decimal point.
15. Highlight table cells with TikZ marker nodes + `fit`, revealed with `\uncover<2->`.
16. Reveal rows with `\onslide<k->{...}`; reveal columns with `<{\onslide<k->}` column specifiers plus
    `\setbeamercovered{invisible}`; don't overuse, and practise the clicking or it feels artificial.
17. Use TikZ for research-design pictures (DiD timing diagram); colour and `\underbrace` annotate equations; do not put up many equations.
18. Use `\note{}` after a frame for speaker notes; `\setbeameroption{show notes on second screen=right}` + pgfpages + Skim
    (synchronised notes document) to present with notes on the laptop.
19. `\appendix` (with `appendixnumberbeamer`) restarts numbering; label frames `[label=name]` and link with
    `\hyperlink{name}{\beamergotobutton{text}}`; put a Back button on the appendix frame.
20. Footline shows only the frame number; navigation symbols removed; itemize bullets are `-` in blue.

## Preamble (verbatim)

```latex
\documentclass[notes,11pt, aspectratio=169]{beamer}

\usepackage{pgfpages}
% These slides also contain speaker notes. You can print just the slides,
% just the notes, or both, depending on the setting below. Comment out the want
% you want.
\setbeameroption{hide notes} % Only slide
%\setbeameroption{show only notes} % Only notes
%\setbeameroption{show notes on second screen=right} % Both

\usepackage{helvet}
\usepackage[default]{lato}
\usepackage{array}

\usepackage{tikz}
\usepackage{verbatim}
\setbeamertemplate{note page}{\pagecolor{yellow!5}\insertnote}
\usetikzlibrary{positioning}
\usetikzlibrary{snakes}
\usetikzlibrary{calc}
\usetikzlibrary{arrows}
\usetikzlibrary{decorations.markings}
\usetikzlibrary{shapes.misc}
\usetikzlibrary{matrix,shapes,arrows,fit,tikzmark}
\usepackage{amsmath}
\usepackage{mathpazo}
\usepackage{hyperref}
\usepackage{lipsum}
\usepackage{multimedia}
\usepackage{graphicx}
\usepackage{multirow}
\usepackage{graphicx}
\usepackage{dcolumn}
\usepackage{bbm}
\newcolumntype{d}[0]{D{.}{.}{5}}

\usepackage{changepage}
\usepackage{appendixnumberbeamer}
\newcommand{\beginbackup}{
   \newcounter{framenumbervorappendix}
   \setcounter{framenumbervorappendix}{\value{framenumber}}
   \setbeamertemplate{footline}
   {
     \leavevmode%
     \hline
     box{%
       \begin{beamercolorbox}[wd=\paperwidth,ht=2.25ex,dp=1ex,right]{footlinecolor}%
%         \insertframenumber  \hspace*{2ex} 
       \end{beamercolorbox}}%
     \vskip0pt%
   }
 }
\newcommand{\backupend}{
   \addtocounter{framenumbervorappendix}{-\value{framenumber}}
   \addtocounter{framenumber}{\value{framenumbervorappendix}} 
}

\usepackage{graphicx}
\usepackage[space]{grffile}
\usepackage{booktabs}

% These are my colors -- there are many like them, but these ones are mine.
\definecolor{blue}{RGB}{0,114,178}
\definecolor{red}{RGB}{213,94,0}
\definecolor{yellow}{RGB}{240,228,66}
\definecolor{green}{RGB}{0,158,115}

\hypersetup{
  colorlinks=false,
  linkbordercolor = {white},
  linkcolor = {blue}
}

%% I use a beige off white for my background
\definecolor{MyBackground}{RGB}{255,253,218}

%% Uncomment this if you want to change the background color to something else
%\setbeamercolor{background canvas}{bg=MyBackground}

%% Change the bg color to adjust your transition slide background color!
\newenvironment{transitionframe}{
  \setbeamercolor{background canvas}{bg=yellow}
  \begin{frame}}{
    \end{frame}
}

\setbeamercolor{frametitle}{fg=blue}
\setbeamercolor{title}{fg=black}
\setbeamertemplate{footline}[frame number]
\setbeamertemplate{navigation symbols}{} 
\setbeamertemplate{itemize items}{-}
\setbeamercolor{itemize item}{fg=blue}
\setbeamercolor{itemize subitem}{fg=blue}
\setbeamercolor{enumerate item}{fg=blue}
\setbeamercolor{enumerate subitem}{fg=blue}
\setbeamercolor{button}{bg=MyBackground,fg=blue,}

% If you like road maps, rather than having clutter at the top, have a roadmap show up at the end of each section 
% (and after your introduction)
% Uncomment this is if you want the roadmap!
% \AtBeginSection[]
% {
%    \begin{frame}
%        \frametitle{Roadmap of Talk}
%        \tableofcontents[currentsection]
%    \end{frame}
% }
\setbeamercolor{section in toc}{fg=blue}
\setbeamercolor{subsection in toc}{fg=red}
\setbeamersize{text margin left=1em,text margin right=1em} 

\newenvironment{wideitemize}{\itemize\addtolength{\itemsep}{10pt}}{\enditemize}

\usepackage{environ}
\NewEnviron{videoframe}[1]{
  \begin{frame}
    \vspace{-8pt}
    \begin{columns}[onlytextwidth, T] % align columns
      \begin{column}{.58\textwidth}
        \begin{minipage}[t][\textheight][t]
          {\dimexpr\textwidth}
          \vspace{8pt}
          \hspace{4pt} {\Large \sc \textcolor{blue}{#1}}
          \vspace{8pt}
          
          \BODY
        \end{minipage}
      \end{column}%
      \hfill%
      \begin{column}{.42\textwidth}
        \colorbox{green!20}{\begin{minipage}[t][1.2\textheight][t]
            {\dimexpr\textwidth}
            Face goes here
          \end{minipage}}
      \end{column}%
    \end{columns}
  \end{frame}
}

\title[]{\textcolor{blue}{Tips + Tricks with Beamer for Economists}}
\author[PGP]{}
\institute[FRBNY]{\small{\begin{tabular}{c c c}
Author A &&  Paul Goldsmith-Pinkham  \\
Somewhere Fancy && FRBNY \\ \\

Author C && Author D   \\
\multicolumn{3}{c}{Somewhere Fancy} \\
\end{tabular}}}

\date{\today}
```

Notes on the preamble: the `\beginbackup`/`\backupend` macros are a leftover (contain a typo `\hline box{`) and are
never used in the deck; `appendixnumberbeamer` + `\appendix` is what actually handles backup numbering. `helvet` is
loaded then overridden by `lato`; `mathpazo` sets the maths font (Palatino).

## TikZ marker setup placed right after `\begin{document}` (verbatim)

```latex
%%% TIKZ STUFF
\tikzset{   
        every picture/.style={remember picture,baseline},
        every node/.style={anchor=base,align=center,outer sep=1.5pt},
        every path/.style={thick},
        }
\newcommand\marktopleft[1]{%
    \tikz[overlay,remember picture] 
        \node (marker-#1-a) at (-.3em,.3em) {};%
}
\newcommand\markbottomright[2]{%
    \tikz[overlay,remember picture] 
        \node (marker-#1-b) at (0em,0em) {};%
}
\tikzstyle{every picture}+=[remember picture] 
\tikzstyle{mybox} =[draw=black, very thick, rectangle, inner sep=10pt, inner ysep=20pt]
\tikzstyle{fancytitle} =[draw=black,fill=red, text=white]
%%%% END TIKZ STUFF
```

## Title slide and transition frame (verbatim)

```latex
% Title Slide
\begin{frame}
\maketitle
  \centering The views expressed do not necessarily reflect the position of the Federal Reserve Bank of New York or the Federal Reserve System.
\end{frame}

\section{Structure and Spacing}
\begin{transitionframe}
  \begin{center}
    { \Huge \textcolor{black}{Spacing and Words}}
  \end{center}
\end{transitionframe}
```

## Roadmap at every section (verbatim, from the "Roadmap" frame; same as the commented header block)

```latex
 \AtBeginSection[]{%
\begin{frame}%
 \frametitle{Roadmap of Talk}%
 \tableofcontents[currentsection] %
\end{frame}%
}
```

## Black-background auditorium frame (verbatim)

```latex
{  \setbeamercolor{background canvas}{bg=black}
\begin{frame}
  \textcolor{white}{Example for an auditorium -- contrast is much higher}
\end{frame}
}
```

## Dummy frames (verbatim)

### Two-column frame
```latex
\begin{frame}{Two Column Frame}
\begin{columns}[T] % align columns
\begin{column}{.58\textwidth}
\color{red}\rule{\linewidth}{4pt}
Column 1
\end{column}%
\hfill%
\begin{column}{.38\textwidth}
\color{blue}\rule{\linewidth}{4pt}
Column 2
\end{column}%
\end{columns}
\end{frame}
```

### Two-column with face (for recorded/Zoom talks)
```latex
\begin{frame}
  \vspace{-8pt}
\begin{columns}[T] % align columns
\begin{column}{.58\textwidth}
\begin{minipage}[t][\textheight][t]
  {\dimexpr\textwidth}
  \vspace{8pt}
  \hspace{4pt} \textcolor{blue}{Two Column w/ Face}
  \vspace{8pt}
  
Information goes here
    \end{minipage}
\end{column}%
\hfill%
\begin{column}{.42\textwidth}
 \colorbox{green!20}{\begin{minipage}[t][1.2\textheight][t]
      {\dimexpr\textwidth}
      Face goes here
    \end{minipage}}
\end{column}%
\end{columns}
\end{frame}
```

### videoframe (uses the `\NewEnviron` definition in the preamble)
```latex
\begin{videoframe}{This Looks Good}
  Using the Environ package
\end{videoframe}
```

### Table frame
```latex
\begin{frame}{Table Frame}
\setbeamercovered{invisible}
\makebox[\linewidth][c]{
\begin{tabular}{l}
\toprule
Temp Table\\
\bottomrule
\end{tabular}
}
\end{frame}
```

### Figure frame
```latex
\begin{frame}{Figure Frame}
\begin{center}
\resizebox{0.5\textwidth}{!}{
  \includegraphics{how-to-draw-an-owl.pdf}
}
\end{center}
\end{frame}
```

### Figure with commentary
```latex
\begin{frame}{Figure with Commentary Frame}
\begin{columns}[T] % align columns
\begin{column}{.58\textwidth}
\resizebox{\textwidth}{!}{
  \includegraphics{how-to-draw-an-owl.pdf}
}
\end{column}%
\hfill%
\begin{column}{.38\textwidth}
  \begin{wideitemize}
  \item Everyone gives these tips on nice presentations
  \item But maybe we'd put less on the slides
  \item If the audience stopped interrupting
  \end{wideitemize}
\end{column}%
\end{columns}
\end{frame}
```

### Figure iteration with `\only` (verbatim)
```latex
\begin{frame}{But sometimes iteration is better}
\begin{columns}[T]
\begin{column}{.58\textwidth}
    \only<1>{\resizebox{\textwidth}{!}{
      \includegraphics{figure1_overlay.pdf}
    }
  }
    \only<2>{\resizebox{\textwidth}{!}{
      \includegraphics{figure2_overlay.pdf}
    }
  }
    \only<3>{\resizebox{\textwidth}{!}{
      \includegraphics{figure3_overlay.pdf}
    }
  }
\end{column}
\hfill
\begin{column}{.4\textwidth}
  \begin{wideitemize}
  \item<1-> Sometimes you want to talk about one effect
  \item<2-> Then switch to a second effect 
  \item<3-> Use the \texttt{\textbackslash only<slidenum>} command
  \item<3-> For the effect, keep the similar axes
  \end{wideitemize}
  \end{column}
\end{columns}
\end{frame}
```

## Table highlighting with TikZ (verbatim)

```latex
\begin{frame}{Highlight cells using tikz (see source code)}
\makebox[\linewidth][c]{
\begin{tabular}{l cc ddd}
  \toprule
  &  Mean at && \multicolumn{3}{c}{Difference-in-Differences Estimates} \\
  \cmidrule{4-6}
  & $ t=-1 $ &&\multicolumn{1}{c}{1 Year} & \multicolumn{1}{c}{2 Years} & \multicolumn{1}{c}{3 Years} \\
  \cmidrule{2-2} \cmidrule{4-6}
  & \multicolumn{1}{c}{(1)}  &&\multicolumn{1}{c}{(2)} & \multicolumn{1}{c}{(3)} & \multicolumn{1}{c}{(4)} \\
  \cmidrule{2-6}
  Outcome 1 & 2.58 && 0.11 &0.08 &\marktopleft{a1} 0.12\\
  & (2.55) && (0.04) & (0.04) & (0.04)\\ 
  Outcome 2 & 60.90 && -0.73 &-1.13 & -1.58\\
  & (17.02) && (0.10) & (0.11) & (0.12)\markbottomright{a1}{red} \\
  Outcome 3 & 18.98 && 0.77 &1.28 & 1.62\\
  & (6.74) && (0.13) & (0.13) & (0.12)\\ 
  \bottomrule
\end{tabular}
}
\uncover<2->{\tikz[overlay,remember picture,inner sep=1pt]
\node[draw=red,rounded corners,fit=(marker-a1-a.north west) (marker-a1-b.south east)] {};}
\end{frame}
```

## Row reveal with `\onslide` (verbatim)

```latex
\begin{frame}{Reveal rows sequentially using \texttt{\textbackslash onslide} (see source code)}
\makebox[\linewidth][c]{
\begin{tabular}{l cc ddd}
  \toprule
  &  Mean at && \multicolumn{3}{c}{Difference-in-Differences Estimates} \\
  \cmidrule{4-6}
  & $ t=-1 $ &&\multicolumn{1}{c}{1 Year} & \multicolumn{1}{c}{2 Years} & \multicolumn{1}{c}{3 Years} \\
  \cmidrule{2-2} \cmidrule{4-6}
  & \multicolumn{1}{c}{(1)}  &&\multicolumn{1}{c}{(2)} & \multicolumn{1}{c}{(3)} & \multicolumn{1}{c}{(4)} \\
  \cmidrule{2-6}
\onslide<2->{
  Outcome 1 & 2.58 && 0.11 &0.08 & 0.12\\
  & (2.55) && (0.04) & (0.04) & (0.04)
}
\onslide<3->{
\\
  Outcome 2 & 60.90 && -0.73 &-1.13 & -1.58\\
  & (17.02) && (0.10) & (0.11) & (0.12) 
}
\onslide<4>{
\\
  Outcome 3 & 18.98 && 0.77 &1.28 & 1.62\\
  & (6.74) && (0.13) & (0.13) & (0.12)
}
\\\bottomrule
\end{tabular}
}
\begin{itemize}
\item[] There are other ways to do this
\item[] Consider whether it is better to have all the context, or control
\item[] If you haven't practiced, the clicking through slides will be artificial
\end{itemize}

\end{frame}
```

## Column reveal with `\onslide` (verbatim; column-spec trick)

```latex
\begin{frame}{Reveal columns sequentially using \texttt{\textbackslash onslide} (see source code)}
\setbeamercovered{invisible}
\makebox[\linewidth][c]{
\begin{tabular}{l cc <{\onslide<1->}d<{\onslide<2->}d<{\onslide<3->}d<{\onslide}}
  \toprule
  &  Mean at && \multicolumn{3}{c}{Difference-in-Differences Estimates} \\
  \cmidrule{4-6}
  & $ t=-1 $ &&\multicolumn{1}{c}{1 Year} & \multicolumn{1}{c}{2 Years} & \multicolumn{1}{c}{3 Years} \\
  \cmidrule{2-2} \cmidrule{4-6}
  & \multicolumn{1}{c}{(1)}  &&\multicolumn{1}{c}{(2)} & \multicolumn{1}{c}{(3)} & \multicolumn{1}{c}{(4)} \\
  \cmidrule{2-6}
  Outcome 1 & 2.58 && 0.11 &0.08 & 0.12\\
  & (2.55) && (0.04) & (0.04) & (0.04)\\ 
  Outcome 2 & 60.90 && -0.73 &-1.13 & -1.58\\
  & (17.02) && (0.10) & (0.11) & (0.12) \\
  Outcome 3 & 18.98 && 0.77 &1.28 & 1.62\\
  & (6.74) && (0.13) & (0.13) & (0.12)\\
\bottomrule
\end{tabular}
}
\end{frame}
```

## TikZ DiD timing diagram (verbatim)

```latex
\begin{center}
\begin{tikzpicture}[snake=zigzag, line before snake = 5mm, line after snake = 5mm, line width=1mm]
    % draw horizontal line   
    \draw (0,0) -- (1,0);
    \draw[snake] (1,0) -- (3,0);
    \draw (3,0) -- (4,0);
    \draw[blue] (4,0) -- (8,0);
    \draw[->] (8,0) -- (9,0);

    \draw (0,.95) -- (1,.95);
    \draw[snake] (1,.95) -- (3,.95);
    \draw (3,.95) -- (4,.95);
    \draw[red] (4,.95) -- (8,.95);
    \draw[->] (8,.95) -- (9,.95);

    % draw vertical lines
    \foreach \x in {8}
      \draw (\x cm,.205) -- (\x cm,-.205);
    \foreach \x in {4}
      \draw (\x cm,{.95+(.205)}) -- (\x cm,{.95-(.205)});
    \foreach \x in {0}
      \draw (\x cm,{.95+(.205)}) -- (\x cm,{0-(.205)});

    % draw nodes
    \draw (0,0) node[below=.105] {}  node[left=.105] {\textcolor{blue}{Control}};
    \draw (0,0.95) node[below=.105] {} node[above=.105] {} node[left=.105] {\textcolor{red}{Treatment}};
    \draw (4,.95) node[below=.105] {} node[above=.105] {} node[below=.105] {Policy Treatment};
    \draw (8,0) node[below=.105] {} node[above=.105] {} node[below=.105] {End Policy};
  \end{tikzpicture}
\end{center}
```

## Annotated equation (verbatim)

```latex
      \begin{equation*}
        \alpha_{g}^{IV} = \left. \underbrace{Cov(Y, g(\textcolor{blue}{Z}))}_{\text{\textcolor{red}{Reduced Form}}} \middle/ \underbrace{Cov(D, g(\textcolor{blue}{Z}))}_{\text{\textcolor{green}{First Stage}}} \right.
      \end{equation*}
```

## Speaker notes (verbatim; note goes *after* the frame, outside it)

```latex
% From here: https://tex.stackexchange.com/questions/114219/add-notes-to-latex-beamer
% To give a presentation with the Skim reader (http://skim-app.sourceforge.net) on OSX so
% that you see the notes on your laptop and the slides on the projector, do the following:
% 
% 1. Generate just the presentation (hide notes) and save to slides.pdf
% 2. Generate onlt the notes (show only nodes) and save to notes.pdf
% 3. With Skim open both slides.pdf and notes.pdf
% 4. Click on slides.pdf to bring it to front.
% 5. In Skim, under "View -> Presentation Option -> Synhcronized Noted Document"
%    select notes.pdf.
% 6. Now as you move around in slides.pdf the notes.pdf file will follow you.
% 7. Arrange windows so that notes.pdf is in full screen mode on your laptop
%    and slides.pdf is in presentation mode on the projector.
\note[itemize]{
\item Make sure not to forget notes!
\item I should use these more
\item put your text here!
}
```

## Appendix / hyperlink / button pattern (verbatim)

```latex
\appendix

\begin{frame}[label=appendix_start]{Almost done!}
  \begin{wideitemize}
  \item See, now we're in backup slide land
  \item This is made useful by having links throughtout the talk
  \item Here's a button, which is how I make links \hyperlink{appendix_end}{\beamergotobutton{Next slide}}
  \end{wideitemize}
\end{frame}

\begin{frame}[label=appendix_end]{Use it to intimidate audiences!}
  \begin{wideitemize}
    \item[] Now you can make it clear you've done a shitload of work
      \begin{itemize}
      \item[]  without having to show everything! \hyperlink{appendix_start}{\beamergotobutton{Back}}
      \end{itemize}
    \item[] You label a frame with the \texttt{[label=name]} option, and then point a link to it
    \item[] You can make an object a link using the \texttt{\textbackslash hyperlink\{label\}\{object\}} command
  \end{wideitemize}
\end{frame}
```

A frame in the main body is labelled the same way (`\begin{frame}[label=methodology]{...}`) so the appendix can link back.

## Conflicts with other sources

- Keeps the default theme and standard phrase-style frame titles ("Text size", "Graphics"); Alley and Doumont require
  full-sentence message titles. Butts and the metropolis theme also keep phrase titles.
- Uses bullet lists (dash bullets) throughout; Alley bans bulleted lists, Tufte and Doumont discourage them.
- 11pt body is small relative to Alley's 18–24pt body / 28pt headline (Beamer 11pt ≈ 20–24pt on a 16:9 PowerPoint
  canvas after scaling, so the gap is smaller than it looks).
- Sequential reveals (`\only`, `\onslide`) are encouraged with caveats; Doumont allows builds only to "state verbally,
  then develop visually"; Tufte dislikes fragmenting evidence across slides.
- Off-white background is optional here; Schwabish suggests light grey rather than white; Alley only says avoid hot colours.
