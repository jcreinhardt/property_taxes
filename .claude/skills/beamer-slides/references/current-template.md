# Current house template — `text/presentation.tex` (repo, read 2026-11-04)

- Author: Jakob Reinhardt (Yale). Title: "Did the TCJA lower fiscal capacity of local jurisdictions?" 720 lines,
  32 frames + 1 title, sections: (intro), Institutional Setting, Impact on Migration, Impact on local finances, Open
  Questions, Ideal Research Design & Data, Feasible Research Design I: New Hampshire, Feasible Research Design II:
  Border-RDD, (thank-you), References, appendix frame `Math`.
- Path: /Users/jr2728/PhD/projects/property_taxes/.claude/worktrees/salt-migration-discrepancy-3f765f/text/presentation.tex
  (bibliography `sample.bib`, figures in `text/figures/`).

## What it uses

1. Class `[10pt, aspectratio=169]{beamer}` — 16:9, 10pt (smaller than Goldsmith-Pinkham's and Butts' 11pt).
2. Theme `\usetheme[progressbar=frametitle]{metropolis}` — dark-teal title bar with a thin orange progress bar under
   every frame title; default `numbering=counter`, `sectionpage=progressbar`. Fonts therefore Fira Sans if compiled
   with XeLaTeX/LuaLaTeX, otherwise Computer Modern sans; no `\setsansfont` override in the file.
3. Packages: `appendixnumberbeamer`, `natbib` (`[round]`, `plainnat`), `booktabs`, `ccicons[scale=2]`, `pgfplots` +
   `dateplot`, `xspace`. No `tikz` libraries beyond what metropolis/pgfplots load; no `dcolumn`, `multirow`, `adjustbox`.
4. Colours defined but barely used: `specialgreen #14B03D`, `specialblue #23373b`, `specialorange #eb811b` (these are
   metropolis' own mLightGreen / mDarkTeal / mLightBrown). Emphasis is done with `\alert{}` (orange) and `\textbf{}`,
   often both (`\alert{\textbf{Contribution:}}`).
5. Custom macros: only `\newcommand{\themename}{\textbf{\textsc{metropolis}}\xspace}` (unused leftover from the demo).
   No `transitionframe`, `wideitemize`, colour-text macros, table-highlight markers or `\imageframe`.
6. Appendix link pattern used once: `\hfill{\footnotesize\hyperlink{Math}{\beamergotobutton{Math}}}` on the NH design
   frame → `\begin{frame}[label=Math]{Research Design}` after the references; no `\appendix` call, so the frame counter
   keeps running and no Back button.
7. Figure frames: `\begin{figure}\centering\includegraphics[width=0.65–0.95\linewidth]{figures/*.png}\label{fig:placeholder}\end{figure}`
   (PNG raster figures, stray identical labels); sources given with an unnumbered `\footnotetext` via
   `\renewcommand{\thefootnote}{}` … restore.
8. Model + figure frames: `\metroset{block=fill}` then `\begin{alertblock}{\textbf{Model}:} $$...$$ \end{alertblock}`
   above the figure, repeated on three frames.
9. Two pgfplots `tikzpicture` axes drawn inline (RD illustration, dashed line at $p = 10{,}000$) inside `columns[c]`.
10. Lists: nested `itemize`/`enumerate` two or three levels deep, `\vspace{.25–.5cm}` between top-level items,
    `\item[$\rightarrow$]` for consequences; many frames are full-text (literature frame has ~20 citations).
11. Frame titles mix phrases ("Related literature", "Corelogic Data", "Results", "Dicussion" [sic]) and sentence
    messages ("Exposed taxfilers moved into lower-SALT counties.", "Exposed counties lose revenue").
12. Closing frame `\begin{frame}{\color{specialblue}hi}\centering\Huge{THANK YOU}\end{frame}`; references
    `[allowframebreaks]` with `\bibliography{sample}`.
13. No speaker notes, no `\only`/`\onslide`/`\pause` overlays anywhere, no section roadmap beyond metropolis' section pages.

## Preamble (verbatim)

```latex
\documentclass[10pt, aspectratio=169]{beamer}

\usetheme[progressbar=frametitle]{metropolis}
\usepackage{appendixnumberbeamer}
\usepackage[round]{natbib}
\bibliographystyle{plainnat}

\usepackage{booktabs}
\usepackage[scale=2]{ccicons}

\usepackage{pgfplots}
\usepgfplotslibrary{dateplot}

\usepackage{xspace}
\newcommand{\themename}{\textbf{\textsc{metropolis}}\xspace}
\definecolor{specialgreen}{HTML}{14B03D}
\definecolor{specialblue}{HTML}{23373b}
\definecolor{specialorange}{HTML}{eb811b}




\title{Did the TCJA lower fiscal capacity of local jurisdictions?}
\date{\today}
\date{}
\author{Jakob Reinhardt}
\institute{Yale University}
% \titlegraphic{\hfill\includegraphics[height=1.5cm]{logo.pdf}}

\begin{document}
```

## Representative frame patterns (verbatim)

```latex


\begin{frame}{State and local tax deductions plummet post TCJA}
    \vspace{.25cm}
    \renewcommand{\thefootnote}{}%
    \footnotetext{Source: IRS SOI Historic Table 2, items N1, N04470, N18425 and N18500.}%
    \renewcommand{\thefootnote}{\arabic{footnote}}
    \begin{figure}
        \centering
        \includegraphics[width=.95\linewidth]{figures/fig1.png}
    \end{figure}
\end{frame}

\begin{frame}{Average AGI of movers in exposed states increased}

    \metroset{block=fill}

    \begin{alertblock}{\textbf{Model}:}
        $$AGI_{ijt} = \alpha_{ij} + \delta_t + \alert{\theta_t\Delta SALT_{ijt}} + \epsilon_{ijt}$$
    \end{alertblock}
    \vspace{-.3cm}
    \begin{figure}
        \centering
        \includegraphics[width=0.75\linewidth]{figures/fig4.png}
        \label{fig:placeholder}
    \end{figure}
\end{frame}

    \hfill{\footnotesize\hyperlink{Math}{\beamergotobutton{Math}}}
    
\end{frame}


```

## Gaps relative to the other sources

- No house macros: everything Goldsmith-Pinkham and Butts add (`transitionframe`, `wideitemize`/spacing, colour
  text commands, table cell/row highlighting, `\imageframe`, bottom-corner buttons, `\appendix` with Back links) is absent.
- Many frames exceed Alley's "≤ 2 lines per text block, ≤ 4 items" and Goldsmith-Pinkham's "one point per slide";
  the literature frame alone would fail Alley's 20 words/minute.
- Sentence titles are used on the result frames (good) but not consistently; Doumont's 12-word/2-line cap is met there.
- Figures are PNG with their own fonts/backgrounds; Goldsmith-Pinkham/Schwabish want figure fonts enlarged and
  backgrounds matched; metropolis' `mlineplot`/Tol palette are unused.
- Bold + alert double emphasis and three-level nesting are the kind of noise Doumont and Tufte flag.
- 10pt with metropolis' Fira Light is thin for large rooms (manual suggests Fira Sans Book/SemiBold).
