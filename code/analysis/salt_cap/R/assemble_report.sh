#!/bin/bash
# Assembles output/analysis/salt_cap/report.md from the hand-written pieces and the machine-generated tables.
O=/Users/jr2728/PhD/projects/property_taxes/output/analysis/salt_cap
{
  cat $O/report_01_summary.md; echo
  cat $O/report_data_section.md; echo
  echo "## 3. Results by strategy"; echo
  cat $O/report_results_draft.md; echo; cat $O/report_03b_results_rest.md; echo
  echo "## 4. Comparison table"; echo
  echo "Headline specification per strategy (1% sample). Cells: post-period coefficient (SE); for Strategies 1–3 and 10 the coefficient is the 2018–19 (2025) average effect per unit of treatment; S4/S8/S9b are triple-difference coefficients; S7 is the change in slope per \$1,000 of bill. Verdict counts summarise the ledger rows of each strategy."; echo
  cat $O/table_comparison.md; echo
  cat $O/report_04_comment.md 2>/dev/null; echo
  echo "## 5. Assumption ledger"; echo; cat $O/table_ledger.md; echo
  cat $O/report_06_capitalization.md; echo
  cat $O/report_07_limitations.md; echo
  cat $O/report_08_tenpct.md 2>/dev/null; echo
  cat $O/report_08_next.md; echo
  echo "## Appendix A. All estimates (\`estimates.csv\`)"; echo; cat $O/table_all_estimates.md; echo
  echo "## Appendix B. Figures"; echo; echo "Event-study figures (PNG + CSV of coefficients) are in \`figures/\`, named \`es_<strategy>_<outcome>_<spec_id>\`; descriptive figures \`desc_bill16_hist.png\`, \`desc_rate16_hist.png\`, \`s7_density_10k.png\`."
} > $O/report.md
wc -w $O/report.md
