| strategy | assumption | test | result | verdict |
|---|---|---|---|---|
| S1 | Parallel trends high vs low exposure ZIPs [exit_rate] | Joint Wald test of 2012-2015 coefficients | F=4.71 p=0.001; post 0.0003736 (0.004453); mean|pre|=0.0152; mean|pre|=0.0152; mean|pre|=0.0152; mean|pre|=0.0 | fail |
| S1 | No other TCJA provision correlated with E_z [exit_rate] | Add ZIP mean AGI x year | beta 0.0003736 -> 0.009393 (2414%) | untestable |
| S1 | Parallel trends high vs low exposure ZIPs [sale_rate] | Joint Wald test of 2012-2015 coefficients | F=11.56 p=0.000; post -0.01329 (0.005561); mean|pre|=0.024; mean|pre|=0.024; mean|pre|=0.024; mean|pre|=0.024; | fail |
| S1 | No other TCJA provision correlated with E_z [sale_rate] | Add ZIP mean AGI x year | beta -0.01329 -> 0.01555 (217%) | untestable |
| S1 | Parallel trends high vs low exposure ZIPs [med_log_price] | Joint Wald test of 2012-2015 coefficients | F=4.09 p=0.003; post -0.3118 (0.07174); mean|pre|=0.266; mean|pre|=0.266; mean|pre|=0.266; mean|pre|=0.266; me | fail |
| S1 | No other TCJA provision correlated with E_z [med_log_price] | Add ZIP mean AGI x year | beta -0.3118 -> 0.06241 (120%) | untestable |
| S1 | No spillovers across ZIPs (SUTVA) | none | untestable; movers may relocate to nearby ZIPs | untestable |
| S2 | Parallel trends within county [sale, above16] | Joint Wald test of 2012-2015 coefficients | F=29.36 p=0.000; post 0.001709 (0.001568); mean|pre|=0.0224; mean|pre|=0.0224; mean|pre|=0.0224; mean|pre|=0.0 | fail |
| S2 | Bill does not proxy for wealth [sale, above16] | Add log(value16) x year; % change in beta | beta 0.001709 -> 1.404e-05 (-99%) | fail |
| S2 | Parallel trends within county [exit, above16] | Joint Wald test of 2012-2015 coefficients | F=6.08 p=0.000; post -0.0001227 (0.001346); mean|pre|=0.0069; mean|pre|=0.0069; mean|pre|=0.0069; mean|pre|=0. | fail |
| S2 | Bill does not proxy for wealth [exit, above16] | Add log(value16) x year; % change in beta | beta -0.0001227 -> 4.257e-05 (135%) | fail |
| S2 | Parallel trends within county [exit_prim, above16] | Joint Wald test of 2012-2015 coefficients | F=6.28 p=0.000; post 0.0001606 (0.001989); mean|pre|=0.0088; mean|pre|=0.0088; mean|pre|=0.0088; mean|pre|=0.0 | fail |
| S2 | Bill does not proxy for wealth [exit_prim, above16] | Add log(value16) x year; % change in beta | beta 0.0001606 -> 0.000149 (-7%) | pass |
| S2 | Parallel trends within county [log_price, above16] | Joint Wald test of 2012-2015 coefficients | F=97.79 p=0.000; post 0.1398 (0.01898); mean|pre|=0.292; mean|pre|=0.292; mean|pre|=0.292; mean|pre|=0.292; me | fail |
| S2 | Bill does not proxy for wealth [log_price, above16] | Add log(value16) x year; % change in beta | beta 0.1398 -> 0.1336 (-4%) | pass |
| S2 | Parallel trends within county [refi, above16] | Joint Wald test of 2012-2015 coefficients | F=60.65 p=0.000; post -0.01245 (0.001909); mean|pre|=0.0147; mean|pre|=0.0147; mean|pre|=0.0147; mean|pre|=0.0 | fail |
| S2 | Bill does not proxy for wealth [refi, above16] | Add log(value16) x year; % change in beta | beta -0.01245 -> -0.004765 (62%) | fail |
| S2 | Parallel trends within county [cashout, above16] | Joint Wald test of 2012-2015 coefficients | F=6.07 p=0.000; post -0.004775 (0.001185); mean|pre|=0.00274; mean|pre|=0.00274; mean|pre|=0.00274; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [cashout, above16] | Add log(value16) x year; % change in beta | beta -0.004775 -> -0.002115 (56%) | fail |
| S2 | Parallel trends within county [to_rental, above16] | Joint Wald test of 2012-2015 coefficients | F=1.70 p=0.146; post -0.002019 (0.0009023); mean|pre|=0.00146; mean|pre|=0.00146; mean|pre|=0.00146; mean|pre| | pass |
| S2 | Bill does not proxy for wealth [to_rental, above16] | Add log(value16) x year; % change in beta | beta -0.002019 -> -0.000793 (61%) | fail |
| S2 | Parallel trends within county [appeal_proxy, above16] | Joint Wald test of 2012-2015 coefficients | F=2.48 p=0.042; post 0.002467 (0.0009854); mean|pre|=0.00257; mean|pre|=0.00257; mean|pre|=0.00257; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [appeal_proxy, above16] | Add log(value16) x year; % change in beta | beta 0.002467 -> 0.0009697 (-61%) | fail |
| S2 | Parallel trends within county [sale, lbill16] | Joint Wald test of 2012-2015 coefficients | F=149.58 p=0.000; post 0.0007003 (0.0004206); mean|pre|=0.0101; mean|pre|=0.0101; mean|pre|=0.0101; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [sale, lbill16] | Add log(value16) x year; % change in beta | beta 0.0007003 -> 0.0022 (214%) | fail |
| S2 | Parallel trends within county [exit, lbill16] | Joint Wald test of 2012-2015 coefficients | F=31.16 p=0.000; post -0.001421 (0.0006688); mean|pre|=0.00461; mean|pre|=0.00461; mean|pre|=0.00461; mean|pre | fail |
| S2 | Bill does not proxy for wealth [exit, lbill16] | Add log(value16) x year; % change in beta | beta -0.001421 -> -0.0004424 (69%) | fail |
| S2 | Parallel trends within county [exit_prim, lbill16] | Joint Wald test of 2012-2015 coefficients | F=21.17 p=0.000; post -0.002282 (0.0007828); mean|pre|=0.00448; mean|pre|=0.00448; mean|pre|=0.00448; mean|pre | fail |
| S2 | Bill does not proxy for wealth [exit_prim, lbill16] | Add log(value16) x year; % change in beta | beta -0.002282 -> -0.0003973 (83%) | fail |
| S2 | Parallel trends within county [log_price, lbill16] | Joint Wald test of 2012-2015 coefficients | F=138.13 p=0.000; post 0.05404 (0.008554); mean|pre|=0.2; mean|pre|=0.2; mean|pre|=0.2; mean|pre|=0.2; mean|pr | fail |
| S2 | Bill does not proxy for wealth [log_price, lbill16] | Add log(value16) x year; % change in beta | beta 0.05404 -> 0.04407 (-18%) | pass |
| S2 | Parallel trends within county [refi, lbill16] | Joint Wald test of 2012-2015 coefficients | F=514.23 p=0.000; post -0.008551 (0.0003333); mean|pre|=0.00735; mean|pre|=0.00735; mean|pre|=0.00735; mean|pr | fail |
| S2 | Bill does not proxy for wealth [refi, lbill16] | Add log(value16) x year; % change in beta | beta -0.008551 -> -0.004831 (44%) | fail |
| S2 | Parallel trends within county [cashout, lbill16] | Joint Wald test of 2012-2015 coefficients | F=44.41 p=0.000; post -0.00161 (0.0002031); mean|pre|=0.00084; mean|pre|=0.00084; mean|pre|=0.00084; mean|pre| | fail |
| S2 | Bill does not proxy for wealth [cashout, lbill16] | Add log(value16) x year; % change in beta | beta -0.00161 -> -0.0009856 (39%) | fail |
| S2 | Parallel trends within county [to_rental, lbill16] | Joint Wald test of 2012-2015 coefficients | F=2.13 p=0.074; post -0.003123 (0.0002708); mean|pre|=0.000392; mean|pre|=0.000392; mean|pre|=0.000392; mean|p | pass |
| S2 | Bill does not proxy for wealth [to_rental, lbill16] | Add log(value16) x year; % change in beta | beta -0.003123 -> -0.0008967 (71%) | fail |
| S2 | Parallel trends within county [appeal_proxy, lbill16] | Joint Wald test of 2012-2015 coefficients | F=8.58 p=0.000; post 0.001675 (0.0002334); mean|pre|=0.00134; mean|pre|=0.00134; mean|pre|=0.00134; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [appeal_proxy, lbill16] | Add log(value16) x year; % change in beta | beta 0.001675 -> 0.0008981 (-46%) | fail |
| S3 | Parallel trends given value bin [sale, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=70.15 p=0.000 (df 4); post avg -0.01581 (SE 0.001108); mean|pre|=0.00785; mean|pre|=0.00785; mean|pre|=0.007 | fail |
| S3 | Value bins absorb other TCJA provisions [sale, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.01581 -> -0.01025 (35% change) | fail |
| S3 | Parallel trends given value bin [exit, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=55.48 p=0.000 (df 4); post avg -0.003435 (SE 0.0006675); mean|pre|=0.01; mean|pre|=0.01; mean|pre|=0.01; mea | fail |
| S3 | Value bins absorb other TCJA provisions [exit, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.003435 -> -0.00212 (38% change) | fail |
| S3 | Parallel trends given value bin [exit_prim, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=56.72 p=0.000 (df 4); post avg -0.006749 (SE 0.001032); mean|pre|=0.0105; mean|pre|=0.0105; mean|pre|=0.0105 | fail |
| S3 | Value bins absorb other TCJA provisions [exit_prim, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.006749 -> -0.004883 (28% change) | fail |
| S3 | Parallel trends given value bin [log_price, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=4.77 p=0.001 (df 4); post avg 0.05228 (SE 0.00792); mean|pre|=0.0436; mean|pre|=0.0436; mean|pre|=0.0436; me | fail |
| S3 | Value bins absorb other TCJA provisions [log_price, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.05228 -> 0.04858 (-7% change) | pass |
| S3 | Parallel trends given value bin [refi, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=5.69 p=0.000 (df 4); post avg -0.001638 (SE 0.0005063); mean|pre|=0.00149; mean|pre|=0.00149; mean|pre|=0.00 | fail |
| S3 | Value bins absorb other TCJA provisions [refi, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.001638 -> 0.001381 (184% change) | fail |
| S3 | Parallel trends given value bin [cashout, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=1.32 p=0.258 (df 4); post avg 0.0001552 (SE 0.0003194); mean|pre|=0.000602; mean|pre|=0.000602; mean|pre|=0. | pass |
| S3 | Value bins absorb other TCJA provisions [cashout, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.0001552 -> 0.0007882 (408% change) | fail |
| S3 | Parallel trends given value bin [to_rental, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=6.30 p=0.000 (df 4); post avg -0.008023 (SE 0.0006605); mean|pre|=0.00201; mean|pre|=0.00201; mean|pre|=0.00 | fail |
| S3 | Value bins absorb other TCJA provisions [to_rental, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.008023 -> -0.007919 (1% change) | pass |
| S3 | Parallel trends given value bin [appeal_proxy, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=4.96 p=0.001 (df 4); post avg 0.0004109 (SE 0.0004855); mean|pre|=0.000805; mean|pre|=0.000805; mean|pre|=0. | fail |
| S3 | Value bins absorb other TCJA provisions [appeal_proxy, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.0004109 -> 0.000607 (48% change) | fail |
| S3 | Parallel trends given value bin [sale, above16] | Joint Wald test of 2012-2015 coefficients | F=0.48 p=0.753 (df 4); post avg -0.007607 (SE 0.002569); mean|pre|=0.00354; mean|pre|=0.00354; mean|pre|=0.003 | pass |
| S3 | Value bins absorb other TCJA provisions [sale, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.007607 -> 0.0009728 (113% change) | fail |
| S3 | Parallel trends given value bin [exit, above16] | Joint Wald test of 2012-2015 coefficients | F=1.27 p=0.278 (df 4); post avg -0.002782 (SE 0.002186); mean|pre|=0.00275; mean|pre|=0.00275; mean|pre|=0.002 | pass |
| S3 | Value bins absorb other TCJA provisions [exit, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.002782 -> -0.0003411 (88% change) | fail |
| S3 | Parallel trends given value bin [exit_prim, above16] | Joint Wald test of 2012-2015 coefficients | F=1.02 p=0.397 (df 4); post avg -0.003268 (SE 0.003092); mean|pre|=0.00377; mean|pre|=0.00377; mean|pre|=0.003 | pass |
| S3 | Value bins absorb other TCJA provisions [exit_prim, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.003268 -> -0.0001531 (95% change) | fail |
| S3 | Parallel trends given value bin [log_price, above16] | Joint Wald test of 2012-2015 coefficients | F=20.84 p=0.000 (df 4); post avg 0.09545 (SE 0.01745); mean|pre|=0.11; mean|pre|=0.11; mean|pre|=0.11; mean|pr | fail |
| S3 | Value bins absorb other TCJA provisions [log_price, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.09545 -> 0.08725 (-9% change) | pass |
| S3 | Parallel trends given value bin [refi, above16] | Joint Wald test of 2012-2015 coefficients | F=0.86 p=0.487 (df 4); post avg 0.002298 (SE 0.002871); mean|pre|=0.00332; mean|pre|=0.00332; mean|pre|=0.0033 | pass |
| S3 | Value bins absorb other TCJA provisions [refi, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.002298 -> 0.0007794 (-66% change) | fail |
| S3 | Parallel trends given value bin [cashout, above16] | Joint Wald test of 2012-2015 coefficients | F=0.60 p=0.662 (df 4); post avg 0.0004117 (SE 0.001802); mean|pre|=0.000976; mean|pre|=0.000976; mean|pre|=0.0 | pass |
| S3 | Value bins absorb other TCJA provisions [cashout, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.0004117 -> 0.001448 (252% change) | fail |
| S3 | Parallel trends given value bin [to_rental, above16] | Joint Wald test of 2012-2015 coefficients | F=2.12 p=0.076 (df 4); post avg -0.001484 (SE 0.001226); mean|pre|=0.0014; mean|pre|=0.0014; mean|pre|=0.0014; | pass |
| S3 | Value bins absorb other TCJA provisions [to_rental, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.001484 -> -0.001346 (9% change) | pass |
| S3 | Parallel trends given value bin [appeal_proxy, above16] | Joint Wald test of 2012-2015 coefficients | F=3.12 p=0.014 (df 4); post avg 0.000434 (SE 0.001224); mean|pre|=0.00276; mean|pre|=0.00276; mean|pre|=0.0027 | fail |
| S3 | Value bins absorb other TCJA provisions [appeal_proxy, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.000434 -> 0.0004514 (4% change) | pass |
| S3 | value16 measured well [sale, rate16_pp] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.00772 (0.005447) vs price -0.02788 (0.005148), N=683288 | fail |
| S3 | value16 measured well [exit, rate16_pp] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.01338 (0.003988) vs price 0.01166 (0.003856), N=518328 | pass |
| S3 | value16 measured well [refi, rate16_pp] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.002206 (0.002132) vs price 0.003735 (0.001995), N=683288 | pass |
| S3 | value16 measured well [sale, above16] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 -0.01064 (0.0139) vs price 0.1922 (0.01608), N=683288 | fail |
| S3 | value16 measured well [exit, above16] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 -0.0111 (0.009997) vs price 0.04231 (0.01004), N=518328 | fail |
| S3 | value16 measured well [refi, above16] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.009421 (0.009407) vs price -0.02081 (0.008528), N=683288 | fail |
| S3 | Given value, millage unrelated to post-2018 trends (balance) | 2016 covariates by rate16 tercile within CBSA x value bin (T3-T1, SD units) | year_built=0.10; living_area=0.18; lot_sqft=-0.01; lor16=-0.27; agi_imp=0.14; age16=-0.27; mortint16=0.02 | fail |
| S3 | Local public goods differences do not change after 2018 | none | untestable; noted | untestable |
| S10 | At least one full post-year of outcomes exists | 2025 arm's-length sales vs 2024 in the sample | 2024: 30421, 2025: 30136 | pass |
| S10 | Responses symmetric (or asymmetry is the object) | Compare S10_rate16_pp 2025 coefficient with S3 2018-19 coefficient | see estimates S3_rate vs S10_rate16_pp | untestable |
| S10 | No other 2025 shock correlated with exposure | none | untestable (rates, insurance costs rose with value in 2023-25) | untestable |
| S13 | Post-2018 buyers paid a capitalised price (descriptive; buyers select into bills) | Incumbent vs 2018-19 buyer response 2020-24 | see S13 rows | untestable |
| S3 | Parallel trends, incumbents-only alternative [sale, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=49.98 p=0.000, mean|pre|=0.00251 vs post 0.003805 (SE 0.0004552); mean|pre|=0.00785 | fail |
| S3 | Parallel trends, incumbents-only alternative [exit_prim, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=15.40 p=0.000, mean|pre|=0.00355 vs post -0.003059 (SE 0.001023); mean|pre|=0.0105 | fail |
| S3 | Parallel trends, incumbents-only alternative [refi, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=5.14 p=0.000, mean|pre|=0.000754 vs post -0.002035 (SE 0.0005244); mean|pre|=0.00149 | fail |
| S3 | Parallel trends, incumbents-only alternative [to_rental, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=4.71 p=0.001, mean|pre|=0.00164 vs post -0.006444 (SE 0.0006711); mean|pre|=0.00201 | fail |
| S3 | Parallel trends, incumbents-only alternative [sale, above16] | Joint Wald test of 2012-2015 coefficients | F=5.41 p=0.000, mean|pre|=0.00299 vs post 0.001144 (SE 0.001544); mean|pre|=0.00354 | fail |
| S3 | Parallel trends, incumbents-only alternative [exit_prim, above16] | Joint Wald test of 2012-2015 coefficients | F=0.47 p=0.757, mean|pre|=0.00154 vs post -0.0009932 (SE 0.003112); mean|pre|=0.00377 | pass |
| S3 | Parallel trends, incumbents-only alternative [refi, above16] | Joint Wald test of 2012-2015 coefficients | F=1.58 p=0.176, mean|pre|=0.0044 vs post 0.001859 (SE 0.003046); mean|pre|=0.00332 | pass |
| S3 | Parallel trends, incumbents-only alternative [to_rental, above16] | Joint Wald test of 2012-2015 coefficients | F=0.81 p=0.520, mean|pre|=0.00137 vs post -0.0008701 (SE 0.00121); mean|pre|=0.0014 | pass |
| S4 | Bill gradient would have changed alike for owners and investors [sale, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=39.00 p=0.000; post DDD -0.01947 (0.00203) | fail |
| S4 | Occupancy misclassification unrelated to bill [sale, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.002446 (0.001833) vs base -0.01947 (0.00203) | fail |
| S4 | Bill gradient would have changed alike for owners and investors [log_price, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=0.70 p=0.591; post DDD -0.01398 (0.01976) | pass |
| S4 | Occupancy misclassification unrelated to bill [log_price, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.04509 (0.02572) vs base -0.01398 (0.01976) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [refi, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=5.91 p=0.000; post DDD -0.003091 (0.0009025) | fail |
| S4 | Occupancy misclassification unrelated to bill [refi, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.004925 (0.001241) vs base -0.003091 (0.0009025) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [to_rental, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=2.55 p=0.037; post DDD -0.005535 (0.001364) | fail |
| S4 | Occupancy misclassification unrelated to bill [to_rental, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.0001492 (0.001486) vs base -0.005535 (0.001364) | fail |
| S4 | Bill gradient would have changed alike for owners and investors [sale, above16] | Pre-period DDD event study (2012-2015 joint test) | F=1.32 p=0.261; post DDD -0.006013 (0.009462) | pass |
| S4 | Occupancy misclassification unrelated to bill [sale, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD 0.00426 (0.009751) vs base -0.006013 (0.009462) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [log_price, above16] | Pre-period DDD event study (2012-2015 joint test) | F=2.20 p=0.066; post DDD 0.02823 (0.05777) | pass |
| S4 | Occupancy misclassification unrelated to bill [log_price, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.09511 (0.06558) vs base 0.02823 (0.05777) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [refi, above16] | Pre-period DDD event study (2012-2015 joint test) | F=0.32 p=0.862; post DDD -0.009467 (0.007863) | pass |
| S4 | Occupancy misclassification unrelated to bill [refi, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.01596 (0.009198) vs base -0.009467 (0.007863) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [to_rental, above16] | Pre-period DDD event study (2012-2015 joint test) | F=0.50 p=0.735; post DDD -0.004475 (0.009197) | pass |
| S4 | Occupancy misclassification unrelated to bill [to_rental, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.004802 (0.009568) vs base -0.004475 (0.009197) | pass |
| S4 | Classification does not respond to treatment | Occupancy fixed at 2016; to_rental reported as an outcome | see to_rental rows | pass |
| S5 | Shocks continuous at the border [sale] | Pre-trends (joint test 2012-2015) | F=33.42 p=0.000; post -0.02476 (0.00262) | fail |
| S5 | Shocks continuous at the border [log_price] | Pre-trends (joint test 2012-2015) | F=0.77 p=0.545; post 0.05642 (0.02383) | pass |
| S5 | Shocks continuous at the border [exit_prim] | Pre-trends (joint test 2012-2015) | F=29.36 p=0.000; post -0.01603 (0.003289) | fail |
| S5 | Shocks continuous at the border [refi] | Pre-trends (joint test 2012-2015) | F=3.49 p=0.007; post -0.005279 (0.00167) | fail |
| S5 | Covariate balance across borders | High- vs low-rate side within segment (SD units) | year_built=0.02; living_area=0.05; lot_sqft=-0.00; lor16=-0.20; agi_imp=0.00; age16=-0.21; value16=-0.07 | fail |
| S5 | Who lives on each side does not change because of TCJA | Household size and surname turnover gap across the border, pre vs post | persons gap pre 0.000 post 0.022; turnover gap pre 0.022 post 0.012 | inconclusive |
| S5 | Services across the border do not change after 2018 | none | untestable; school consolidations not screened | untestable |
| S5 | No spillovers across the border | none | untestable; movers may relocate across the line | untestable |
| S6 | TAXSIM simulation reproduces itemisation (validation vs SOI) | ZIP itemisation share correlation 2016/2018, mean error (homeowners vs all filers) | cor16=0.80 cor18=0.80, mean err 0.108/0.043 | pass |
| S6 | Given controls, dprice affects outcomes only through the tax price [sale] | Pre-trends of dprice x year | F=25.68 p=0.000; post -0.04805 (0.004295) | fail |
| S6 | Given controls, dprice affects outcomes only through the tax price [log_price] | Pre-trends of dprice x year | F=3.61 p=0.006; post 0.009655 (0.01993) | fail |
| S6 | Given controls, dprice affects outcomes only through the tax price [exit_prim] | Pre-trends of dprice x year | F=8.38 p=0.000; post -0.02018 (0.005237) | fail |
| S6 | Given controls, dprice affects outcomes only through the tax price [refi] | Pre-trends of dprice x year | F=1.44 p=0.216; post -0.002297 (0.004) | pass |
| S6 | Imputation error unrelated to potential outcomes | none | untestable | untestable |
| S6 | Functional form of controls [sale] | beta under coarse / base / fine controls | S6_base=-0.04805; S6_coarse=-0.06865; S6_fine=-0.03891 | fail |
| S6 | Functional form of controls [log_price] | beta under coarse / base / fine controls | S6_base=0.009655; S6_coarse=0.05179; S6_fine=0.01112 | pass |
| S6 | Functional form of controls [exit_prim] | beta under coarse / base / fine controls | S6_base=-0.02018; S6_coarse=-0.01369; S6_fine=-0.01783 | pass |
| S6 | Functional form of controls [refi] | beta under coarse / base / fine controls | S6_base=-0.002297; S6_coarse=0.004893; S6_fine=0.01129 | fail |
| S7 | No other kink at $10k | Pre-period (2014-16) kink in each outcome, no-income-tax states | sale: 0.00505 (0.0195); log_price: 0.607 (0.281); exit_prim: 0.0208 (0.0218); refi: -0.00422 (0.0147) | fail |
| S7 | Outcomes otherwise smooth in the bill | Covariate kinks at $10k (slope change per $1k, SD units) | year_built=0.047(0.193); living_area=0.007(0.142); value16=-0.029(0.264); agi_imp=0.220(0.291); lor16=0.020(0. | pass |
| S7 | No manipulation of the bill around $10k | Excess mass in [$9k,$10k) 2019 vs 2016 | excess 2016 -0.0312, 2019 -0.0412 | fail |
| S7 | Sales-tax deduction does not blur the threshold | Threshold shifted by IRS optional sales-tax table amount | see S7_notax_salesadj rows | inconclusive |
| S8 | Married and unmarried co-owners parallel [sale, S8_pooled] | Pre-period DDD joint test | F=1.23 p=0.294; post 0.02701 (0.02114) | pass |
| S8 | Married and unmarried co-owners parallel [log_price, S8_pooled] | Pre-period DDD joint test | F=0.49 p=0.741; post 0.2579 (0.1125) | pass |
| S8 | Married and unmarried co-owners parallel [exit_prim, S8_pooled] | Pre-period DDD joint test | F=0.11 p=0.979; post 0.02619 (0.02977) | pass |
| S8 | Married and unmarried co-owners parallel [refi, S8_pooled] | Pre-period DDD joint test | F=0.95 p=0.433; post -0.01126 (0.0215) | pass |
| S8 | Balance married vs unmarried on age, tenure, value [S8_pooled] | Difference in SD units within value bin x bill group | age16=0.50; lor16=0.59; value16=0.00; agi_imp=0.03 | fail |
| S8 | Married and unmarried co-owners parallel [sale, S8_strict] | Pre-period DDD joint test | F=0.75 p=0.556; post 0.02859 (0.02149) | pass |
| S8 | Married and unmarried co-owners parallel [log_price, S8_strict] | Pre-period DDD joint test | F=0.90 p=0.462; post 0.2316 (0.1241) | pass |
| S8 | Married and unmarried co-owners parallel [exit_prim, S8_strict] | Pre-period DDD joint test | F=0.09 p=0.985; post 0.01777 (0.03031) | pass |
| S8 | Married and unmarried co-owners parallel [refi, S8_strict] | Pre-period DDD joint test | F=0.65 p=0.623; post -0.004116 (0.02249) | pass |
| S8 | Balance married vs unmarried on age, tenure, value [S8_strict] | Difference in SD units within value bin x bill group | age16=0.33; lor16=0.48; value16=0.00; agi_imp=0.03 | fail |
| S8 | Marriage/divorce do not respond to the cap | Vesting changes of the same parcel (roll relationship code) reported as an outcome | see table_s8_vesting_change.csv | inconclusive |
| S8 | Misclassification unrelated to the bill | strict vs pooled definition compared | see S8_strict vs S8_pooled rows | inconclusive |
| S8 | Unmarried co-owners actually split the deduction | none | untestable; estimate is a lower bound | untestable |
| S9a | Revaluation timing exogenous to 2018 (schedules set before 2017) | Published NJ lists 2017-22 / CT statutory cycle vs roll-based revaluation years; unscheduled revaluations | CT: 5 matches, 130 unscheduled (roll only), 121 published-only; NJ: 171 matches, 151 unscheduled (roll only),  | inconclusive |
| S9a | Parallel trends reval vs other-time towns [sale] | Pre-trends 2012-2015 | F=1.45 p=0.214 | pass |
| S9a | Parallel trends reval vs other-time towns [log_price] | Pre-trends 2012-2015 | F=3.14 p=0.014 | fail |
| S9a | Parallel trends reval vs other-time towns [exit_prim] | Pre-trends 2012-2015 | F=2.68 p=0.030 | fail |
| S9a | Parallel trends reval vs other-time towns [refi] | Pre-trends 2012-2015 | F=1.83 p=0.119 | pass |
| S9b | Cohort trends same in CA and control states [sale] | Pre-period DDD joint test | F=4.19 p=0.002; post 0.02716 (0.006602) | fail |
| S9b | Cohort trends same in CA and control states [log_price] | Pre-period DDD joint test | not identified on this sample: 5 DDD terms collinear within block group x year (N=12743) | untestable |
| S9b | Cohort trends same in CA and control states [refi] | Pre-period DDD joint test | F=2.39 p=0.049; post -0.02411 (0.008443) | fail |
