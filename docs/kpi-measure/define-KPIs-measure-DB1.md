# KPI Measure Definition 
**Dashboard 01 — Quotation Conversion & Sales Analytics**
Insurance Analytics Platform | v1.0

---

# KPI Measures
| Dashboard 01 — Quotation Conversion & Sales Analytics  |  Insurance Analytics Platform  |  v1.0 |  |  |  |  |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KPI ID | Measure Name | Group | Chart Type | DAX Formula | Business Meaning | Why It Is Important? | Database Source Table | Calculation Column | Status |
| Overview |  |  |  |  |  |  |  |  |  |
| M-01 | Total Quotations | KPI Card | KPI Card | COUNTROWS(fact_quotation) | Total number of quotations created during the period | This is the baseline of the entire funnel. Every conversion rate uses this number as the denominator. | fact_quotation | quotation_key | Draft |
| M-02 | Accepted Quotations | KPI Card | KPI Card | CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] IN {"ACCEPTED", "CONVERTED"}) | Number of quotations accepted by customers | Measures customer acceptance. The gap between M-02 and M-03 helps identify conversion leakage after quotation acceptance. | fact_quotation, dim_quotation_status | quotation_status_key | Draft |
| M-03 | Policies Issued | KPI Card | KPI Card | COUNTROWS(fact_policy) | Number of insurance policies successfully issued | Actual revenue. Different from accepted revenue because it goes through underwriting and payment. | fact_policy | policy_key | Draft |
| M-04 | Conversion Rate | KPI Card | KPI Card | DIVIDE(CALCULATE(COUNTROWS(fact_quotation), fact_quotation[converted_flag] = TRUE), COUNTROWS(fact_quotation), 0) * 100 | Percentage of quotations converted into policies | Most important KPI. 1,000 BG → 297 policy = 29.75%. Benchmark for the entire team. | M-01, M-03 |  | Draft |
| M-05 | Total Written Premium (VND) | KPI Card | KPI Card | SUM(fact_policy[premium_amount]) | Total insurance premium generated during the period | Main revenue indicator. Combined with M-03 to calculate average deal size. | fact_policy | premium_amount | Draft |
| M-06 | Collected Premium Revenue | KPI Card | KPI Card | CALCULATE(SUM(fact_payment[payment_amount]), dim_payment_status[payment_status_code] = "PAID") | Total premium revenue successfully collected from paid policy payments. | Measures actual cash inflow rather than booked premium. Helps monitor collection performance and cash flow health. | fact_payment, dim_payment_status | payment_amount | Draft |
| M-07 | Avg Written Premium per Policy (VND) | KPI Card | KPI Card | DIVIDE([Total Written Premium (VND)], [Policies Issued], 0) | Average premium amount per policy | Measure deal quality. Increasing average premium = successful upsell or mix shift. | M-03, M-05 | premium_amount | Draft |
| M-08 | Active Customers | KPI Card | KPI Card | CALCULATE(DISTINCTCOUNT(fact_policy[customer_key]), dim_policy_status[policy_status_code] = "ACTIVE") | Number of customers with active policies | Customer base size. Foundation for campaign renewal and cross-selling. | fact_policy, dim_policy_status | customer_key | Draft |
| M-09 | Acceptance Rate | Funnel | Funnel | DIVIDE([Accepted Quotations], [Total Quotations], 0) * 100 | Percentage of quotations accepted | Measure quote quality: Is the pricing correct? Is the agent's explanation good? | M-01, M-02 |  | Draft |
| M-10 | Policies Issued Rate | Funnel | Funnel | DIVIDE([Policies Issued], [Total Quotations], 0) * 100 | Percentage of total quotations that resulted in issued policies. | Conversion rate across the entire funnel — the most important number in the funnel | M-01, M-03 |  | Draft |
| M-11 | Policies In Force | Funnel | Funnel | CALCULATE(COUNT(fact_policy[policy_key]), dim_policy_status[policy_status_code] = "ACTIVE") | Number of active policies currently in force | Measures the number of policies generating active insurance coverage. Helps identify activation bottlenecks after policy issuance. | fact_policy, dim_policy_status | policy_key | Draft |
| M-12 | In Force Rate | Funnel | Funnel | DIVIDE([Policies In Force], [Total Quotations], 0) * 100 | Percentage of total quotations that resulted in policies currently in force. | Measures the end-to-end effectiveness of the sales and activation process, showing how many quotations ultimately become active policies. | M-01, M-11 |  | Draft |
| M-13 | Funnel Drop-off: Issued → In Force | Funnel | Funnel | [Policies Issued] - [Policies In Force] | Number of policies dropped between issued and active status | 7,257 - 6,891 = 366 contracts are "stuck" — need payment follow-up | M-03, M-11 |  | Draft |
| M-14 | NTU Gap (VND) | Funnel | Funnel | CALCULATE(SUM(fact_quotation[premium_amount]), dim_quotation_status[quotation_status_code] IN {"ACCEPTED", "CONVERTED"}) - [Total Written Premium (VND)] | Premium gap between NTU target and actual issued premium | This is "money sitting on the table". Large NTU Gap = problems with the underwriting/payment process or customers are waiting | fact_quotation, fact_policy | premium_amount | Draft |
| M-15 | Quotation Count by Month | Trend | Line  | CALCULATE([Total Quotations], DATESMTD(dim_date[full_date])) | Monthly quotation volume trend | Helps monitor quotation demand trends and distinguish between changes in sales volume and conversion performance. | fact_quotation, dim_date | quotation_date_key | Draft |
| M-16 | Policies Issued by Month | Trend | Line  | CALCULATE([Policies Issued], DATESMTD(dim_date[full_date])) | Monthly issued policy trend  | Helps identify trends in policy issuance and evaluate whether growth in quotation volume is translating into issued policies. | fact_policy, dim_date | issued_date_key | Draft |
| M-17 | Conversion Rate by Month | Trend | Line | CALCULATE([Conversion Rate], DATESMTD(dim_date[full_date])) | Monthly conversion rate trend | Detecting seasonality and anomalies before the end-of-month meeting. | M-04, dim_date |  | Draft |
| M-18 | Premium by Month | Trend | Line | CALCULATE([Total Written Premium (VND)], DATESMTD(dim_date[full_date])) | Monthly premium revenue trend | Revenue trend. Differentiation: monthly volume increases but premium drops = deal size is shrinking (downgrade package). | M-05, dim_date |  | Draft |
| M-19 | Rejected + Expired Rate by Month | Trend | Line | DIVIDE(CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] IN {"REJECTED","EXPIRED"}), [Total Quotations], 0) * 100 | Monthly rejected and expired quotation ratio | Early warning: If the lost rate increases while volume increases, the quality of leads is deteriorating and needs investigation. | fact_quotation, dim_date | quotation_status_key | Draft |
| M-20 | Quotations PY | Time Intel |  | CALCULATE([Total Quotations], SAMEPERIODLASTYEAR(dim_date[full_date])) | Quotation volume in previous year | Basline for calculating year-on-year growth. Avoid confusing it with the 365-day period. | fact_quotation, dim_date | quotation_date_key | Draft |
| M-21 | YoY Quotation Growth % | Time Intel |  | DIVIDE([Total Quotations] - [Quotations PY], [Quotations PY], 0) * 100 | Year-over-year quotation growth percentage | Measure growth health. +18.6% as in the mockup means the team is expanding well. | M-01, M-20 |  | Draft |
| M-22 | Conversion Rate PY | Time Intel |  | CALCULATE([Conversion Rate], SAMEPERIODLASTYEAR(dim_date[full_date])) | Previous year conversion rate | Detect when the rate is falling despite increasing volume — this is a dangerous signal. | M-04, dim_date |  | Draft |
| M-23 | YoY Premium Growth % | Time Intel |  | DIVIDE([Total Written Premium (VND)] - [Premium PY], [Premium PY], 0) * 100 | Year-over-year premium growth percentage | Separate: volume growth vs. pricing/mix growth. If premium growth > volume growth, the average deal size is increasing. | M-05, M-24 |  | Draft |
| M-24 | Premium PY | Time Intel |  | CALCULATE([Total Written Premium (VND)], SAMEPERIODLASTYEAR(dim_date[full_date])) | Previous year premium value | Baseline for calculating YoY Premium Growth (M-23) and comparing current premium performance against the previous year. | M-05, dim_date |  | Draft |
| M-25 | MoM Conversion Rate Change | Time Intel |  | VAR curr = [Conversion Rate] VAR prev = CALCULATE([Conversion Rate], DATEADD(dim_date[full_date], -1, MONTH)) RETURN IF(NOT ISBLANK(prev), curr - prev, BLANK()) | Month-over-month conversion rate difference | Quickly detect deterioration: -2 points in one month = a signal to investigate immediately, don't wait until year-on-year. | M-04, dim_date |  | Draft |
| Nav 1 - Provider & Product |  |  |  |  |  |  |  |  |  |
| N1-01 | Conv Rate by Provider | Provider | Bar | CALCULATE([Conversion Rate], ALLEXCEPT(dim_provider, dim_provider[provider_code])) | Conversion rate by insurance provider | BV = 35.6% vs Liberty = 24.3% → Is the agent pushing BV because it's easy to sell, or is Liberty less competitive? | fact_quotation, dim_provider | provider_key | Draft |
| N1-02 | Avg Premium by Provider | Provider | Bar | CALCULATE([Avg Written Premium per Policy (VND)], ALLEXCEPT(dim_provider, dim_provider[provider_code])) | Average premium by provider | Liberty's average is higher than BV → Liberty is targeting the premium segment. Insights for agents regarding product positioning and upsell strategy. | fact_policy, dim_provider | provider_key | Draft |
| N1-03 | Volume Share by Provider | Provider | Donut | DIVIDE(CALCULATE([Total Quotations], ALLEXCEPT(dim_provider, dim_provider[provider_code])), CALCULATE([Total Quotations], ALL(dim_provider)), 0) * 100 | Quotation share by provider | Measure the level of dependence on a single provider. If BV accounts for >40% = concentration risk when BV changes terms. | fact_quotation, dim_provider | provider_key | Draft |
| N1-04 | Rejected Quotations | KPI Card | KPI Card | CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] = "REJECTED") | Number of quotations rejected by customers | Measure the level of inappropriate pricing/coverage. A high percentage = need to review the product package or train agents. | fact_quotation, dim_quotation_status | quotation_status_key | Draft |
| N1-05 | Expired Quotations | KPI Card | KPI Card | CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] = "EXPIRED") | Number of quotations expired without action | Leads abandoned — customers don't reject but don't buy. Agents need to follow up before the expiry date. | fact_quotation, dim_quotation_status | quotation_status_key | Draft |
| N1-06 | Quote-to-Accept Rate | KPI Card | KPI Card | DIVIDE([Accepted Quotations], [Total Quotations], 0) * 100 | Percentage of accepted quotations from total quotations | Separate from Conversion Rate to measure quote quality vs. closing quality. Low rate = pricing/product fit issues. | M-01, M-02 |  | Draft |
| N1-07 | Quotations by Package | Ops | Donut  | CALCULATE([Total Quotations], KEEPFILTERS(dim_package[package_code])) | Distribution of quotations by insurance package | BASIC = 41.2% of the funnel → insight into customer segmentation. Basis for upsell targeting. | fact_quotation, dim_package | package_key | Draft |
| N1-08 | Detail Records Page | Navigation | Page | -- Page: Detail Records -- Recent quotation table | Detailed quotation records page | Allows the operations team to research and follow up on each specific quotation. Drill-through from every visual. | fact_quotation + All dims | All keys | Draft |
| Nav 2 - Agent Performance |  |  |  |  |  |  |  |  |  |
| N2-01 | Policies Issued by Agent | Agent | Bar  | CALCULATE([Policies Issued], ALLEXCEPT(dim_agent, dim_agent[agent_id])) | Number of policies issued by each agent | Ranking agents. Combine with region to avoid miscomparative comparisons (difficult regions naturally have lower rates). | fact_policy, dim_agent | agent_key | Draft |
| N2-02 | Conv Rate by Agent | Agent | Bar  | CALCULATE([Conversion Rate], ALLEXCEPT(dim_agent, dim_agent[agent_id])) | Conversion rate by agent | Combine with policy volume metrics to distinguish agents with high activity but low conversion from agents with lower volume but stronger conversion performance. | fact_quotation, dim_agent | agent_key | Draft |
| N2-03 | Avg Premium by Agent | Agent | Bar  | CALCULATE([Avg Written Premium per Policy (VND)], ALLEXCEPT(dim_agent, dim_agent[agent_id])) | Average premium generated by agent | Agents with a high average premium are doing well upselling or targeting the right segment. Basis for incentive structure. | fact_policy, dim_agent | agent_key | Draft |
| N2-04 | Quotations PY | Time Intel |  | CALCULATE([Total Quotations], SAMEPERIODLASTYEAR(dim_date[full_date])) | Quotation volume in previous year | Basline for calculating year-on-year growth. Avoid confusing it with the 365-day period. | fact_quotation, dim_date | quotation_date_key | Draft |
| N2-05 | Conversion Rate PY | Time Intel |  | CALCULATE([Conversion Rate], SAMEPERIODLASTYEAR(dim_date[full_date])) | Previous year conversion rate | Detect when the rate is falling despite increasing volume — this is a dangerous signal. | M-04, dim_date |  | Draft |
| N2-06 | Agent vs Regional Benchmark | Agent | Bar | [Conv Rate by Agent] - [Conv Rate by Region] | Agent conversion difference compared to the regional average. | Identify agents needing coaching without unfairly penalizing agents in difficult regions. | N2-02,  N3-01 | region, agent_id | Draft |
| Nav 3 - Geography & Ops |  |  |  |  |  |  |  |  |  |
| N3-01 | Conv Rate by Region | Region | Maps | CALCULATE([Conversion Rate], ALLEXCEPT(dim_agent, dim_agent[region])) | Conversion rate across regions | Hanoi = 34.2% vs Can Tho = 28.7% → prioritize focusing resources and coaching at the local level. | fact_quotation, dim_agent | region | Draft |
| N3-02 | Detail Records Page | Navigation | Page | -- Page: Detail Records -- Recent quotation table | Page to view detailed recent quotation records | Allow the operations team to track and follow up each quotation. Drill-through supported from all visuals. | fact_quotation + All dims | All keys | Draft |
| N3-03 | Rejected Quotations (Ops View) | KPI Card | KPI Card | CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] = "REJECTED")  | Number of quotations rejected by customers | Caused by unsuitable pricing or coverage. A high rate indicates the need to review product packages or provide additional agent training. | fact_quotation, dim_quotation_status | quotation_status_key | Draft |
| N3-04 | Expired Quotations | KPI Card | KPI Card | CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] = "EXPIRED") | Number of quotations expired without customer action | Leads are abandoned — customers neither reject nor purchase. Agents should follow up before the expiry date. | fact_quotation, dim_quotation_status | quotation_status_key | Draft |
| N3-05 | Quotation Age (day) | Agent | Column | AVERAGEX(FILTER(fact_quotation, RELATED(dim_quotation_status[quotation_status_code]) = "QUOTED"), DATEDIFF(RELATED(dim_date[full_date]), TODAY(), DAY)) | Average age of open quotations currently in QUOTED status. | Identifies aging quotations that may require follow-up before expiring or being lost. | fact_quotation, dim_date | quotation_date_key | Draft |
| N3-06 | Lost Deal Analysis | Agent | Column | CALCULATE([Total Quotations], dim_quotation_status[quotation_status_code] IN {"REJECTED","EXPIRED"}) | Number of lost quotations (rejected or expired) | Root causes may include high pricing, unsuitable coverage, timing issues, or competitors. Each reason requires different actions. | fact_quotation, dim_quotation_status | quotation_status_key | Draft |

# Changelog
| CHANGELOG — Dashboard_01_KPI_Measures.xlsx |  |  |  |  |  |
| --- | --- | --- | --- | --- | --- |
| Version: v2.0   |   Applied fixes per cleanup review |  |  |  |  |  |
| # | Fix Category | Location (Row / KPI) | Change Description | Before | After |
| 1 | Fix 1 – Gold Status Filter | Row 50 / N3-04 (Expired Quotations) | Updated filter to use Gold semantic model dimension | quotation[quotation_status] = "EXPIRED" | dim_quotation_status[quotation_status_code] = "EXPIRED" |
| 2 | Fix 2 – Consistent Measure Name | Row 11 / M-07 (Avg Written Premium per Policy) | DAX reference updated to match defined measure name | [Total Written Premium] | [Total Written Premium (VND)] |
| 3 | Fix 2 – Consistent Measure Name | Row 32 / N1-02 (Avg Premium by Provider) | DAX reference updated to match defined measure name | [Avg Premium (VND)] | [Avg Written Premium per Policy (VND)] |
| 4 | Fix 2 – Consistent Measure Name | Row 42 / N2-03 (Avg Premium by Agent) | DAX reference updated to match defined measure name | [Avg Premium (VND)] | [Avg Written Premium per Policy (VND)] |
| 5 | Fix 3 – KPI ID Renumbering | All 45 data rows | Replaced plain integer IDs with prefixed unique IDs. Overview → M-01..M-25, Nav1 → N1-01..N1-08, Nav2 → N2-01..N2-06, Nav3 → N3-01..N3-06. Eliminates "b"-suffix pairs. | 1, 2, 3 … 45 (flat integers, "b" suffix pairs) | M-01..M-25, N1-01..N1-08, N2-01..N2-06, N3-01..N3-06 |
| 6 | Fix 4 – Quotations by Package | Row 37 / N1-07 (Quotations by Package) | Database Source Table corrected from dim_quotation_status to dim_package (DAX was already correct) | Database Source Table: fact_quotation, dim_quotation_status | Database Source Table: fact_quotation, dim_package |

# Legend
| LEGEND & USER GUIDE |  |
| --- | --- |
| GROUP (Column C) |  |
| KPI Card | 7 KPI cards displayed on the same dashboard |
| Funnel | Measures for funnel chart (4 funnel levels) |
| Trend | Trend Measures by time (month/quarter) |
| Provider/Agent/Region/Package | Breakdown theo dimension |
| Time Intel | Comparison with previous period (YoY, PY, Rolling) |
| Suggested | Suggested Additional suggestions — requires Business sign-off |
| PRIORITY (Column J) |  |
| HIGH | Required on Dashboard v1 |
| MEDIUM | Recommended, can drill-through |
| LOW | Nice-to-have |
| STATUS (Column K) |  |
| Ready | DAX formula validated |
| Draft | Requires further review with Business Owner |
| Suggested | Requires Business sign-off before build |

