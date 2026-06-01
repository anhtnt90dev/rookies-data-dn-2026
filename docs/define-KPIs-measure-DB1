# KPI Measure Definition Template
**Dashboard 01 — Quotation Conversion & Sales Analytics | Insurance Analytics Platform | v1.0**

---

## Overview

| KPI ID | Measure Name | Group | Chart Type | DAX Formula | Business Meaning | Why It Is Important? | Database Source Table | Calculation Column | Filter / Context | Priority | Status | Notes |
|--------|-------------|-------|-----------|-------------|-----------------|---------------------|----------------------|-------------------|-----------------|----------|--------|-------|
| M-01 | Total Quotations | KPI Card | KPI Card | `COUNT(quotation[quotation_id])` | Total number of quotations created during the period | This is the baseline of the entire funnel. Every conversion rate uses this number as the denominator. | quotation | quotation_id | Date filter | HIGH | Ready | |
| M-02 | Accepted Quotations | KPI Card | KPI Card | `CALCULATE([Total Quotations], quotation[quotation_status] IN {"ACCEPTED","CONVERTED"})` | Number of quotations accepted by customers | Level 2 of the funnel. The distance between M02 and M-03 = NTU gap (money left on the table). | quotation | quotation_status | Date filter | HIGH | Ready | Combine ACCEPTED + CONVERTED |
| M-03 | Policies Issued | KPI Card | KPI Card | `COUNT(policy_info[policy_id])` | Number of insurance policies successfully issued | Actual revenue. Different from accepted revenue because it goes through underwriting and payment. | policy_info | policy_id | Date filter | HIGH | Ready | |
| M-04 | Conversion Rate | KPI Card | KPI Card | `DIVIDE([Policies Issued], [Total Quotations], 0) * 100` | Percentage of quotations converted into policies | Most important KPI. 1,000 BG → 297 policy = 29.75%. Benchmark for the entire team. | quotation + policy_info | quotation_id (join key) | Date filter | HIGH | Ready | Format: 0.00% |
| M-05 | Total Premium (VND) | KPI Card | KPI Card | `SUM(policy_info[premium_amount])` | Total insurance premium generated during the period | Main revenue indicator. Combined with M-03 to calculate average deal size. | policy_info | premium_amount | Date + status filter | HIGH | Ready | Unit: VND |
| M-06 | Avg Premium (VND) | KPI Card | KPI Card | `DIVIDE([Total Premium], [Policies Issued], 0)` | Average premium amount per policy | Measure deal quality. Increasing average premium = successful upsell or mix shift. | policy_info | premium_amount, policy_id | Date filter | HIGH | Ready | |
| M-07 | Active Customers | KPI Card | KPI Card | `CALCULATE(DISTINCTCOUNT(policy_info[customer_id]), policy_info[policy_status] = "ACTIVE")` | Number of customers with active policies | Customer base size. Foundation for campaign renewal and cross-selling. | policy_info + customers | customer_id, policy_status | policy_status=ACTIVE | HIGH | Ready | |
| M-08 | Acceptance Rate | Funnel | Funnel | `DIVIDE([Accepted Quotations], [Total Quotations], 0) * 100` | Percentage of quotations accepted | Measure quote quality: Is the pricing correct? Is the agent's explanation good? | quotation | quotation_status | Date filter | HIGH | Ready | Format: 0.00% |
| M-08b | Policies Issued Rate | Funnel | Funnel | `DIVIDE([Policies Issued], [Total Quotations], 0) * 100` | Percentage of accepted quotations converted to issued policies | Conversion rate across the entire funnel — the most important number in the funnel | quotation + policy_info | policy_id, quotation_id | Date filter | HIGH | Ready | 1→3: 24,752 → 7,257 (29.75%) |
| M-08c | Policies In Force | Funnel | Funnel | `CALCULATE(COUNT(policy_info[policy_id]), policy_info[policy_status] = "ACTIVE")` | Number of active policies currently in force | Drop-off between Issued and In Force = contracts issued but not yet active (waiting for payment, pending) | policy_info | policy_id, policy_status | policy_status = ACTIVE | HIGH | Ready | Level 3→4: 7,257 → 6,891 (27.83%) |
| M-08d | In Force Rate | Funnel | Funnel | `DIVIDE([Policies In Force], [Total Quotations], 0) * 100` | Percentage of active policies compared to issued policies | Drop-off between Issued and In Force = contracts issued but not yet active (pending payment) | quotation + policy_info | policy_status | Date filter | HIGH | Ready | Level 1→4: 27.83% — compared to 29.75% issued |
| M-08e | Funnel Drop-off: Issued → In Force | Funnel | Funnel | `[Policies Issued] - [Policies In Force]` | Number of policies dropped between issued and active status | 7,257 - 6,891 = 366 contracts are "stuck" — need payment follow-up | policy_info | policy_id, policy_status | issued vs active | MEDIUM | Ready | 366 Contracts = Uncollected Fees |
| M-24 | NTU Gap (VND) | Funnel | Funnel | `SUMX(FILTER(Fact_Quotation, Fact_Quotation[quotation_status] IN {"ACCEPTED","CONVERTED"}), Fact_Quotation[premium_amount]) - [Total Premium]` | Premium gap between NTU target and actual issued premium | "Money sitting on the table." Large NTU Gap = problems with underwriting/payment or customers are waiting. | Fact_Quotation + Fact_Policy | premium_amount | Date filter | HIGH | New | NTU = Not Taken Up. Alert if gap > threshold |
| M-10 | Quotation Count by Month | Trend | Line | `CALCULATE([Total Quotations], DATESMTD(DIM_DATE[date]))` | Monthly quotation volume trend | Distinguish volume effect vs. rate effect: volume increases while rate drops = lead quality issues | quotation + DIM_DATE | quotation_date | Month context | HIGH | Ready | |
| M-11 | Policies Issued by Month | Trend | Line | `CALCULATE([Policies Issued], DATESMTD(DIM_DATE[date]))` | Monthly issued policy trend | Parallel comparison with M-10. March Issued increased sharply (2.3K) while May dropped (2.1K) despite increased volume → conversion is deteriorating | policy_info | issued_date | Month context | HIGH | Ready | Uses the same axis as M-10 |
| M-12 | Conversion Rate by Month | Trend | Line | `CALCULATE([Conversion Rate], DATESMTD(DIM_DATE[date]))` | Monthly conversion rate trend | Detecting seasonality and anomalies before end-of-month meeting. | quotation + policy_info + DIM_DATE | quotation_date | Month context | HIGH | Ready | Requires DIM_DATE |
| M-25 | Premium by Month | Trend | Line | `CALCULATE([Total Premium], DATESMTD(Dim_Date[date]))` | Monthly premium revenue trend | Revenue trend. If monthly volume increases but premium drops = deal size is shrinking (downgrade package). | Fact_Policy + Dim_Date | premium_amount, issued_date | Month context | HIGH | New | Uses the same axis as M-10, M-11 |
| M-26 | Rejected + Expired Rate by Month | Trend | Line | `DIVIDE(CALCULATE([Total Quotations], quotation[quotation_status] IN {"REJECTED","EXPIRED"}), [Total Quotations], 0) * 100` | Monthly rejected and expired quotation ratio | Early warning: If the lost rate increases while volume increases, lead quality is deteriorating. | Fact_Quotation + Dim_Date | quotation_status, quotation_date | Month context | MEDIUM | New | Complement of Conversion Rate trend |
| M-16 | Quotations PY | Time Intel | | `CALCULATE([Total Quotations], SAMEPERIODLASTYEAR(DIM_DATE[date]))` | Quotation volume in previous year | Baseline for calculating year-on-year growth. | quotation + DIM_DATE | quotation_date | SAMEPERIODLASTYEAR | HIGH | Ready | |
| M-17 | YoY Quotation Growth % | Time Intel | | `DIVIDE([Total Quotations] - [Quotations PY], [Quotations PY], 0) * 100` | Year-over-year quotation growth percentage | Measure growth health. +18.6% = team is expanding well. | quotation + DIM_DATE | | | HIGH | Ready | Format: +/-0.0% |
| M-18 | Conversion Rate PY | Time Intel | | `CALCULATE([Conversion Rate], SAMEPERIODLASTYEAR(DIM_DATE[date]))` | Previous year conversion rate | Detect when the rate is falling despite increasing volume — a dangerous signal. | quotation + policy_info + DIM_DATE | | | HIGH | Ready | |
| M-19 | YoY Premium Growth % | Time Intel | | `DIVIDE([Total Premium] - [Premium PY], [Premium PY], 0) * 100` | Year-over-year premium growth percentage | Separate volume growth vs. pricing/mix growth. If premium growth > volume growth, average deal size is increasing. | policy_info + DIM_DATE | | | HIGH | Ready | |
| M-31 | Premium PY | Time Intel | | `CALCULATE([Total Premium], SAMEPERIODLASTYEAR(Dim_Date[date]))` | Previous year premium value | Baseline for calculating M-19 YoY Premium Growth. | Fact_Policy + Dim_Date | premium_amount | SAMEPERIODLASTYEAR | HIGH | New | Supporting measure for M-19 |
| M-32 | MoM Conversion Rate Change | Time Intel | | `VAR curr = [Conversion Rate] VAR prev = CALCULATE([Conversion Rate], DATEADD(Dim_Date[date], -1, MONTH)) RETURN IF(NOT ISBLANK(prev), curr - prev, BLANK())` | Month-over-month conversion rate difference | Quickly detect deterioration: -2 points in one month = investigate immediately. | Fact_Quotation + Fact_Policy + Dim_Date | quotation_date | Month context | MEDIUM | New | Display delta badge on KPI card |

---

## Nav 1 — Provider & Product

| KPI ID | Measure Name | Group | Chart Type | DAX Formula | Business Meaning | Why It Is Important? | Database Source Table | Calculation Column | Filter / Context | Priority | Status | Notes |
|--------|-------------|-------|-----------|-------------|-----------------|---------------------|----------------------|-------------------|-----------------|----------|--------|-------|
| M-12 | Conv Rate by Provider | Provider | Bar | `CALCULATE([Conversion Rate], ALLEXCEPT(insurance_providers, insurance_providers[provider_code]))` | Conversion rate by insurance provider | BV = 35.6% vs Liberty = 24.3% → Is the agent pushing BV because it's easy to sell, or is Liberty less competitive? | quotation + policy_info + providers | provider_code | Provider context | HIGH | Ready | |
| M-27 | Avg Premium by Provider | Provider | Bar | `CALCULATE([Avg Premium], ALLEXCEPT(Dim_Provider, Dim_Provider[provider_code]))` | Average premium by provider | Liberty's average is higher than BV → Liberty is targeting the premium segment. Insights for product positioning and upsell strategy. | Fact_Policy + Dim_Provider | premium_amount, provider_code | Provider context | HIGH | New | Combine with M-12 Conversion Rate by Provider |
| M-28 | Volume Share by Provider | Provider | Donut | `DIVIDE(CALCULATE([Total Quotations], ALLEXCEPT(Dim_Provider, Dim_Provider[provider_code])), CALCULATE([Total Quotations], ALL(Dim_Provider))) * 100` | Quotation share by provider | Measure dependence on a single provider. If BV > 40% = concentration risk. | Fact_Quotation + Dim_Provider | provider_code | Provider context | MEDIUM | New | Alert if single provider > 40% volume |
| M-21 | Rejected Quotations | KPI Card | KPI Card | `CALCULATE([Total Quotations], quotation[quotation_status] = "REJECTED")` | Number of quotations rejected by customers | Measure level of inappropriate pricing/coverage. High % = review product package or train agents. | Fact_Quotation | quotation_status | Date filter | HIGH | New | Compare with EXPIRED to differentiate proactive rejection vs. expiration |
| M-22 | Expired Quotations | KPI Card | KPI Card | `CALCULATE([Total Quotations], quotation[quotation_status] = "EXPIRED")` | Number of quotations expired without action | Leads abandoned — customers don't reject but don't buy. Agents need to follow up before expiry date. | Fact_Quotation | quotation_status | Date filter | HIGH | New | Combine with S-01 Quotation Age for follow-up alerts |
| M-23 | Quote-to-Accept Rate | KPI Card | KPI Card | `DIVIDE([Accepted Quotations], [Total Quotations], 0) * 100` | Percentage of accepted quotations from total quotations | Separate from Conversion Rate to measure quote quality vs. closing quality. Low rate = pricing/product fit issues. | Fact_Quotation | quotation_status | Date filter | HIGH | New | Different from M-04: M-04 measures the entire funnel, M-23 only measures steps 1→2 |
| M-15 | Quotations by Package | Package | Bar | `CALCULATE([Total Quotations], ALLEXCEPT(quotation, quotation[package_code]))` | Distribution of quotations by insurance package | BASIC = 41.2% of the funnel → insight into customer segmentation. Basis for upsell targeting. | quotation | package_code | Package context | MEDIUM | Ready | |
| N-05 | Detail Records Page | Navigation | Page | `-- Page: Detail Records -- Recent quotation table` | Detailed quotation records page | Allows the operations team to research and follow up each specific quotation. Drill-through from every visual. | Fact_Quotation + All Dims | All keys | Date, Provider, Agent, Status, Package | MEDIUM | Ready | TOPN 100 recent records; export to Excel |

---

## Nav 2 — Agent Performance

| KPI ID | Measure Name | Group | Chart Type | DAX Formula | Business Meaning | Why It Is Important? | Database Source Table | Calculation Column | Filter / Context | Priority | Status | Notes |
|--------|-------------|-------|-----------|-------------|-----------------|---------------------|----------------------|-------------------|-----------------|----------|--------|-------|
| M-13 | Policies Issued by Agent | Agent | Bar | `CALCULATE([Policies Issued], ALLEXCEPT(agents, agents[agent_id]))` | Number of policies issued by each agent | Ranking agents. Combine with region to avoid miscomparative comparisons. | policy_info + agents | agent_id | Agent context | HIGH | Ready | TOPN(5) for visual |
| M-29 | Conv Rate by Agent | Agent | Bar | `CALCULATE([Conversion Rate], ALLEXCEPT(Dim_Agent, Dim_Agent[agent_id]))` | Conversion rate by agent | Combine with M-13 (volume) to differentiate: agents with many deals but low rates vs. agents with few deals but high rates. | Fact_Quotation + Fact_Policy + Dim_Agent | agent_id | Agent context | HIGH | New | Use in parallel with M-13 on the same visual |
| M-30 | Avg Premium by Agent | Agent | Bar | `CALCULATE([Avg Premium], ALLEXCEPT(Dim_Agent, Dim_Agent[agent_id]))` | Average premium generated by agent | Agents with high average premium are upselling well or targeting the right segment. Basis for incentive structure. | Fact_Policy + Dim_Agent | premium_amount, agent_id | Agent context | MEDIUM | New | Ranking top 5 agents |
| M-14 | Conv Rate by Region | Region | Maps | `CALCULATE([Conversion Rate], ALLEXCEPT(agents, agents[region]))` | Conversion rate across regions | Hanoi = 34.2% vs. Can Tho = 28.7% → prioritize resources and coaching in the local area. | quotation + agents | region | Region context | HIGH | Ready | |
| M-16 | Quotations PY | Time Intel | | `CALCULATE([Total Quotations], SAMEPERIODLASTYEAR(DIM_DATE[date]))` | Quotation volume in previous year | Baseline for calculating year-on-year growth. | quotation + DIM_DATE | quotation_date | SAMEPERIODLASTYEAR | HIGH | Ready | |
| M-18 | Conversion Rate PY | Time Intel | | `CALCULATE([Conversion Rate], SAMEPERIODLASTYEAR(DIM_DATE[date]))` | Previous year conversion rate | Detect when rates are falling despite increasing volume — a dangerous signal. | quotation + policy_info + DIM_DATE | | | HIGH | Ready | |
| S-04 | Agent vs Regional Benchmark | Suggested | | `[Agent Conv Rate] - [Regional Avg Conv Rate]` | Agent conversion rate difference vs. regional average | Identify agents needing coaching without unfairly penalizing agents in difficult regions. | quotation + agents | region, agent_id | Agent + Region | HIGH | Suggested | Positive = outperform; Negative = needs review |

---

## Nav 3 — Geography & Ops

| KPI ID | Measure Name | Group | Chart Type | DAX Formula | Business Meaning | Why It Is Important? | Database Source Table | Calculation Column | Filter / Context | Priority | Status | Notes |
|--------|-------------|-------|-----------|-------------|-----------------|---------------------|----------------------|-------------------|-----------------|----------|--------|-------|
| M-14 | Conv Rate by Region | Region | Maps | `CALCULATE([Conversion Rate], ALLEXCEPT(agents, agents[region]))` | Conversion rate across regions | Hanoi = 34.2% vs Can Tho = 28.7% → prioritize resources and coaching at the local level. | quotation + agents | region | Region context | HIGH | Ready | |
| N-05 | Detail Records Page | Navigation | Page | `-- Page: Detail Records -- Recent quotation table` | Page to view detailed recent quotation records | Allow the operations team to track and follow up each quotation. Drill-through supported from all visuals. | Fact_Quotation + All Dims | All keys | Date, Provider, Agent, Status, Package | MEDIUM | Ready | TOPN 100 recent records; export to Excel |
| M-21 | Rejected Quotations | KPI Card | KPI Card | `CALCULATE([Total Quotations], quotation[quotation_status] = "REJECTED")` | Number of quotations rejected by customers | Caused by unsuitable pricing or coverage. High rate = review product packages or provide additional agent training. | Fact_Quotation | quotation_status | Date filter | HIGH | New | Compare with EXPIRED to distinguish active rejection vs. expiration |
| M-22 | Expired Quotations | KPI Card | KPI Card | `CALCULATE([Total Quotations], quotation[quotation_status] = "EXPIRED")` | Number of quotations expired without customer action | Leads are abandoned — customers neither reject nor purchase. Agents should follow up before the expiry date. | Fact_Quotation | quotation_status | Date filter | HIGH | New | Combine with S-01 Quotation Age for follow-up alerts |
| S-01 | Quotation Age (days) | Suggested | | `AVERAGEX(FILTER(quotation, quotation[quotation_status] = "QUOTED"), DATEDIFF(quotation[quotation_date], TODAY(), DAY))` | Average number of days a quotation remains open before closing | Average quotation age > 15 days indicates abandoned leads. Alert agents to follow up before expiration. | quotation | quotation_date, quotation_status | status=QUOTED | MEDIUM | Suggested | Alert threshold: 15 days |
| S-05 | Lost Deal Analysis | Suggested | | `CALCULATE([Total Quotations], quotation[quotation_status] IN {"REJECTED","EXPIRED"})` | Number of lost quotations (rejected or expired) | Root causes may include high pricing, unsuitable coverage, timing issues, or competitors. Each reason requires different actions. | quotation | quotation_status | REJECTED or EXPIRED | HIGH | Suggested | Need to add rejection_reason into the data source |

---

## Legend & User Guide

### Group (Column C)

| Value | Description |
|-------|-------------|
| KPI Card | 7 KPI cards displayed on the same dashboard |
| Funnel | Measures for funnel chart (4 funnel levels) |
| Trend | Trend measures by time (month/quarter) |
| Provider / Agent / Region / Package | Breakdown by dimension |
| Time Intel | Comparison with previous period (YoY, PY, Rolling) |
| Suggested | Additional suggestions — requires Business sign-off |

### Priority (Column J)

| Value | Description |
|-------|-------------|
| HIGH | Required on Dashboard v1 |
| MEDIUM | Recommended, can drill-through |
| LOW | Nice-to-have |

### Status (Column K)

| Value | Description |
|-------|-------------|
| Ready | DAX formula validated |
| Draft | Requires further review with Business Owner |
| Suggested | Requires Business sign-off before build |
