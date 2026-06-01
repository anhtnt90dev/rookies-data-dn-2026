# KPI Measure Definition Template
**Dashboard 02 — Policy & Payment Operations Analytics**

---

## KPI Measures

| KPI ID | Measure Name | Group | Chart Type | DAX Formula | Business Meaning | Why It Is Important? | Database Source Table | Calculation Column | Filter / Context | Priority | Status | Notes |
|--------|-------------|-------|-----------|-------------|-----------------|---------------------|----------------------|-------------------|-----------------|----------|--------|-------|
| M-01 | Active Policies | KPI Card | KPI Card | `CALCULATE(COUNT(policy_info[policy_id]), policy_info[policy_status] = "ACTIVE")` | Number of policies still active | Portfolio health. Decreasing = churn. Increasing = growth. | policy_info | policy_status | Date filter (policy_end_date >= today) | HIGH | Ready | |
| M-02 | Policies Issued | KPI Card | KPI Card | `COUNT(policy_info[policy_id])` | Number of policies issued in the period | Actual revenue indicator. Requires underwriting and payment. | policy_info | policy_id | Date filter (issued_date) | HIGH | Ready | |
| M-03 | Cancelled Policies | KPI Card | KPI Card | `CALCULATE(COUNT(cancellation[cancellation_id]))` | Number of policies cancelled in the period | Churn indicator. Compare with issued to see net growth. | cancellation | cancellation_id | Date filter (cancellation_date) | HIGH | Ready | |
| M-04 | Payment Success Rate | KPI Card | KPI Card | `DIVIDE(CALCULATE(COUNT(payment[payment_id]), payment[payment_status]="PAID"), COUNT(payment[payment_id]), 0) * 100` | % of payment transactions that are successful | Operational health. Below 90% = investigate immediately. | payment | payment_status | Date filter (payment_date) | HIGH | Ready | Format: 0.00%; Target: 92.35% |
| M-05 | Total Written Premium | KPI Card | KPI Card | `CALCULATE(SUM(policy_info[premium_amount]), policy_info[policy_status] IN {"ACTIVE","EXPIRED"})` | Total insurance premium collected (VND) | Main revenue indicator for the period. | policy_info | premium_amount | Date filter + status filter | HIGH | Ready | Unit: VND billion |
| M-06 | Pending Payments (VND) | KPI Card | KPI Card | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING")` | Total amount of payments waiting to be processed | Cash flow risk. Large number = liquidity risk. | payment | payment_amount, payment_status | Date filter | HIGH | Ready | |
| M-07 | Failed Payments (VND) | KPI Card | KPI Card | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="FAILED")` | Total amount of failed payment transactions | Revenue at risk. Needs follow-up or error investigation. | payment | payment_amount, payment_status | Date filter | HIGH | Ready | |
| M-08 | % Change Active vs Prior Year | KPI Card | KPI Card (delta) | `DIVIDE([Active Policies] - CALCULATE([Active Policies], SAMEPERIODLASTYEAR(policy_info[issued_date])), CALCULATE([Active Policies], SAMEPERIODLASTYEAR(policy_info[issued_date])), 0) * 100` | Year-over-year % change in active policies | Growth trending. Currently +15.4% vs prior year. | policy_info | policy_status, issued_date | Date intelligence | MEDIUM | Pending | Needs Date Table |
| M-09 | Policy Status Distribution | Donut Chart | Donut Chart | `CALCULATE(DIVIDE(COUNT(policy_info[policy_id]), COUNTROWS(policy_info), 0)) * 100` | Distribution of Active / Expired / Cancelled policies | Portfolio health. Active 84.2%, Expired 10.3%, Cancelled 5.5%. | policy_info | policy_status | Date filter | HIGH | Ready | Group by policy_status |
| M-10 | Monthly Collected Amount | Line Chart | Line Chart | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="SUCCESS")` | Total collected payment amount by month (VND) | Revenue growth trend. Jan → May: 28.7bn → 38.7bn. | payment | payment_amount, payment_date | Date hierarchy (month) | HIGH | Ready | Series: Collected / Pending / Failed |
| M-11 | Monthly Pending Amount | Line Chart | Line Chart | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING")` | Total pending payment amount by month | Detect months with unusual pending spikes. | payment | payment_amount, payment_date | Date hierarchy (month) | HIGH | Ready | |
| M-12 | Monthly Failed Amount | Line Chart | Line Chart | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="FAILED")` | Total failed payment amount by month | Operational alert. Spike = system error or fraud. | payment | payment_amount, payment_date | Date hierarchy (month) | HIGH | Ready | |
| M-13 | Monthly Policies Issued | Line Chart | Line Chart | `CALCULATE(COUNT(policy_info[policy_id]))` | Number of policies issued by month | Volume trend. Jan 1.2K → Apr 1.7K → May 1.4K (dip needs analysis). | policy_info | policy_id, issued_date | Date hierarchy (month) | HIGH | Ready | |
| M-14 | Monthly Cancellations | Line Chart | Line Chart | `CALCULATE(COUNT(cancellation[cancellation_id]))` | Number of policies cancelled by month | Churn trend. Jan 186 → May 341. Strong increase needs investigation. | cancellation | cancellation_id, cancellation_date | Date hierarchy (month) | HIGH | Ready | |
| M-15 | Pending Aging: 0–7 Days | Bar Chart | Horizontal Bar | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING", DATEDIFF(payment[payment_date], TODAY(), DAY) <= 7)` | Pending amount within 7 days | Aging bucket. Recent = easier to recover. | payment | payment_amount, payment_date, payment_status | DATEDIFF from payment_date | HIGH | Pending | Needs calculated column: aging_bucket |
| M-16 | Pending Aging: 8–30 Days | Bar Chart | Horizontal Bar | `-- Calculated column: aging_days = DATEDIFF(payment[payment_date], TODAY(), DAY)`<br>`CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING", payment[aging_days] >= 8, payment[aging_days] <= 30)` | Pending amount between 8 and 30 days | Medium-term bucket. Needs proactive follow-up. | payment | payment_amount, payment_date | Aging calculated column | HIGH | Pending | |
| M-17 | Pending Aging: 31–60 Days | Bar Chart | Horizontal Bar | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING", payment[aging_days] >= 31, payment[aging_days] <= 60)` | Pending amount between 31 and 60 days | Risk of becoming bad debt. | payment | payment_amount, payment_date | Aging calculated column | HIGH | Pending | |
| M-18 | Pending Aging: 61–90 Days | Bar Chart | Horizontal Bar | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING", payment[aging_days] >= 61, payment[aging_days] <= 90)` | Pending amount between 61 and 90 days | High risk bucket. | payment | payment_amount, payment_date | Aging calculated column | MEDIUM | Pending | |
| M-19 | Pending Aging: Over 90 Days | Bar Chart | Horizontal Bar | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="PENDING", payment[aging_days] > 90)` | Pending amount over 90 days | Likely uncollectable. Needs write-off decision. | payment | payment_amount, payment_date | Aging calculated column | MEDIUM | Pending | |
| M-20 | Payment Success Rate by Provider | Bar Chart | Horizontal Bar | `CALCULATE([Payment Success Rate])` | Payment success rate for each insurance provider | Find provider problems. Bao Viet 95.41% vs Liberty 89.14%. | payment + policy_info | payment_status, provider_code | Group by provider_code | HIGH | Ready | Join through policy_info |
| M-21 | Failure Amount by Reason | Bar Chart | Horizontal Bar | `CALCULATE(SUM(payment[payment_amount]), payment[payment_status]="FAILED")` | Failed payment amount broken down by reason (VND) | Insufficient Funds 2.35bn, Card Declined 1.48bn, Bank Error 1.22bn. | payment | payment_amount, payment_status | | HIGH | TBD | |
| M-22 | Average Payment Time (Days) | Gauge | Gauge | `AVERAGEX(FILTER(payment, payment[payment_status]="SUCCESS"), DATEDIFF(RELATED(policy_info[issued_date]), payment[payment_date], DAY))` | Average days from policy issuance to payment | 8.6 days vs target ≤10. Operational efficiency KPI. | payment + policy_info | payment_date, issued_date | SUCCESS payments only | HIGH | Pending | Target: ≤10 days |
| M-23 | Policy Processing SLA % | Gauge | Gauge | `DIVIDE(CALCULATE(COUNT(policy_info[policy_id]), DATEDIFF(policy_info[issued_date], TODAY(), DAY) <= 10), COUNT(policy_info[policy_id]), 0) * 100` | % of policies processed within SLA (≤10 days) | 94.2% vs target ≥90%. Operations SLA compliance. | policy_info | issued_date | Date filter + SLA threshold | HIGH | Pending | Target: ≥90% |
| M-24 | Recent Payment Details | Table | Detail Table | `TOPN(5, payment, payment[payment_date], DESC)` | The 5 most recent payment transactions | Operational awareness. Agents can look up transactions quickly. | payment + policy_info + customers | payment_id, payment_date, policy_number, full_name, payment_amount, payment_status | TOPN 5 by date | HIGH | Ready | Join customers through policy_info |

---

## Legend & User Guide

### Group (Column C)

| Value | Description |
|-------|-------------|
| KPI Card | 7 KPI cards displayed at the top of the dashboard |
| Funnel | Measures for funnel chart (4 funnel stages) |
| Trend | Measures over time (monthly / quarterly) |
| Provider / Agent / Region / Package | Breakdown by dimension |
| Time Intel | Comparison with prior period (YoY, PY, Rolling) |
| Suggested | Proposed additions — need Business sign-off |

### Priority (Column J)

| Value | Description |
|-------|-------------|
| HIGH | Must have on Dashboard v1 |
| MEDIUM | Should have, can be used in drill-through |
| LOW | Nice-to-have |

### Status (Column K)

| Value | Description |
|-------|-------------|
| Ready | DAX formula has been validated |
| Draft | Needs further review with Business Owner |
| Suggested | Needs Business sign-off before building |
