# Insurance Business Process Understanding

## Quotation · Policy Issuance · Payment · Cancellation

> **Purpose:** Capture the team’s current understanding of the insurance business process from a business perspective, so the first user story can be reviewed by PM/PO/client before the team moves into data mapping, data modeling, pipeline design, and reporting.

---

## 1. Document Scope

This document supports the first business user story:

### In scope

- End-to-end insurance business process overview
- Business process sequence / BPMN diagram
- Quotation lifecycle
- Policy issuance lifecycle
- Payment lifecycle
- Cancellation lifecycle
- Business actors
- Business entities
- Status mapping
- Business rules and assumptions

---

## 2. Big Picture

The insurance process can be understood as a sequence of business events:

```text
Customer requests insurance
        ↓
Agent creates quotation
        ↓
Quotation is reviewed and priced
        ↓
Customer accepts, rejects, or lets quotation expire
        ↓
Accepted quotation is converted into policy
        ↓
Policy is issued
        ↓
Payment is created and processed
        ↓
Policy becomes active if activation rules are met
        ↓
Policy may later expire naturally or be cancelled
        ↓
Cancellation may create refund if applicable
```

### Key distinction

```text
Quotation = offer / proposal
Policy = formal insurance contract
Payment = premium transaction
Cancellation = business event that terminates a policy before natural expiry
```

The user story 1 is achieved when the team can clearly explain the above flow, the actors involved, the entities created, the status transitions, and the uncertain business rules that require PO/client confirmation.

---

## 3. Business Actors

| Actor                  | Business responsibility                                                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Customer**           | Requests insurance, provides personal and vehicle information, accepts/rejects quotation, pays premium, may request cancellation           |
| **Agent**              | Collects customer information, creates quotation, explains quotation, supports policy issuance and cancellation communication              |
| **Insurance Provider** | Provides insurance products, pricing logic, policy issuance, and policy lifecycle control                                                  |
| **Underwriting Team**  | Reviews risk, validates eligibility, approves/rejects quotation or pricing if underwriting is required                                     |
| **Payment Processor**  | Processes payment and refund transactions; returns payment status such as `PENDING`, `PAID`, `FAILED`, `REFUNDED`                          |
| **Operations Team**    | Handles exceptional cases such as failed payment, overdue payment, cancellation validation, refund follow-up, and policy expiry monitoring |

---

## 4. Business Entities

| Entity             | Business meaning                                  | Typical relationship                                   |
| ------------------ | ------------------------------------------------- | ------------------------------------------------------ |
| **Customer**       | Person or organization buying insurance           | One customer can own one or more vehicles and policies |
| **Vehicle**        | Asset being insured                               | Belongs to a customer                                  |
| **Agent**          | Person/channel selling or supporting insurance    | Creates quotation for customer                         |
| **Provider**       | Insurance company/provider                        | Offers insurance product and issues policy             |
| **Quotation**      | Insurance offer given to customer                 | Created before policy                                  |
| **Quotation Item** | Coverage detail inside a quotation                | Belongs to quotation                                   |
| **Policy**         | Formal contract issued from an accepted quotation | Created after quotation is accepted/converted          |
| **Payment**        | Premium transaction for policy                    | Belongs to policy                                      |
| **Cancellation**   | Record of policy cancellation                     | Belongs to policy                                      |

### Entity flow

```text
Customer → Vehicle
Customer + Agent + Provider → Quotation
Quotation → Quotation Item(s)
Accepted / Converted Quotation → Policy
Policy → Payment
Policy → Cancellation
```

---

## 5. Status Mapping

## 5.1 Quotation Status

| Status      | Business meaning                                            | Notes / questions                                                |
| ----------- | ----------------------------------------------------------- | ---------------------------------------------------------------- |
| `QUOTED`    | Quotation has been created and can be presented to customer | Confirm whether underwriting happens before or after this status |
| `ACCEPTED`  | Customer agrees to proceed with the quotation               | Confirm whether policy is created immediately after this status  |
| `REJECTED`  | Customer or provider rejects the quotation                  | Confirm who can reject: customer, underwriting team, or provider |
| `EXPIRED`   | Quotation is no longer valid after expiry date              | Confirm whether expired quotation can be reopened or re-quoted   |
| `CONVERTED` | Quotation has been used to create a policy                  | Confirm exact timing versus policy issuance                      |

### Important distinction

```text
ACCEPTED = customer agrees with the quotation
CONVERTED = quotation has been used to create a policy
```

Do not assume these two statuses mean the same thing.

---

## 5.2 Policy Status

| Status      | Business meaning                                     | Notes / questions                                            |
| ----------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| `ISSUED`    | Policy has been generated by provider/system         | May not be active yet                                        |
| `ACTIVE`    | Policy is currently in force and provides coverage   | Confirm whether this depends on payment, start date, or both |
| `EXPIRED`   | Policy has reached its natural end date              | Different from cancellation                                  |
| `CANCELLED` | Policy was terminated before its natural expiry date | May trigger cancellation record and refund                   |

### Important distinction

```text
ISSUED = policy exists as a formal contract
ACTIVE = policy is currently effective and provides coverage
```

A policy can be issued but not yet active if:

- Start date is in the future
- Payment is still pending
- Payment failed
- Business rule requires manual confirmation

---

## 5.3 Payment Status

| Status     | Business meaning                                   | Notes / questions                                        |
| ---------- | -------------------------------------------------- | -------------------------------------------------------- |
| `PENDING`  | Payment request has been created but not completed | Confirm how long payment can remain pending              |
| `PAID`     | Payment has been successfully completed            | May trigger policy activation                            |
| `FAILED`   | Payment attempt failed                             | May require retry, grace period, or operations follow-up |
| `REFUNDED` | Refund has been processed                          | Usually related to cancellation, but needs confirmation  |

---

## 6. Business Process Sequence

### 6.1 Happy Path

```text
1. Customer requests vehicle insurance.
2. Agent collects customer and vehicle information.
3. Agent creates quotation.
4. Quotation status becomes QUOTED.
5. Agent adds quotation item(s), including coverage, sum insured, and deductible.
6. Insurance provider / underwriting team reviews risk and pricing if required.
7. Quotation is presented to customer.
8. Customer accepts quotation.
9. Quotation status becomes ACCEPTED.
10. Accepted quotation is converted into policy.
11. Quotation status becomes CONVERTED.
12. Provider/system issues policy.
13. Policy status becomes ISSUED.
14. Payment request is created.
15. Payment status becomes PENDING.
16. Customer pays premium.
17. Payment status becomes PAID.
18. System checks policy activation rules.
19. Policy status becomes ACTIVE if activation conditions are met.
```

### 6.2 Rejected Quotation Path

```text
1. Quotation is created.
2. Quotation is presented to customer.
3. Customer or provider rejects quotation.
4. Quotation status becomes REJECTED.
5. No policy is created.
6. No payment is required.
```

### 6.3 Expired Quotation Path

```text
1. Quotation is created with an expiry date.
2. Customer does not accept before expiry date.
3. Quotation status becomes EXPIRED.
4. No policy is created unless re-quote or reopen is allowed.
```

### 6.4 Payment Failed / Overdue Path

```text
1. Policy is issued.
2. Payment request is created.
3. Customer payment fails.
4. Payment status becomes FAILED.
5. Operations Team follows up with customer.
6. Customer may retry payment.
7. If retry succeeds, payment becomes PAID and policy may become ACTIVE.
8. If payment remains unpaid, business rule is required to decide policy outcome.
```

Possible policy outcomes for unpaid cases:

- Remain `ISSUED`
- Become `CANCELLED`
- Become `EXPIRED`

### 6.5 Cancellation Path

```text
1. Customer requests policy cancellation.
2. Agent submits cancellation request.
3. Operations Team validates cancellation eligibility.
4. If approved, cancellation record is created.
5. Policy status becomes CANCELLED.
6. Refund is calculated if applicable.
7. If refund applies, payment/refund status becomes REFUNDED.
8. Customer receives cancellation and refund confirmation.
```

### 6.6 Natural Policy Expiry Path

```text
1. Policy reaches policy_end_date.
2. If no renewal or extension applies, policy status becomes EXPIRED.
3. This is different from CANCELLED because the policy ended naturally.
```

---

## 7. Business Rules and Assumptions

## 7.1 Quotation Rules

- A quotation is created before a policy.
- A quotation belongs to a customer.
- A quotation is usually created by an agent.
- A quotation is linked to an insurance provider.
- A quotation can contain one or more quotation items.
- A quotation has an expiry date.
- A quotation must be accepted before it can be converted into a policy.
- Rejected or expired quotations should not create policy or payment records unless PO/client confirms a special exception.

## 7.2 Policy Rules

- A policy is created from an accepted/converted quotation.
- A policy has start date and end date.
- `ISSUED` does not necessarily mean `ACTIVE`.
- Policy activation may depend on successful payment, policy start date, or both.
- `EXPIRED` means the policy ended naturally.
- `CANCELLED` means the policy was terminated before its natural end date.

## 7.3 Payment Rules

- Payment belongs to a policy.
- Payment starts as `PENDING` when payment request is created.
- Payment becomes `PAID` after successful transaction.
- Payment becomes `FAILED` if the transaction fails.
- Failed payment may require retry, grace period, or operations follow-up.
- Current status mapping does not include overdue or lapsed payment/policy status.

## 7.4 Cancellation Rules

- Cancellation belongs to a policy.
- Cancellation should create a cancellation record with date, reason, and refund amount if applicable.
- Cancellation changes policy status to `CANCELLED` if approved.
- Cancellation may trigger refund.
- Refund may update payment status to `REFUNDED`, but this needs PO/client confirmation.

---

_Document scope: Business process understanding · Not a technical specification · Version 1.0_
