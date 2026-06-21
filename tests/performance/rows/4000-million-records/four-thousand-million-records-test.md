## Row Count
*   **Total Records**: 4,000,000,000 records (4 billion records in total: 1,000,000,000 per table for Customers, Vehicles, Quotations, and Quotation Items)

## Execution Time

~3 hour 9 minutes 25 seconds *(Note: Run failed during Gold Ingestion stage)*

![Execution Time](time.png)

### Medallion Layer Durations:
*   **Bronze (ingestion)**: 43m 4s
*   **Silver**: 27m 47s
*   **Gold**: 42m 17s
*   **Validation**: 01h 16m 17s
