## Row Count
*   **Total Records**: 4,000,000,000 records (4 billion records in total: 1,000,000,000 per table for Customers, Vehicles, Quotations, and Quotation Items)

## Execution Time

~1 hour 50 minutes 29 seconds *(Note: Run failed during Gold Ingestion stage)*

![Execution Time](time.png)

### Medallion Layer Durations:
*   **Bronze (ingestion)**: 46m 56s
*   **Silver (nb_process_transform_silver_layer)**: 28m 52s
*   **Gold (nb_ingestion_gold)**: 34m 11s *(Failed)*
*   **Validation**: N/A *(Aborted due to Gold layer failure)*
