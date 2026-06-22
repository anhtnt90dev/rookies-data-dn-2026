## Row Count
*   **Total Records**: 4,000,010 records (4 million records in total: 1,000,000 per table for Customers, Vehicles, Quotations, and Quotation Items)

![Row Count](insert-1million-record.png)

## Execution Time

~16 minutes 31 seconds

![Execution Time](time.png)

### Medallion Layer Durations:
*   **Bronze (ingestion)**: 5m 28s
*   **Silver (nb_process_transform_silver_layer)**: 2m 19s
*   **Gold (nb_ingestion_gold)**: 6m 16s
*   **Validation (nb_validation_gold)**: 2m 28s

## Inserted Records

![Inserted Records](audit-record-log.png)

## Validation Results

![Validation Results](validation.png)