# Bug 137 CRM Connection Cleanup

## Root cause

The dev Fabric workspace Git branch contained a CRM Fabric SQL Database item and a Landing ingestion pipeline that were created with a personal Microsoft Fabric SQL connection. When other team members synced the branch or ran the pipeline, Fabric could still resolve the old user-specific connection metadata.

## Artifacts removed

- `fabric/Source/insurance_crm_db.SQLDatabase`
  - Fabric item type: `SQLDatabase`
  - Display name: `insurance_crm_db`
  - Removed because it represented the temporary CRM source database artifact in Git.
- `fabric/Pipelines/pl_landing_fabric_sql_db_ingest_dev.DataPipeline`
  - Fabric item type: `DataPipeline`
  - Display name: `pl_landing_fabric_sql_db_ingest_dev`
  - Removed because its source connection settings referenced the CRM SQL Database item and an external Fabric connection ID.

## Artifacts intentionally kept

- `fabric/Lakehouse/lh_insurance_dev.Lakehouse`
- Bronze, Silver, and Gold table creation notebooks
- Config and pipeline-control notebooks
- Monitoring and audit notebooks
- Source SQL scripts under `sql/source`
- Architecture, standards, and data-modeling documentation

These items use CRM table names, landing paths, or medallion model metadata, but they do not contain the old Fabric personal connection reference.

## Safe recreation guidance

After this cleanup is merged and synced to Fabric, recreate the CRM source ingestion in Fabric using a team-owned or environment-specific connection:

1. Create or provision the CRM source database outside Git with the approved team/shared ownership model.
2. Create a new Fabric connection using a team service account, workspace-managed identity, or another approved shared credential.
3. Recreate the Landing ingestion pipeline in Fabric and bind it to the shared connection.
4. Confirm the pipeline writes to `Files/landing/crm/...` or the agreed landing path for CRM source tables.
5. Commit the recreated Fabric item only after confirming it does not serialize personal `connection`, `connectionId`, `gatewayId`, or credential metadata.

## Validation after PR merge

1. Sync the Fabric workspace from the updated branch.
2. Confirm `insurance_crm_db.SQLDatabase` and `pl_landing_fabric_sql_db_ingest_dev.DataPipeline` are no longer restored from Git.
3. Recreate the CRM source connection and Landing pipeline with shared credentials.
4. Run the Landing ingestion for `customers`, `agents`, `insurance_providers`, `vehicle`, `quotation`, and `quotation_item`.
5. Verify files are written to the Lakehouse landing area and downstream Bronze/Silver/Gold notebooks still run.
