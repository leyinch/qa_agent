# SCD Validation Testing Guide

## 🚀 Overview

The QA Agent now supports **SCD Type 1 and Type 2 Validation**. This feature validates the integrity of dimension tables by checking:
- **SCD Type 1**: Standard dimension tables (Overwrites old data). Validation checks for Primary Key uniqueness and nulls.
- **SCD Type 2**: Historical dimension tables (Tracks history). Validation checks for chronological consistency, active flags, and gaps/overlaps.

---

## 🧠 System Logic & Architecture

### 1. Scheduler Logic
The system includes a robust integration with **Google Cloud Scheduler** to automate testing.
- **Timezone**: All schedules run in **Melbourne Time (Australia/Melbourne)**.
- **Sync Mechanism**: The backend automatically keeps Cloud Scheduler in sync with your BigQuery config table.
  - **Auto-Sync**: Occurs internally to ensure system integrity.
  - **Manual Sync**: Triggered via the **🔄 Sync Jobs** button in the UI or via API (`/api/sync-scheduler`). Use this after making manual deletions or changes directly in BigQuery.
  - **Self-Healing**: The system automatically maintains a `qa-agent-master-sync` job that triggers a full sync hourly. This job is self-healing and will be recreated if deleted.
  - **Cleanup**: Obsolete jobs (configs deleted from BQ) are automatically removed during any sync operation.
- **Execution Source**: The history table distinguishes how a test was triggered:
  - `Scheduled Run`: Triggered automatically at the configured cron time (e.g., 9:00 AM).
  - `Manual Run`: Triggered manually via the UI or "Force Run" in Console.

### 2. History & Reporting
All test results—whether run manually via the frontend or automatically via the scheduler—are logged to a **single source of truth**:
- **Table**: `leyin-sandpit.qa_results.scd_test_history`
- **Timestamps**: Stored in **Melbourne Local Time** for easy readability (DATETIME).
- **Partitioning**: The table is partitioned by day for performance.

---

## 📋 Testing Instructions

### Prerequisites
✅ Backend deployed to Cloud Run: `data-qa-agent-backend2`  
✅ Frontend deployed to Cloud Run: `data-qa-agent-frontend2`

### Step 1: Create Mock Data in BigQuery

If you haven't already, run the master setup script to populate your environment with test data and tables.

1. **Open BigQuery Console**: [Link](https://console.cloud.google.com/bigquery)
2. **Run Script**: Copy and run the contents of [`setup_scd_resources.sql`](setup_scd_resources.sql).
3. **Verify Resources**:
   - `crown_scd_mock.D_Seat_WD` (SCD1 Mock)
   - `crown_scd_mock.D_Employee_WD` (SCD2 Mock)
   - `crown_scd_mock.D_Player_WD` (SCD2 Mock)
   - `config.scd_validation_config` (Config Table)
   - `qa_results.scd_test_history` (History Table)

### Step 2: Test SCD Type 1 Validation (Manual)

1. Navigate to **SCD Validation** in the Frontend (🔄 icon).
2. Enter the following details:
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Seat_WD`
   - **SCD Type**: `Type 1`
   - **Primary Keys**: `TableId, PositionIDX`
   - **Surrogate Key**: `DWSeatID`
3. Click **Generate & Run Tests**.
4. **Expected Results**:
   - ✅ `table_exists`, `surrogate_key_null/unique` -> **PASS**
   - ❌ `scd1_primary_key_unique` -> **FAIL** (Duplicate detected)
   - ❌ `scd1_primary_key_null` -> **FAIL** (Null detected)

### Step 3: Test SCD Type 2 Validation (Manual)

1. In the same form, verify **Type 2** settings:
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Employee_WD`
   - **SCD Type**: `Type 2`
   - **Primary Keys**: `UserId`
   - **Surrogate Key**: `DWEmployeeID`
   - **Columns**: Default settings (`DWBeginEffDateTime`, etc.)
2. Click **Generate & Run Tests**.
3. **Expected Results**:
   - ✅ Null/Unique Checks -> **PASS**
   - ❌ `scd2_continuity` -> **FAIL** (Overlaps/Gaps detected)
   - ❌ `scd2_one_current_row` -> **FAIL** (Multiple active flags)

### Step 4: Add New Configuration (Saved for Automation)

You can add a table to the permanent configuration so it runs automatically.

1. Toggle **"Config Table Mode"** switch.
2. Toggle **"Add New Configuration"** switch.
3. Fill in the form (e.g., for `D_Seat_WD` or a new table).
4. Select **Schedule Frequency** (e.g., Daily).
5. Click **Add Configuration**.
   - **Result**: The config is saved to BQ, and a Cloud Scheduler job is **instantly created**.

### Step 5: Synchronizing External Changes

If you delete a configuration record manually via the BigQuery Console:
1. Navigate to the **Config Table** mode in the UI.
2. Click the **🔄 Sync Jobs** button.
3. **Expected Result**: Any orphaned Cloud Scheduler jobs will be deleted, and the master sync job will be verified/recreated.

---

## 🔍 Understanding the Mock Data

The mock tables are designed to fail specific tests to demonstrate validity.

### SCD1 Mock Table (`D_Seat_WD`)
| TableId | PositionIDX | Issue |
|---------|-------------|-------|
| 101 | 1 | ✅ Valid |
| 101 | 1 | ❌ Duplicate PK |
| 103 | NULL | ❌ NULL PK |

### SCD2 Mock Table (`D_Employee_WD`)
| UserId | Begin | End | Flag | Issue |
|--------|-------|-----|------|-------|
| U2 | Jan 1 | Aug 1 | N | ❌ Overlaps with next row |
| U2 | Jul 1 | Dec 31| Y | ❌ Overlaps with prev row |
| U4 | Dec 1 | Jan 1 | Y | ❌ Begin Date > End Date |
| U5 | Jan 1 | Mar 1 | N | ❌ Gap before next row (May 1) |

---

## �️ Troubleshooting & Notes

### Bad Data Preview
If a test fails, click the **"View Bad Data"** button in the results to see the exact rows causing the failure. You can also click **"Show SQL"** to debug the query in BigQuery.

### Service Account Permissions
Ensure your Cloud Run service account has:
- `BigQuery Data Viewer` & `BigQuery Data Editor`
- `BigQuery Job User`
- `Cloud Scheduler Admin` (for creating jobs)

### Managing Configs via SQL
You can also manage configs directly in BigQuery:
```sql
INSERT INTO `leyin-sandpit.config.scd_validation_config`
(config_id, target_dataset, target_table, scd_type, primary_keys, cron_schedule)
VALUES
('my_new_table', 'my_ds', 'my_table', 'scd1', ['id'], '0 9 * * *');
```
*Note: Run `/api/sync-scheduler` after manual SQL inserts if you want the job created immediately.*

---

## 📄 Related Files
- **Frontend**: [`DashboardForm.tsx`](src/components/DashboardForm.tsx), [`ResultsView.tsx`](src/components/ResultsView.tsx)
- **Backend Logic**: [`test_executor.py`](backend/app/services/test_executor.py), [`scheduler_service.py`](backend/app/services/scheduler_service.py)
- **Test Definitions**: [`predefined_tests.py`](backend/app/tests/predefined_tests.py)
- **Setup**: [`setup_scd_resources.sql`](setup_scd_resources.sql)
