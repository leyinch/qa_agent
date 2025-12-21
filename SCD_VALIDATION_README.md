# SCD Validation Handover & Instructions

This document summarizes the implementation of Slowly Changing Dimension (SCD) validation and outlines the steps to verify the feature.

## 🚀 Overview
The QA Agent now supports **SCD Type 1 and Type 2 Validation**. This allows you to verify the integrity of dimension tables, ensuring natural keys are unique/not null and that history tracking (SCD2) is valid without overlaps or gaps.

## 🛠️ Implementation Details

### 1. Backend
- **New API Endpoint logic**: Added dynamic test selection based on `scd_type` in `backend/app/services/test_executor.py`.
- **Predefined Tests**: Added 13+ tests in `backend/app/tests/predefined_tests.py`:
  - `scd1_primary_key_null`, `scd1_primary_key_unique`
  - `scd2_begin_date_null`, `scd2_end_date_null`, `scd2_flag_null`
  - `scd2_one_current_row`, `scd2_overlapping_dates`, `scd2_continuity` (gap check)
  - `scd2_invalid_flag_combination`, `scd2_date_order` (Begin < End)
  - `surrogate_key_null`, `surrogate_key_unique`

### 2. Frontend
- **Navigation**: Added "SCD Validation" to the Sidebar.
- **Form**: Created a dedicated setup UI in `DashboardForm.tsx` that conditionally renders fields for:
  - SCD Type (1 or 2)
  - Natural Keys (comma-separated)
  - Surrogate Key (optional)
  - SCD2-specific columns: `DWBeginEffDateTime`, `DWEndEffDateTime`, `DWCurrentRowFlag`

### 3. Config-Driven Approach
- **Why `transform_config`?**: We created a `scd_validation_config` table in BigQuery. This allows the system to store "known" configurations for dimensions (like `D_Seat_WD`).
- **Benefit**: Instead of typing column names every time, the Agent can look up the configuration, making the test run repeatable and scalable across all dimensions.

---

## 📋 Verification Steps

### Step 1: Deploy Changes
Ensure the updated backend and frontend code is deployed to your Cloud Run instance.

### Step 2: Set Up Mock Data
Since `python` was not found in your local environment, use the SQL method:
1. Open the BigQuery console.
2. Run the SQL from: [setup_scd_resources.sql](file:///c:/Users/LeyinChen/Documents/Client - Crown/Antigravity/qa_agent/setup_scd_resources.sql)
   - This creates `crown_scd_mock.scd1_mock` and `scd2_mock` with **intentional errors**.

### Step 3: Run Validation in UI
1. Select **SCD Validation** in the Sidebar.
2. **Test SCD1**:
   - Table: `crown_scd_mock.scd1_mock`
   - Natural Keys: `TableId, PositionIDX`
   - **Expect**: `scd1_primary_key_unique` should fail (item 101 has duplicates).
3. **Test SCD2**:
   - Table: `crown_scd_mock.scd2_mock`
   - Natural Keys: `UserId`
   - **Expect**: 
     - `scd2_continuity` should fail for `U5` (gap).
     - `scd2_overlapping_dates` should fail for `U2`.
     - `scd2_one_current_row` should fail for `U3` (two 'Y' flags).

---

## ⚠️ Known Issues / Environment Notes
- **Local Python**: The `python` command is not currently in your system's PATH. Use the `setup_scd_resources.sql` file as an alternative for data setup.
- **Service Account**: Ensure the Cloud Run service account has `BigQuery Data Viewer` and `BigQuery Job User` permissions on both `leyin-sandpit` and `miruna-sandpit`.

---

## 📄 Related Files
- [DashboardForm.tsx](file:///c:/Users/LeyinChen/Documents/Client - Crown/Antigravity/qa_agent/src/components/DashboardForm.tsx)
- [test_executor.py](file:///c:/Users/LeyinChen/Documents/Client - Crown/Antigravity/qa_agent/backend/app/services/test_executor.py)
- [predefined_tests.py](file:///c:/Users/LeyinChen/Documents/Client - Crown/Antigravity/qa_agent/backend/app/tests/predefined_tests.py)
- [setup_scd_resources.sql](file:///c:/Users/LeyinChen/Documents/Client - Crown/Antigravity/qa_agent/setup_scd_resources.sql)
