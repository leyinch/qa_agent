# SCD Validation Testing Guide

## 🚀 Overview
The QA Agent now supports **SCD Type 1 and Type 2 Validation**. This feature validates the integrity of dimension tables by checking:
- **SCD Type 1**: Natural key uniqueness and null checks
- **SCD Type 2**: Historical tracking validity (no overlaps, gaps, or invalid flags)

## 🛠️ What Was Implemented

### Backend Changes
**File**: `backend/app/services/test_executor.py`
- Added `process_scd()` method to handle SCD validation requests
- Auto-selects appropriate tests based on SCD type (Type 1 or Type 2)

**File**: `backend/app/services/bigquery_service.py`
- Added `insert_scd_config()` method to insert new configurations into BigQuery

**File**: `backend/app/main.py`
- Added SCD mode handling in `/api/generate-tests` endpoint
- Added `/api/scd-config` endpoint to support adding new configurations

**File**: `backend/app/tests/predefined_tests.py`
- Added 13 SCD-specific test templates:
  - **SCD1 Tests**: `scd1_primary_key_null`, `scd1_primary_key_unique`
  - **SCD2 Tests**: `scd2_begin_date_null`, `scd2_end_date_null`, `scd2_flag_null`, `scd2_one_current_row`, `scd2_overlapping_dates`, `scd2_continuity`, `scd2_invalid_flag_combination`, `scd2_date_order`, `scd2_unique_begin_date`, `scd2_unique_end_date`, `scd2_no_record_after_current`
  - **Surrogate Key Tests**: `surrogate_key_null`, `surrogate_key_unique`

### Frontend Changes
**File**: `src/components/Sidebar.tsx`
- Added "SCD Validation" navigation option with 🔄 icon

**File**: `src/components/DashboardForm.tsx`
- Added SCD-specific form fields:
  - SCD Type selector (Type 1 / Type 2)
  - Natural Keys input (comma-separated)
  - Surrogate Key input (optional)
  - SCD2-specific fields: Begin Date Column, End Date Column, Active Flag Column
  - **New Feature**: "Add New Configuration" toggle allows adding new SCD tables to the configuration table directly from the UI

**File**: `src/app/page.tsx` & `src/app/dashboard/page.tsx`
- Updated `ComparisonMode` type to include `'scd'`

---

## 📋 Testing Instructions

### Prerequisites
✅ Backend deployed to Cloud Run: `data-qa-agent-backend`  
✅ Frontend deployed to Cloud Run: `data-qa-agent-frontend`

### Step 1: Create Mock Data in BigQuery

1. **Open BigQuery Console**:  
   https://console.cloud.google.com/bigquery?project=leyin-sandpit

2. **Run the Setup SQL**:  
   Copy and paste the entire contents of [`setup_scd_resources.sql`](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/setup_scd_resources.sql) into the query editor and click **Run**.

3. **Verify Tables Created**:
   - `leyin-sandpit.crown_scd_mock.D_Seat_WD` (4 rows)
   - `leyin-sandpit.crown_scd_mock.D_Employee_WD` (8 rows)
   - `leyin-sandpit.transform_config.scd_validation_config` (3 rows)

### Step 2: Test SCD Type 1 Validation

1. **Open the Frontend**:  
   Navigate to your Cloud Run frontend URL (e.g., `https://data-qa-agent-frontend-xxxxx.us-central1.run.app`)

2. **Select SCD Validation**:  
   Click "SCD Validation" in the sidebar (🔄 icon)

3. **Fill in the Form**:
   - **Project ID**: `leyin-sandpit`
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Seat_WD`
   - **SCD Type**: Select **Type 1**
   - **Natural Keys**: `TableId, PositionIDX`
   - **Surrogate Key** (optional): `DWSeatID`

4. **Run Tests**:  
   Click "Generate & Run Tests"

5. **Expected Results**:
   - ✅ **PASS**: `surrogate_key_null` (all rows have DWSeatID)
   - ✅ **PASS**: `surrogate_key_unique` (all DWSeatID values are unique)
   - ❌ **FAIL**: `scd1_primary_key_unique` - Should detect **1 duplicate** (TableId=101, PositionIDX=1 appears twice)
   - ❌ **FAIL**: `scd1_primary_key_null` - Should detect **1 null** (TableId=103 has NULL PositionIDX)

### Step 3: Test SCD Type 2 Validation

1. **Fill in the Form**:
   - **Project ID**: `leyin-sandpit`
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Employee_WD`
   - **SCD Type**: Select **Type 2**
   - **Natural Keys**: `UserId`
   - **Surrogate Key** (optional): `DWEmployeeID`
   - **Begin Date Column**: `DWBeginEffDateTime` (default)
   - **End Date Column**: `DWEndEffDateTime` (default)
   - **Active Flag Column**: `DWCurrentRowFlag` (default)

2. **Run Tests**:  
   Click "Generate & Run Tests"

3. **Expected Results**:
   - ✅ **PASS**: `scd2_begin_date_null`, `scd2_end_date_null`, `scd2_flag_null` (no nulls)
   - ✅ **PASS**: `surrogate_key_null`, `surrogate_key_unique`
   - ❌ **FAIL**: `scd2_overlapping_dates` - Should detect **UserId='U2'** (dates overlap: 2023-07-01 starts before 2023-08-01 ends)
   - ❌ **FAIL**: `scd2_one_current_row` - Should detect **UserId='U3'** (has 2 rows with DWCurrentRowFlag='Y')
   - ❌ **FAIL**: `scd2_date_order` - Should detect **UserId='U4'** (Begin Date > End Date)
   - ❌ **FAIL**: `scd2_continuity` - Should detect **UserId='U5'** (gap between 2023-03-01 and 2023-05-01)

### Step 4: Add New Configuration (New Feature)

1. **Toggle "Add New Configuration"**:
   - In "Config Table" mode, flip the toggle switch to enable adding a new configuration.

2. **Fill in details**:
   - **Config ID**: `temp_test_config`
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Seat_WD` (reusing for demo)
   - **Natural Keys**: `TableId`
   - **Description**: `Temporary test config added from UI`

3. **Click "Add Configuration"**:
   - Verify success message: "Configuration added successfully"

4. **Run Config Tests**:
   - Turn off the toggle.
   - Click "Generate & Run Tests" again.
   - Verify that your new `temp_test_config` is now included in the batch run results.

---

## 🔍 Understanding the Mock Data

### SCD1 Mock Table (`crown_scd_mock.D_Seat_WD`)
| TableId | PositionIDX | PositionCode | DWSeatID | Issue |
|---------|-------------|--------------|----------|-------|
| 101 | 1 | P1 | 1001 | ✅ Valid |
| 101 | 1 | P1_DUPE | 1002 | ❌ Duplicate natural key |
| 102 | 2 | P2 | 1003 | ✅ Valid |
| 103 | NULL | P3 | 1004 | ❌ NULL in natural key |

### SCD2 Mock Table (`crown_scd_mock.D_Employee_WD`)
| UserId | UserName | Begin Date | End Date | Flag | Issue |
|--------|----------|------------|----------|------|-------|
| U1 | User 1 Old | 2023-01-01 | 2023-06-01 | N | ✅ Valid |
| U1 | User 1 New | 2023-06-01 | 9999-12-31 | Y | ✅ Valid |
| U2 | User 2 A | 2023-01-01 | 2023-08-01 | N | ❌ Overlaps with next row |
| U2 | User 2 B | 2023-07-01 | 9999-12-31 | Y | ❌ Overlaps with previous row |
| U3 | User 3 A | 2023-01-01 | 9999-12-31 | Y | ❌ Multiple active flags |
| U3 | User 3 B | 2023-06-01 | 9999-12-31 | Y | ❌ Multiple active flags |
| U4 | User 4 | 2023-12-01 | 2023-01-01 | Y | ❌ Begin > End |
| U5 | User 5 A | 2023-01-01 | 2023-03-01 | N | ❌ Gap before next row |
| U5 | User 5 B | 2023-05-01 | 9999-12-31 | Y | ❌ Gap after previous row |

---

## 📝 Notes

### About the Config Table
The `transform_config.scd_validation_config` table can now be used for **batch validation** of multiple dimension tables.

**Two ways to use SCD Validation:**
1. **Direct Input** (for testing individual tables): Manually enter dataset, table, and key information
2. **Config Table** (for batch validation): Read configurations from `scd_validation_config` and validate all tables at once

**Using Config Table Mode:**
1. Navigate to SCD Validation in the UI
2. Toggle to "Config Table" mode
3. Enter:
   - **Config Dataset**: `transform_config`
   - **Config Table**: `scd_validation_config`
4. Click "Generate & Run Tests"
5. The app will validate ALL tables defined in the config table

**Current Config Table Contents:**
- `crown_scd_mock.D_Seat_WD` (SCD Type 1) - Gaming Seats mock data with intentional errors
- `crown_scd_mock.D_Employee_WD` (SCD Type 2) - Employee mock data with intentional errors

> [!NOTE]
> The mock tables use production naming convention. These tables contain test data with intentional errors for validation testing.

### Service Account Permissions
Ensure the Cloud Run service account has these BigQuery permissions:
- `BigQuery Data Viewer`
- `BigQuery Job User`
- `BigQuery Data Editor` (Required for adding new configurations)

---

## 📄 Related Files
- [DashboardForm.tsx](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/src/components/DashboardForm.tsx) - Frontend form with SCD fields
- [Sidebar.tsx](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/src/components/Sidebar.tsx) - Navigation with SCD option
- [test_executor.py](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/backend/app/services/test_executor.py) - Backend SCD processing logic
- [predefined_tests.py](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/backend/app/tests/predefined_tests.py) - SCD test definitions
- [setup_scd_resources.sql](file:///c:/Users/LeyinChen/Documents/Client%20-%20Crown/Antigravity/qa_agent/setup_scd_resources.sql) - BigQuery setup script
