# SCD Validation Testing Guide

## 🚀 Overview
The QA Agent now supports **SCD Type 1 and Type 2 Validation**. This feature validates the integrity of dimension tables by checking:
- **SCD Type 1**: Primary Key uniqueness and null checks
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
- Added 16 data quality test templates:
  - **SCD1 Validation Suite (5 Tests)**:
    - ✅ Table exists (smoke)
    - ✅ Primary Key NOT NULL
    - ✅ Primary Key uniqueness
    - ✅ Surrogate key NOT NULL
    - ✅ Surrogate key uniqueness
  - **SCD2 Validation Suite (15 Tests)**:
    - ✅ Table exists (smoke)
    - ✅ Primary Key NOT NULL
    - ✅ Surrogate key NOT NULL
    - ✅ Surrogate key uniqueness
    - ✅ Begin effective datetime NOT NULL
    - ✅ End effective datetime NOT NULL
    - ✅ Current row flag NOT NULL
    - ✅ One current row per Primary Key
    - ✅ Current rows end on 2099-12-31
    - ✅ No invalid current-row combinations
    - ✅ Begin < End datetime
    - ✅ Unique begin datetime per Primary Key
    - ✅ Unique end datetime per Primary Key
    - ✅ Continuous history (no gaps) (Using DATE_ADD with 1-second intervals)
    - ✅ No record after current row

### Frontend Changes
**File**: `src/components/Sidebar.tsx`
- Added "SCD Validation" navigation option with 🔄 icon

**File**: `src/components/DashboardForm.tsx`
- Added SCD-specific form fields:
  - SCD Type selector (Type 1 / Type 2)
  - Primary Keys input (comma-separated)
  - Surrogate Key input (optional)
  - SCD2-specific fields: Begin Date Column, End Date Column, Active Flag Column
  - **New Feature**: "Add New Configuration" toggle allows adding new SCD tables to the configuration table directly from the UI

**File**: `src/app/page.tsx` & `src/app/dashboard/page.tsx`
- Updated `ComparisonMode` type to include `'scd'`

**File**: `src/components/ResultsView.tsx` (Major Update)
- **Tabbed UI**: Batch results are now organized into selectable tabs by mapping ID
- **Bad Data Preview**: Added "View Bad Data" button to display actual problematic rows from BigQuery
- **SQL Transparency**: Added "Show SQL" button for every test to view the underlying BQ query
- **AI Integration**: Added display and one-click saving for AI-suggested custom tests
- **Comprehensive Typing**: Fully refactored with strict TypeScript interfaces for all data structures

### Advanced Result Analysis
The results page now provides deep insight into test failures:
- **Status Badges**: Color-coded badges for PASS (Green), FAIL (Red), and ERROR (Amber)
- **Summary Cards**: At-a-glance mapping counts and success rates
- **Sample Data Grid**: Inline tables showing the specific values that triggered validation failures
- **AI Recommendation Engine**: "🤖 AI Suggested Tests" section providing context-aware testing improvements

---

## 📋 Testing Instructions

### Prerequisites
✅ Backend deployed to Cloud Run: `data-qa-agent-backend`  
✅ Frontend deployed to Cloud Run: `data-qa-agent-frontend`

### Step 1: Create Mock Data in BigQuery

1. **Open BigQuery Console**:  
   https://console.cloud.google.com/bigquery?project=leyin-sandpit

2. **Run the Setup SQL**:  
   Copy and paste the entire contents of [`setup_scd_test_resources.sql`](./setup_scd_test_resources.sql) into the query editor and click **Run**. (Note: Replace `{{PROJECT_ID}}` with your actual GCP Project ID).

3. **Verify Tables Created**:
   - `leyin-sandpit.crown_scd_mock.D_Seat_WD` (4 rows)
   - `leyin-sandpit.crown_scd_mock.D_Employee_WD` (9 rows)
   - `leyin-sandpit.crown_scd_mock.D_Player_WD` (11 rows)
   - `leyin-sandpit.config.scd_validation_config` (3 rows)

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
   - **Primary Keys**: `TableId, PositionIDX`
   - **Surrogate Key** (optional): `DWSeatID`

4. **Run Tests**:  
   Click "Generate & Run Tests"

5. **Expected Results (5 Tests)**:
   - ✅ **PASS**: `table_exists` (Smoke test)
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
   - **Primary Keys**: `UserId`
   - **Surrogate Key** (optional): `DWEmployeeID`
   - **Begin Date Column**: `DWBeginEffDateTime` (default)
   - **End Date Column**: `DWEndEffDateTime` (default)
   - **Active Flag Column**: `DWCurrentRowFlag` (default)

2. **Run Tests**:  
   Click "Generate & Run Tests"

   - ❌ **FAIL**: `scd2_current_date_check` - Should detect **Primary Key='U4'** (active flag 'Y' but end date not 2099)

### Step 4: Test SCD Type 2 with Business Rules (`D_Player_WD`)

1. **Fill in the Form**:
   - **Project ID**: `leyin-sandpit`
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Player_WD`
   - **SCD Type**: Select **Type 2**
   - **Primary Keys**: `PlayerId`
   - **Surrogate Key**: `DWPlayerID`
   - **Custom Tests**: (Already pre-configured in the config table, but you can enter them manually if using Direct Input)

2. **Run Tests**:  
   Click "Generate & Run Tests"

3. **Expected Results**:
   - ✅ **SCD Validation**: Detects standard SCD2 failures (P2 overlap, P3 multi-flag, P4 date order, P5 gap).
   - ❌ **Business Rule Failure (P6)**: `CreatedDtm Not Null` fails for PlayerId='P6'.
   - ❌ **Business Rule Failure (P7)**: `CreatedDtm before UpdatedDtm` fails for PlayerId='P7'.

### Step 5: Batch Validation using Config Table

1. **Navigate to SCD Validation**
2. **Toggle to "Config Table" mode**
3. **Enter Configuration Details**:
   - **Config Dataset**: `config`
   - **Config Table**: `scd_validation_config`
4. **Click "Generate & Run Tests"**
5. **Expected Results**:
   - The UI will show **3 separate tabs**: `seat_scd1`, `employee_scd2`, and `player_scd2`.
   - You can toggle between tabs to see the detailed results for each table in the batch.

### Step 6: Add New Configuration (New Feature)

1. **Toggle "Add New Configuration"**:
   - In "Config Table" mode, flip the toggle switch to enable adding a new configuration.

2. **Fill in details**:
   - **Config ID**: `temp_test_config`
   - **Target Dataset**: `crown_scd_mock`
   - **Target Table**: `D_Seat_WD` (reusing for demo)
   - **Primary Keys**: `TableId`
   - **Description**: `Temporary test config added from UI`

3. **Click "Add Configuration"**:
   - Verify success message: "Configuration added successfully"

4. **Run Config Tests**:
   - Turn off the toggle.
   - Click "Generate & Run Tests" again.
   - Verify that your new `temp_test_config` is now included in the batch run results.

### Step 7: Analyze Failures with Bad Data Preview
1. **Find a Failed Test**:
   - Locate a test with a ❌ **FAIL** status (e.g., `scd2_continuity`).
2. **View Problematic Rows**:
   - Click the **"View Bad Data"** button.
   - A grid will appear showing the specific rows in BigQuery that caused the failure.
3. **Compare with SQL**:
   - Click **"Show SQL"** to see the exact query generated. You can copy this into the BigQuery console for further debugging.

### Step 8: Expand Coverage with AI Suggestions
1. **Scroll to AI Section**:
   - At the bottom of each mapping's results, find the **"🤖 AI Suggested Tests"** section.
2. **Review Reasoning**:
   - Read the AI's logic for why it suggested specific tests like `outlier_value_check` or `cross_column_consistency`.
3. **Save to Custom Tests**:
   - Click **"+ Add to Custom"**.
   - The test is now registered in your custom test suite and will run in future batch executions.

---

## 🔍 Understanding the Mock Data

### SCD1 Mock Table (`crown_scd_mock.D_Seat_WD`)
| TableId | PositionIDX | PositionCode | DWSeatID | Issue |
|---------|-------------|--------------|----------|-------|
| 101 | 1 | P1 | 1001 | ✅ Valid |
| 101 | 1 | P1_DUPE | 1002 | ❌ Duplicate primary key |
| 102 | 2 | P2 | 1003 | ✅ Valid |
| 103 | NULL | P3 | 1004 | ❌ NULL in primary key |

### SCD2 Mock Table (`crown_scd_mock.D_Employee_WD`)
| UserId | UserName | Begin Date | End Date | Flag | Issue |
|--------|----------|------------|----------|------|-------|
| U1 | User 1 New | 2023-06-01 | 2099-12-31 | Y | ✅ Valid |
| U2 | User 2 A | 2023-01-01 | 2023-08-01 | N | ❌ Overlaps with next row |
| U3 | User 3 A | 2023-01-01 | 2099-12-31 | Y | ❌ Multiple active flags |
| U4 | User 4 | 2023-12-01 | 2023-01-01 | Y | ❌ Begin > End |
| U5 | User 5 A | 2023-01-01 | 2023-03-01 | N | ❌ Gap before next row |

### SCD2 with Business Rules (`crown_scd_mock.D_Player_WD`)
| PlayerId | Issue Type | Description |
|----------|------------|-------------|
| P2, P3, P4, P5 | SCD2 Errors | Overlaps, Multi-flag, Date Order, Gaps (similar to Employee table) |
| P6 | Business Rule | `CreatedDtm` is NULL |
| P7 | Business Rule | `CreatedDtm` > `UpdatedDtm` |

---

## 📝 Notes

### About the Config Table
The `config.scd_validation_config` table can now be used for **batch validation** of multiple dimension tables.

**Two ways to use SCD Validation:**
1. **Direct Input** (for testing individual tables): Manually enter dataset, table, and key information
2. **Config Table** (for batch validation): Read configurations from `scd_validation_config` and validate all tables at once

**Using Config Table Mode:**
1. Navigate to SCD Validation in the UI
2. Toggle to "Config Table" mode
3. Enter:
   - **Config Dataset**: `config`
   - **Config Table**: `scd_validation_config`
4. Click "Generate & Run Tests"
5. The app will validate ALL tables defined in the config table

**Current Config Table Contents:**
- `crown_scd_mock.D_Seat_WD` (`seat_scd1`) - SCD Type 1 mock data.
- `crown_scd_mock.D_Employee_WD` (`employee_scd2`) - SCD Type 2 mock data.
- `crown_scd_mock.D_Player_WD` (`player_scd2`) - SCD Type 2 with custom business rules.

> [!NOTE]
> The mock tables use production naming convention. These tables contain test data with intentional errors for validation testing.

### Service Account Permissions
Ensure the Cloud Run service account has these BigQuery permissions:
- `BigQuery Data Viewer`
- `BigQuery Job User`
- `BigQuery Data Editor` (Required for adding new configurations)

---

## 📄 Related Files
- [DashboardForm.tsx](src/components/DashboardForm.tsx) - Frontend form with SCD fields
- [ResultsView.tsx](src/components/ResultsView.tsx) - Advanced results dashboard with tabs, data previews, and AI
- [Sidebar.tsx](src/components/Sidebar.tsx) - Navigation with SCD option
- [test_executor.py](backend/app/services/test_executor.py) - Backend SCD processing logic
- [predefined_tests.py](backend/app/services/predefined_tests.py) - SCD test definitions
- [setup_scd_test_resources.sql](./setup_scd_test_resources.sql) - Combined BigQuery setup script
