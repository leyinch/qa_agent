# 🗺️ Data QA Agent - Development Roadmap

> **Last Updated:** December 2024  
> **Status:** Active Development  
> **Primary Goal:** Automated daily execution with every data update

---

## 📊 Current State

The QA Agent currently supports:
- ✅ GCS to BigQuery file comparison
- ✅ Schema validation against ERD descriptions
- ✅ Basic predefined tests (row count, nulls, duplicates, patterns)
- ✅ AI-powered test suggestions via Vertex AI
- ✅ Execution history logging
- ✅ Custom test saving

**Current Limitations:**
- ❌ Requires manual UI interaction to run tests
- ❌ No automated scheduling or triggers
- ❌ No alerting on test failures
- ❌ Missing SCD (Slowly Changing Dimension) tests
- ❌ No surrogate key validation

---

## 🎯 Roadmap Overview

**Goal:** Transform from manual UI tool → Fully automated daily testing agent

```
Phase 1: Automation Foundation   [Week 1-2]     ← START HERE
    ↓
Phase 2: Alerting & Monitoring   [Week 2-3]
    ↓
Phase 3: Test Coverage Expansion [Week 3-5]
    ↓
Phase 4: Enterprise Maturity     [Week 5-8]
```

---

## Phase 1: Automation Foundation ⚡
**Timeline:** Week 1-2  
**Priority:** CRITICAL  
**Goal:** Enable headless, automated test execution

### 1.1 Headless Execution API
Create API endpoint that runs tests without UI interaction.

**New Endpoint:**
```python
# backend/app/main.py
@app.post("/api/run-scheduled-tests")
async def run_scheduled_tests(
    project_id: str,
    config_dataset: str = "config",
    config_table: str = "data_load_config"
):
    """Run all tests from config table - no UI required"""
    result = await test_executor.process_config_table(
        project_id=project_id,
        config_dataset=config_dataset,
        config_table=config_table
    )
    return result
```

**Deliverables:**
| Item | Description | Effort |
|------|-------------|--------|
| `/api/run-scheduled-tests` endpoint | Headless test execution | 0.5 day |
| CLI script `scripts/run_tests.py` | Command-line execution | 0.5 day |
| Exit codes (0=pass, 1=fail) | For CI/CD integration | 0.5 day |
| Environment config | Default project/dataset/table | 0.5 day |

### 1.2 Cloud Scheduler Integration
Set up automated daily execution.

**Setup:**
```bash
# Create Cloud Scheduler job - runs daily at 6 AM
gcloud scheduler jobs create http qa-agent-daily \
  --schedule="0 6 * * *" \
  --uri="https://your-backend-url/api/run-scheduled-tests" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --body='{"project_id":"your-project","config_dataset":"config","config_table":"data_load_config"}'
```

**Deliverables:**
| Item | Description | Effort |
|------|-------------|--------|
| Cloud Scheduler job | Daily 6 AM trigger | 0.5 day |
| Service account setup | Proper IAM permissions | 0.5 day |
| Health check endpoint | `/health` for monitoring | 0.5 day |

### 1.3 Pub/Sub Trigger (On Data Update)
Trigger tests automatically when data lands in BigQuery.

**New Endpoint:**
```python
@app.post("/api/pubsub-trigger")
async def pubsub_trigger(message: dict):
    """Triggered by BigQuery data update via Pub/Sub"""
    # Parse table info from Pub/Sub message
    table_info = parse_bq_notification(message)
    # Find matching config and run tests
    result = await test_executor.process_single_mapping(table_info)
    return result
```

**Deliverables:**
| Item | Description | Effort |
|------|-------------|--------|
| Pub/Sub trigger endpoint | React to data updates | 1 day |
| BigQuery audit log sink | Capture table updates | 0.5 day |
| Table-to-config mapping | Match updates to test configs | 0.5 day |

---

## Phase 2: Alerting & Monitoring 🚨
**Timeline:** Week 2-3  
**Priority:** HIGH  
**Goal:** Immediate notification on test failures

### 2.1 Notification Service
Create centralized alerting service.

**New Service:**
```python
# backend/app/services/notification_service.py
class NotificationService:
    async def send_alert(self, result: dict):
        if result['summary']['failed'] > 0:
            await self._send_slack(result)
            await self._send_email(result)
    
    async def _send_slack(self, result: dict):
        webhook_url = settings.slack_webhook_url
        payload = {
            "text": f"🚨 QA Tests Failed",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": "Data Quality Alert"}},
                {"type": "section", "text": {"type": "mrkdwn", 
                    "text": f"*{result['summary']['failed']}* tests failed out of *{result['summary']['total_tests']}*"}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"✅ Passed: {result['summary']['passed']}"},
                    {"type": "mrkdwn", "text": f"❌ Failed: {result['summary']['failed']}"}
                ]}
            ]
        }
        await httpx.post(webhook_url, json=payload)
```

**Deliverables:**
| Item | Description | Effort |
|------|-------------|--------|
| NotificationService class | Centralized alerting | 1 day |
| Slack integration | Webhook notifications | 0.5 day |
| Email integration | SMTP/SendGrid alerts | 0.5 day |
| PagerDuty integration | Critical failure escalation | 0.5 day |

### 2.2 Alert Configuration
Make alerting configurable per environment.

**Environment Variables:**
```bash
# Alert configuration
ALERT_ON_FAILURE=true
ALERT_CHANNELS=slack,email
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx
ALERT_EMAIL_RECIPIENTS=team@company.com
PAGERDUTY_ROUTING_KEY=xxx

# Thresholds
ALERT_FAILURE_THRESHOLD=1        # Alert if >= N failures
ALERT_ERROR_THRESHOLD=1          # Alert if >= N errors
```

**Deliverables:**
| Item | Description | Effort |
|------|-------------|--------|
| Config settings | Alert thresholds & channels | 0.5 day |
| Per-table alert config | Override defaults per mapping | 0.5 day |
| Alert templates | Customizable message formats | 0.5 day |

### 2.3 Monitoring Dashboard
Track test execution over time.

**Deliverables:**
| Item | Description | Effort |
|------|-------------|--------|
| Execution metrics endpoint | `/api/metrics` | 0.5 day |
| BigQuery dashboard view | Historical pass/fail rates | 1 day |
| Trend analysis queries | Detect degradation | 0.5 day |

---

## Phase 3: Test Coverage Expansion 🧪
**Timeline:** Week 3-5  
**Priority:** HIGH  
**Goal:** Comprehensive test coverage matching enterprise standards

### 3.1 Surrogate Key Tests
| Test ID | Test Name | SQL Template | Severity |
|---------|-----------|--------------|----------|
| `surrogate_key_null` | Surrogate Key Null Check | `SELECT COUNT(0) = 0 FROM {{target}} WHERE {{surrogateKey}} IS NULL` | HIGH |
| `surrogate_key_unique` | Surrogate Key Uniqueness | `SELECT COUNT(0) = COUNT(DISTINCT {{surrogateKey}}) FROM {{target}}` | HIGH |

### 3.2 Template Variable System
Implement `{{variable}}` placeholder support for reusable tests.

**Variables:**
- `{{target}}` - Full table name (`project.dataset.table`)
- `{{primaryKey}}` - Primary key column(s), concatenated for composite
- `{{surrogateKey}}` - Surrogate key column
- `{{beginDate}}` - SCD2 effective begin date
- `{{endDate}}` - SCD2 effective end date
- `{{currentFlag}}` - SCD2 current row flag

### 3.3 SCD Configuration Schema
Extend config table to support SCD metadata.

**New Config Columns:**
```sql
ALTER TABLE config.data_load_config ADD COLUMN scd_type STRING;           -- 'scd1', 'scd2', 'event'
ALTER TABLE config.data_load_config ADD COLUMN surrogate_key STRING;      -- 'DWSeatID'
ALTER TABLE config.data_load_config ADD COLUMN current_flag STRING;       -- 'DWCurrentRowFlag'
ALTER TABLE config.data_load_config ADD COLUMN begin_date STRING;         -- 'DWBeginEffDateTime'
ALTER TABLE config.data_load_config ADD COLUMN end_date STRING;           -- 'DWEndEffDateTime'
```

### 3.4 SCD2 Tests (12 New Tests)

#### Null Checks
| Test ID | SQL Template | Severity |
|---------|--------------|----------|
| `scd2_begin_date_null` | `SELECT COUNT(0) = 0 FROM {{target}} WHERE {{beginDate}} IS NULL` | HIGH |
| `scd2_end_date_null` | `SELECT COUNT(0) = 0 FROM {{target}} WHERE {{endDate}} IS NULL` | HIGH |
| `scd2_current_flag_null` | `SELECT COUNT(0) = 0 FROM {{target}} WHERE {{currentFlag}} IS NULL` | HIGH |

#### Date Validation
| Test ID | SQL Template | Severity |
|---------|--------------|----------|
| `scd2_date_order` | `SELECT COUNT(0) = COUNTIF({{beginDate}} < {{endDate}}) FROM {{target}}` | HIGH |
| `scd2_begin_date_unique` | `SELECT COUNT(0) = COUNT(DISTINCT TO_JSON_STRING(STRUCT({{primaryKey}}, {{beginDate}}))) FROM {{target}}` | MEDIUM |
| `scd2_end_date_unique` | `SELECT COUNT(0) = COUNT(DISTINCT TO_JSON_STRING(STRUCT({{primaryKey}}, {{endDate}}))) FROM {{target}}` | MEDIUM |

#### Current Record Validation
| Test ID | SQL Template | Severity |
|---------|--------------|----------|
| `scd2_single_current` | `SELECT COUNTIF({{currentFlag}} = TRUE) = COUNT(DISTINCT {{primaryKey}}) FROM {{target}}` | HIGH |
| `scd2_current_end_date` | `SELECT COUNT(0) = COUNTIF({{endDate}} = '2099-12-31T00:00:00') FROM {{target}} WHERE {{currentFlag}} = TRUE` | HIGH |
| `scd2_current_flag_consistency` | `SELECT COUNT(0) = 0 FROM {{target}} WHERE {{currentFlag}} = TRUE AND {{endDate}} <> '2099-12-31'` | MEDIUM |

#### History Continuity
| Test ID | SQL Template | Severity |
|---------|--------------|----------|
| `scd2_no_gaps` | See full SQL in implementation | MEDIUM |
| `scd2_no_future_after_current` | See full SQL in implementation | LOW |

### 3.5 Test Type Filtering
Only run applicable tests based on SCD type.

```python
PREDEFINED_TESTS = {
    'surrogate_key_null': TestTemplate(..., applicable_types=['scd1', 'scd2', 'event']),
    'scd2_single_current': TestTemplate(..., applicable_types=['scd2']),
    'scd1_pk_unique': TestTemplate(..., applicable_types=['scd1']),
}
```

---

## Phase 4: Enterprise Maturity 🏢
**Timeline:** Week 5-8  
**Priority:** MEDIUM  
**Goal:** Production-grade reliability and observability

### 4.1 Airflow/Composer Integration
Full integration with data pipeline orchestration.

**Airflow DAG:**
```python
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import BranchPythonOperator

with DAG('data_qa_pipeline', schedule_interval=None) as dag:
    
    run_tests = SimpleHttpOperator(
        task_id='run_qa_tests',
        http_conn_id='qa_agent',
        endpoint='/api/run-scheduled-tests',
        method='POST',
        data='{"project_id":"{{var.value.gcp_project}}"}',
    )
    
    check_results = BranchPythonOperator(
        task_id='check_results',
        python_callable=lambda ti: 'alert' if ti.xcom_pull(task_ids='run_qa_tests')['summary']['failed'] > 0 else 'success'
    )
    
    # ETL tasks depend on QA passing
    etl_task >> run_tests >> check_results
```

### 4.2 Parallel Test Execution
Speed up execution for large config tables.

```python
async def process_config_table_parallel(self, ...):
    mappings = await bigquery_service.read_config_table(...)
    
    # Run mappings in parallel (max 10 concurrent)
    semaphore = asyncio.Semaphore(10)
    async def process_with_limit(mapping):
        async with semaphore:
            return await self.process_mapping(mapping)
    
    results = await asyncio.gather(*[process_with_limit(m) for m in mappings])
    return results
```

### 4.3 Result Caching
Cache query results to avoid redundant execution.

### 4.4 Multi-Environment Support
- Dev/Staging/Production configurations
- Environment-specific thresholds
- Cross-environment comparison

---

## 📋 Implementation Priority Matrix

| Priority | Phase | Feature | Effort | Impact | Week |
|----------|-------|---------|--------|--------|------|
| 🔴 P0 | 1.1 | Headless API endpoint | 1 day | Critical | 1 |
| 🔴 P0 | 1.2 | Cloud Scheduler setup | 1 day | Critical | 1 |
| 🔴 P0 | 2.1 | Slack alerting | 1 day | Critical | 2 |
| 🟠 P1 | 1.3 | Pub/Sub trigger | 2 days | High | 2 |
| 🟠 P1 | 2.2 | Alert configuration | 1 day | High | 2 |
| 🟠 P1 | 3.1 | Surrogate key tests | 1 day | High | 3 |
| 🟠 P1 | 3.2 | Template variables | 2 days | High | 3 |
| 🟡 P2 | 3.3 | SCD config schema | 1 day | High | 3 |
| 🟡 P2 | 3.4 | SCD2 tests (12) | 3 days | High | 4 |
| 🟡 P2 | 3.5 | Test type filtering | 1 day | Medium | 4 |
| 🟢 P3 | 4.1 | Airflow integration | 2 days | Medium | 5 |
| 🟢 P3 | 4.2 | Parallel execution | 2 days | Medium | 6 |
| ⚪ P4 | 4.3 | Result caching | 1 day | Low | 7 |
| ⚪ P4 | 4.4 | Multi-environment | 3 days | Medium | 8 |

---

## 🎯 Milestones

| Milestone | Target Date | Criteria |
|-----------|-------------|----------|
| **M1: Automated Daily Runs** | End of Week 2 | Tests run daily via Cloud Scheduler, Slack alerts on failure |
| **M2: Event-Driven Testing** | End of Week 3 | Tests trigger on data updates via Pub/Sub |
| **M3: Full Test Coverage** | End of Week 5 | 20+ tests including all SCD2 validations |
| **M4: Production Ready** | End of Week 8 | Parallel execution, caching, multi-env support |

---

## 📈 Success Metrics

| Metric | Current | M1 Target | M2 Target | M4 Target |
|--------|---------|-----------|-----------|-----------|
| Automation Level | Manual | Daily scheduled | Event-driven | Full auto |
| Alert Latency | None | <5 min | <2 min | <1 min |
| Predefined Tests | 8 | 10 | 15 | 25+ |
| Execution Mode | UI only | API + UI | Pub/Sub + API | All modes |
| Avg. Execution Time | N/A | <60s | <45s | <30s |

---

## 🔧 Technical Requirements

### Infrastructure
- [ ] Cloud Scheduler enabled
- [ ] Pub/Sub topic for BQ notifications
- [ ] Service account with proper IAM roles
- [ ] Slack webhook configured

### Code Quality
- [ ] Unit tests for all new endpoints
- [ ] Integration tests for scheduler
- [ ] Error handling for all failure modes

### Monitoring
- [ ] Cloud Logging for all executions
- [ ] Alerting on service failures
- [ ] Execution metrics dashboard

---

## 🚀 Quick Start: Week 1 Sprint

**Day 1-2: Headless API**
1. Add `/api/run-scheduled-tests` endpoint
2. Create `scripts/run_tests.py` CLI
3. Add exit codes for CI/CD

**Day 3-4: Cloud Scheduler**
1. Create scheduler job
2. Set up service account
3. Test daily trigger

**Day 5: Slack Alerting**
1. Create NotificationService
2. Integrate with test executor
3. Test failure alerts

**End of Week 1 Goal:** Tests run automatically every day at 6 AM with Slack alerts on failure.

---

## 📝 Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2024-12-19 | 0.2.0 | Roadmap rewritten with automation focus |
| | | Added Phase 1: Automation Foundation |
| | | Added Phase 2: Alerting & Monitoring |
| | | Reorganized phases for daily execution priority |
| 2024-12-19 | 0.1.0 | Initial roadmap created |



