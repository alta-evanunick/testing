# Prefect Deployment Guide - PestRoutes Pipeline

Complete deployment guide for the PestRoutes to Snowflake data pipeline using Prefect Cloud with secret blocks.

## 🎯 Overview

This pipeline processes 27 PestRoutes entities across 17 offices, transforming data through RAW and STAGING layers in Snowflake with full automation and monitoring.

## 📋 Prerequisites

1. **Prefect Cloud Account**: https://app.prefect.cloud
2. **Secret Blocks Created**: All API credentials stored as Prefect secrets
3. **Snowflake Access**: Warehouse access with CREATE privileges
4. **Docker**: For containerized deployments
5. **GitHub Repository**: For code storage and CI/CD

## 🔐 Secret Blocks Setup

### Required Secret Blocks (Already Created)

#### Snowflake
- `snowflake-altapestanalytics` (SnowflakeCredentials block)

#### GitHub  
- `github-repo` (GitHub repository)
- `github-evanunick` (GitHub credentials)

#### PestRoutes API (Per Office)
Format: `fieldroutes-[officename]-auth-key` and `fieldroutes-[officename]-auth-token`

Examples:
- `fieldroutes-virginiabeach-auth-key`
- `fieldroutes-virginiabeach-auth-token`
- `fieldroutes-spokane-auth-key`  
- `fieldroutes-spokane-auth-token`
- (etc. for all 17 offices)

## 🚀 Quick Deployment

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Authenticate with Prefect Cloud
```bash
prefect cloud login
```

### 3. Test Locally First
```bash
# Quick test with single entity
python test_prefect_pipeline.py quick

# Full comprehensive test
python test_prefect_pipeline.py full
```

### 4. Deploy to Prefect Cloud
```bash
# Automated deployment script
python deploy.py

# OR manual deployment
prefect deploy --all
```

### 5. Start Worker
```bash
prefect worker start --pool default-agent-pool
```

## 📊 Available Deployments

### 1. Full Daily Pipeline
- **Name**: `pestroutes-full-pipeline`
- **Schedule**: Daily at 2 AM UTC
- **Purpose**: Process all 27 entities for all offices
- **Parameters**:
  - `start_date`: Data extraction start date
  - `end_date`: Data extraction end date  
  - `batch_size`: Records per batch (default: 5000)
  - `entities_to_process`: Specific entities (default: all)
  - `offices_to_process`: Specific offices (default: all)
  - `run_staging`: Enable staging transformations (default: true)

### 2. Incremental Pipeline  
- **Name**: `pestroutes-incremental`
- **Schedule**: Every 4 hours
- **Purpose**: Process recent updates for near real-time data
- **Parameters**:
  - `hours_lookback`: Hours to look back (default: 6)
  - `batch_size`: Records per batch (default: 1000)

### 3. Single Entity Pipeline
- **Name**: `pestroutes-single-entity`
- **Schedule**: Manual/ad-hoc
- **Purpose**: Test or process specific entities
- **Parameters**:
  - `entity`: Entity name (e.g., "customer")
  - `start_date`: Start date
  - `end_date`: End date
  - `office_ids`: Specific offices (default: all applicable)

## 🏗️ Architecture Components

### Flow Structure
```
flows/
└── main_pipeline.py          # Main Prefect flows
    ├── pestroutes_full_pipeline()
    ├── pestroutes_incremental_pipeline()
    ├── pestroutes_single_entity_pipeline()
    └── Supporting tasks
```

### Core Modules
- `office_config.py` - Entity configuration & office mappings
- `multi_office_api.py` - API client with multi-office support
- `snowflake_integration.py` - RAW layer data loading
- `multi_entity_staging.py` - STAGING layer transformations

## 💻 Local Testing

### Test Individual Components
```bash
# Test credential loading
python -c "
import asyncio
from flows.main_pipeline import load_credentials
print(asyncio.run(load_credentials()))
"

# Test single entity locally
python test_prefect_pipeline.py quick
```

### Test Complete Pipeline
```bash
# Run full test suite
python test_prefect_pipeline.py full
```

### Manual Flow Execution
```python
import asyncio
from flows.main_pipeline import pestroutes_single_entity_pipeline

result = asyncio.run(pestroutes_single_entity_pipeline(
    entity="customer",
    start_date="2025-06-08", 
    end_date="2025-06-09",
    run_staging=True
))
```

## 🎮 Running Deployments

### Via Prefect CLI
```bash
# Run full pipeline
prefect deployment run "pestroutes-full-pipeline/pestroutes-full-pipeline"

# Run incremental pipeline  
prefect deployment run "pestroutes-incremental/pestroutes-incremental"

# Run single entity with parameters
prefect deployment run "pestroutes-single-entity-pipeline/pestroutes-single-entity" \
  --param entity=customer \
  --param start_date=2025-06-08 \
  --param end_date=2025-06-09
```

### Via Prefect UI
1. Go to https://app.prefect.cloud
2. Navigate to Deployments
3. Click "Run" on desired deployment
4. Set parameters if needed
5. Monitor execution in real-time

## 📈 Monitoring & Observability

### Built-in Monitoring
- **Flow Run Status**: Success/failure tracking
- **Task-level Logging**: Detailed operation logs  
- **Performance Metrics**: Execution time, record counts
- **Error Tracking**: Automatic error capture and reporting

### Key Metrics Tracked
- Records processed per entity
- API call statistics
- Office-level success/failure rates
- Staging transformation results
- Data quality validation results

### Alerts & Notifications
Configure in Prefect UI:
- Failed flow notifications
- Long-running job alerts
- Data volume anomaly detection

## 🔧 Configuration

### Resource Requirements
- **Memory**: 4-8 GB (depends on data volume)
- **CPU**: 2-4 cores 
- **Timeout**: 60-120 minutes
- **Storage**: 1-2 GB for temporary files

### Environment Variables
Set in your deployment environment:
```bash
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/[account]/workspaces/[workspace]
PREFECT_API_KEY=your_api_key
```

### Scaling Configuration
```yaml
# In prefect.yaml
work_pool:
  name: default-agent-pool
  concurrency: 10  # Adjust based on load
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Authentication Errors
```bash
# Verify secret blocks
prefect block ls | grep fieldroutes
prefect block ls | grep snowflake

# Test credential loading
python -c "
import asyncio
from flows.main_pipeline import load_credentials
asyncio.run(load_credentials())
"
```

#### 2. API Rate Limiting
- Pipeline includes automatic retry logic
- Batch sizes are optimized for API limits
- Monitor API call statistics in logs

#### 3. Snowflake Connection Issues
```bash
# Test Snowflake connectivity
python -c "
from snowflake_integration import SnowflakeConnector
conn = SnowflakeConnector('account', 'user', 'password', 'warehouse')
print('Connected:', conn.connect())
"
```

#### 4. Memory Issues
- Reduce batch_size parameter
- Process fewer entities per run
- Increase worker memory allocation

### Debugging Commands
```bash
# View recent flow runs
prefect flow-run ls --limit 10

# Get detailed logs
prefect flow-run logs <flow-run-id>

# Check worker status
prefect worker status

# View deployments
prefect deployment ls
```

## 📞 Support Resources

- **Prefect Docs**: https://docs.prefect.io
- **Prefect Community**: https://prefect.io/slack  
- **Pipeline Documentation**: See PROCESS_OUTLINE.md
- **GitHub Issues**: For pipeline-specific issues

## 🎉 Success Criteria

Your deployment is successful when:
✅ All secret blocks load correctly
✅ Local tests pass completely  
✅ Deployments create without errors
✅ Workers start and remain healthy
✅ Test runs complete successfully
✅ Data appears in Snowflake RAW and STAGING layers
✅ Monitoring shows expected metrics