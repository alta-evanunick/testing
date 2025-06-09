# PestRoutes to Snowflake Data Pipeline

🚀 **Production-ready ETL pipeline** processing 27 PestRoutes entities across 17 offices into Snowflake data warehouse with automated orchestration via Prefect Cloud.

## 🎯 Overview

This comprehensive data pipeline extracts, transforms, and loads PestRoutes business data through a robust three-layer architecture:

- **RAW Layer**: JSON storage preserving source data integrity
- **STAGING Layer**: Flattened, typed data with comprehensive transformations  
- **PRODUCTION Layer**: Business-ready analytics views (ready for future development)

### Key Features
✅ **27 Entities**: Complete PestRoutes data model coverage
✅ **17 Offices**: Multi-office architecture with separate credentials
✅ **Automated Scheduling**: Daily and incremental processing
✅ **Error Handling**: Robust retry logic and comprehensive logging
✅ **Data Quality**: Duplicate prevention and validation
✅ **Monitoring**: Real-time observability via Prefect Cloud

## 📊 Supported Entities

### FACT Tables (20)
High-volume transactional data:
- `customer`, `appointment`, `subscription`, `ticket`, `payment`
- `employee`, `note`, `task`, `door`, `knock`, `disbursement`
- `appliedPayment`, `ticketItem`, `paymentProfile`, `appointmentReminder`
- `chargeback`, `additionalContacts`, `genericFlagAssignment`

### DIM Tables (7) 
Reference and lookup data:
- `region`, `serviceType`, `genericFlag`, `product`
- `cancellationReason`, `reserviceReason`, `customerSource`

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PestRoutes    │    │   Snowflake     │    │    Prefect     │
│   17 Offices    │───▶│   Data Warehouse│◀───│   Orchestration │
│   27 Entities   │    │   3 Layers      │    │   Monitoring    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow
1. **Extraction**: Multi-office API calls with date-based filtering
2. **Raw Storage**: JSON preservation in Snowflake VARIANT columns
3. **Staging Transform**: Type conversion, validation, deduplication
4. **Scheduling**: Automated daily/incremental processing

## 🚀 Quick Start

### 1. Local Testing
```bash
# Install dependencies
pip install -r requirements.txt

# Test pipeline components
python test_prefect_pipeline.py quick
```

### 2. Deploy to Prefect Cloud
```bash
# Authenticate with Prefect Cloud
prefect cloud login

# Deploy all flows
python deploy.py
```

### 3. Start Processing
```bash
# Start worker
prefect worker start --pool default-agent-pool

# Run test deployment
prefect deployment run "pestroutes-single-entity/pestroutes-single-entity"
```

## 📁 Project Structure

```
pestroutes-pipeline/
├── flows/
│   └── main_pipeline.py           # Prefect flows and orchestration
├── office_config.py               # Entity configuration & credentials
├── multi_office_api.py            # API client with multi-office support
├── snowflake_integration.py       # RAW layer data loading
├── multi_entity_staging.py        # STAGING layer transformations (27 entities)
├── test_prefect_pipeline.py       # Local testing framework
├── deploy.py                      # Deployment automation
├── prefect.yaml                   # Prefect deployment configuration
├── requirements.txt               # Python dependencies
├── Dockerfile                     # Container configuration
└── docs/
    ├── PREFECT_DEPLOYMENT.md      # Deployment guide
    └── PROCESS_OUTLINE.md         # Technical documentation
```

## 🔐 Credentials Management

All credentials managed via **Prefect Secret Blocks**:

### Snowflake
- `snowflake-altapestanalytics` (SnowflakeCredentials)

### PestRoutes API (per office)
- `fieldroutes-[officename]-auth-key`
- `fieldroutes-[officename]-auth-token`

### GitHub Integration  
- `github-repo`, `github-evanunick`

## 📅 Deployment Options

### 1. Full Daily Pipeline
- **Schedule**: Daily at 2 AM UTC
- **Purpose**: Complete data refresh for all entities
- **Runtime**: ~60-90 minutes

### 2. Incremental Pipeline
- **Schedule**: Every 4 hours  
- **Purpose**: Near real-time updates
- **Runtime**: ~15-30 minutes

### 3. Single Entity Pipeline
- **Schedule**: On-demand
- **Purpose**: Testing and ad-hoc processing
- **Runtime**: ~5-15 minutes

## 📈 Monitoring & Observability

### Built-in Metrics
- **API Call Statistics**: Rate limiting and performance tracking
- **Data Volume Metrics**: Records processed per entity/office
- **Error Tracking**: Failed operations with detailed context
- **Performance Monitoring**: Execution times and resource usage

### Alerts & Notifications
- Flow failure notifications
- Data volume anomaly detection  
- API rate limit warnings
- Long-running job alerts

## 🛠️ Development

### Local Testing
```bash
# Test credential loading
python -c "
import asyncio
from flows.main_pipeline import load_credentials
asyncio.run(load_credentials())
"

# Test single entity
python test_prefect_pipeline.py quick

# Full test suite
python test_prefect_pipeline.py full
```

### Adding New Entities
1. Add entity configuration to `office_config.py`
2. Generate staging configuration via data sampling
3. Update `multi_entity_staging.py` with new transformations
4. Test locally before deployment

### Debugging
```bash
# View recent runs
prefect flow-run ls --limit 10

# Get detailed logs  
prefect flow-run logs <flow-run-id>

# Monitor deployments
prefect deployment ls
```

## 📞 Support & Documentation

- **[Deployment Guide](PREFECT_DEPLOYMENT.md)**: Complete setup instructions
- **[Process Documentation](PROCESS_OUTLINE.md)**: Technical deep-dive
- **Prefect Documentation**: https://docs.prefect.io
- **Issue Tracking**: GitHub Issues for pipeline-specific problems

## 🎉 Production Readiness

This pipeline is **production-ready** with:

✅ **Comprehensive Error Handling**: Retry logic, timeout management
✅ **Data Quality Assurance**: Deduplication, validation, audit trails  
✅ **Scalability**: Configurable batch sizes and parallel processing
✅ **Monitoring**: Real-time observability and alerting
✅ **Documentation**: Complete setup and troubleshooting guides
✅ **Testing Framework**: Local validation before deployment
✅ **Security**: Credential management via secret blocks

## 🔄 Data Quality Features

- **Deduplication**: ROW_NUMBER() window functions prevent duplicates
- **Timestamp Validation**: Invalid dates (0000-00-00) handled gracefully
- **Character Escaping**: JSON parsing errors prevented
- **Audit Trail**: Complete lineage tracking with load timestamps
- **Idempotency**: Safe to re-run without data corruption

---

**🚀 Ready to process your PestRoutes data at enterprise scale!**