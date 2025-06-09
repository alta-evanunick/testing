# PestRoutes to Snowflake Data Pipeline - Process Outline

## Overview
End-to-end pipeline for extracting data from PestRoutes API and loading it into Snowflake with a three-layer architecture (RAW → STAGING → PRODUCTION).

## Current Implementation Status

### 1. API Data Extraction Layer (`pestroutes_api.py`)

#### 1.1 Authentication
- API credentials stored as constants
- Headers: `authenticationToken` and `authenticationKey`
- Base URL: `https://alta.pestroutes.com/api`

#### 1.2 Search Functionality (`search_disbursement_items`)
- **Dual Date Field Search**: Both `dateUpdated` and `dateCreated`
- **Pagination Handling**: 
  - 50,000 record limit per API call
  - Uses primary key (`gatewayDisbursementEntryID`) with `>` operator
  - Continues until result set < 50,000
- **Returns**: Combined, deduplicated list of IDs

#### 1.3 Get Functionality (`get_disbursement_items`)
- **Batch Processing**: 1,000 IDs maximum per request
- **Request Method**: POST with JSON body (avoiding URL length limits)
- **For 17,377 records**: Processes in 18 batches (17×1000 + 1×377)

#### 1.4 Task/Flow Architecture
- `Task` base class for reusable operations
- `DisbursementItemTask` combines search + get operations
- `Flow` class orchestrates multiple tasks
- Optional Snowflake integration via configuration

### 2. Snowflake RAW Layer (`snowflake_integration.py`)

#### 2.1 Connection Management
- Account: AUEDKKB-TH88792
- Warehouse: ALTAPESTANALYTICS
- Database: RAW_DB_DEV
- Schema: FIELDROUTES

#### 2.2 Table Structure
```sql
DISBURSEMENTITEM_FACT (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_JSON VARIANT,
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR(255),
    BATCH_ID VARCHAR(255)
)
```

#### 2.3 Data Loading
- Batch inserts in chunks of 5,000 records
- Uses `INSERT INTO ... SELECT PARSE_JSON() FROM VALUES` pattern
- Tracks batch_id with timestamp and date range
- Commits after each chunk for reliability

### 3. Snowflake STAGING Layer (`snowflake_staging.py`)

#### 3.1 Schema Setup
- Database: STAGING_DB_DEV
- Schema: FIELDROUTES
- Auto-creates if not exists

#### 3.2 Transformation Logic
- **Flattens JSON** to structured columns:
  - IDs: `GATEWAY_DISBURSEMENT_ENTRY_ID` (PK), `GATEWAY_DISBURSEMENT_ID`, `GATEWAY_EVENT_ID`
  - Dates: `DATE_CREATED`, `DATE_UPDATED` (TIMESTAMP_NTZ)
  - Customer: `BILLING_FIRST_NAME`, `BILLING_LAST_NAME`
  - Amounts: `AMOUNT` (10,2), `ACTUAL_AMOUNT` (10,6)
  - Flags: `IS_FEE` (BOOLEAN from "0"/"1" string)
  - Descriptions: Various VARCHAR fields

#### 3.3 Deduplication Strategy
- MERGE statement with source/target pattern
- Uses `ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp DESC)`
- Updates existing records if newer version found
- Inserts new records not in target

### 4. Data Flow Summary

#### Single-Entity Flow (Original)
```
PestRoutes API
    ↓
[Search by dateUpdated + dateCreated]
    ↓
[Deduplicate IDs]
    ↓
[Batch GET requests (1000/batch)]
    ↓
RAW_DB_DEV.FIELDROUTES.DISBURSEMENTITEM_FACT
    ↓
[MERGE with deduplication]
    ↓
STAGING_DB_DEV.FIELDROUTES.DISBURSEMENTITEM_FACT
    ↓
[Future: PRODUCTION views]
```

#### Multi-Office Flow (Enhanced)
```
Office 1 API ─┐
Office 2 API ─┤
Office 3 API ─┤ [Global Entities: Process Once]
...          ─│ [Office-Specific: Process Each]
Office N API ─┘
    ↓
[Entity-Specific Date Field Searches]
    ↓
[Office-Aware ID Deduplication]
    ↓
[Batch GET with Office Identification]
    ↓
RAW_DB_DEV.FIELDROUTES.[ENTITY]_FACT
    ↓
[Multi-Office MERGE with Deduplication]
    ↓
STAGING_DB_DEV.FIELDROUTES.[ENTITY]_FACT
    ↓
[Future: PRODUCTION views]
```

## Key Design Decisions

1. **JSON Storage**: RAW layer preserves complete JSON for audit/reprocessing
2. **Batch Sizes**: 
   - API GET: 1,000 records (API limit)
   - Snowflake INSERT: 5,000 records (performance optimization)
3. **Deduplication**: At STAGING layer, not RAW (preserves history)
4. **Error Handling**: Graceful failures with transaction rollback
5. **Modularity**: Separate modules for each layer, reusable components

## Configuration Requirements

1. **Python Environment**:
   - `requests` library
   - `snowflake-connector-python`
   
2. **API Credentials**:
   - API Key
   - Authentication Token

3. **Snowflake Access**:
   - User credentials
   - Appropriate database/schema permissions
   - Active warehouse allocation

## Multi-Entity Support (IMPLEMENTED)

### 4. Enhanced Multi-Entity Architecture

#### 4.1 Entity Configuration (`multi_entity_api.py`)
- **Supported Entities**: `disbursementItem`, `customer`
- **Configuration-Driven**: Entity-specific date fields, ID fields, and table names
- **Date Field Mapping**:
  - `disbursementItem`: `dateUpdated`, `dateCreated`
  - `customer`: `dateUpdated`, `dateAdded`

#### 4.2 Unified API Client (`PestRoutesMultiEntityAPI`)
- **Generic Methods**: `search_entity()`, `get_entity_details()`
- **Entity Support**: Handles different response formats per entity
- **Batch Processing**: Consistent 1000-record batching across all entities

#### 4.3 Multi-Entity Flow (`MultiEntityFlow`)
- **Orchestration**: Single flow handles multiple entities
- **Sequential Processing**: Processes entities one at a time
- **Unified Results**: Combines results from all entities

#### 4.4 Enhanced Staging Layer (`multi_entity_staging.py`)
- **Customer Entity Schema**:
  ```sql
  CUSTOMER_FACT (
      CUSTOMER_ID NUMBER PRIMARY KEY,
      FNAME, LNAME, EMAIL, PHONE VARCHAR,
      BILLING_* fields for billing address,
      SERVICE_* fields for service address,
      DATE_ADDED, DATE_UPDATED, DATE_CANCELLED TIMESTAMP_NTZ,
      STATUS, IS_ACTIVE BOOLEAN,
      BALANCE, RESPONSIBLE_BALANCE NUMBER
  )
  ```
- **Generic Processing**: Same MERGE pattern for all entities
- **Entity-Specific Statistics**: Tailored metrics per entity type

#### 4.5 Complete Pipeline Test (`test_multi_entity_pipeline.py`)
- **End-to-End Testing**: API → RAW → STAGING for all entities
- **Quality Assurance**: Statistics and validation for each entity
- **Progress Reporting**: Real-time status updates during processing

## Multi-Office Support (IMPLEMENTED)

### 5. Multi-Office Architecture

#### 5.1 Office Management (`office_config.py`)
- **Credential Management**: Supports 17-18 office API key/token pairs
- **Environment Variables**: `PESTROUTES_OFFICE_N_API_KEY`, `PESTROUTES_OFFICE_N_TOKEN`
- **Office Identification**: Each office has unique ID and name
- **Prefect Integration**: Ready for Prefect secret blocks in production

#### 5.2 Entity Office Requirements
- **Global Entities**: `disbursementItem` - processed once with primary office
- **Office-Specific Entities**: `customer` - processed for each office separately
- **Configuration-Driven**: `requires_multi_office` flag determines processing

#### 5.3 Multi-Office API Client (`multi_office_api.py`)
- **Office-Aware Processing**: Automatically handles global vs. multi-office entities
- **Data Identification**: Adds `_office_id` and `_office_name` to each record
- **Batch Processing**: Office-specific batch IDs for audit trail
- **Error Isolation**: Office failures don't stop other offices

#### 5.4 Enhanced Data Pipeline
- **RAW Layer**: Office identification in batch_id and source_file
- **STAGING Layer**: Same tables, office data combined and deduplicated
- **Audit Trail**: Complete traceability of which office provided each record

#### 5.5 Environment Setup (`setup_environment.py`)
- **Local Testing**: .env file support for development
- **Production Ready**: Prefect secret block integration guide
- **Credential Validation**: Checks all office credentials before execution

#### 5.6 Complete Multi-Office Pipeline (`test_multi_office_pipeline.py`)
- **Intelligent Processing**: Global entities once, office-specific per office
- **Progress Reporting**: Office-level status and statistics
- **Unified Results**: Combined reporting across all offices

## Ready for Expansion

This architecture supports:
- **Additional entities** (follow ENTITY_CONFIG pattern in `multi_entity_api.py`)
- **Multiple date ranges/incremental loads**
- **Different transformation rules per entity** (via STAGING_CONFIG)
- **Parallel processing capabilities** (can be enhanced for concurrent execution)
- **Audit trail via RAW layer retention**
- **Unified monitoring and reporting** across all entities