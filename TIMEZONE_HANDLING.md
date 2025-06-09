# Timezone Handling in PestRoutes Pipeline

## Overview

All PestRoutes timestamps are stored in **Pacific Time (America/Los_Angeles)**, which includes automatic daylight saving time adjustments. The pipeline handles this by:

1. **API Layer**: Accepting date filters as-is (Pacific time interpretation)
2. **Staging Layer**: Converting all timestamps from Pacific to UTC for storage
3. **Snowflake Storage**: Storing timestamps in UTC for consistency

## Implementation Details

### 1. API Date Filtering
When querying the PestRoutes API with date ranges:
- Date strings (YYYY-MM-DD) are interpreted by PestRoutes as Pacific time
- No conversion needed at the API level
- Example: "2025-06-09" means midnight Pacific time on that date

### 2. Timestamp Conversion in Staging
The staging layer includes a custom Snowflake UDF to handle timezone conversion:

```sql
CREATE FUNCTION STAGING_DB_DEV.FIELDROUTES.PACIFIC_TO_UTC(pacific_time_str STRING)
RETURNS TIMESTAMP_NTZ
AS
$$
CASE 
    WHEN pacific_time_str IS NULL OR pacific_time_str = '' OR pacific_time_str = '0000-00-00 00:00:00' THEN NULL
    ELSE 
        CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TRY_TO_TIMESTAMP_NTZ(pacific_time_str))
END
$$;
```

This function:
- Handles null/empty values gracefully
- Converts Pacific time strings to UTC timestamps
- Accounts for daylight saving time automatically

### 3. Updated Entities

Key timestamp fields have been updated to use Pacific to UTC conversion:

#### Customer Entity
- `DATE_ADDED`: Pacific → UTC
- `DATE_UPDATED`: Pacific → UTC  
- `DATE_CANCELLED`: Pacific → UTC

#### Appointment Entity
- `TIME_IN`: Pacific → UTC
- `TIME_OUT`: Pacific → UTC
- `CHECK_IN`: Pacific → UTC
- `CHECK_OUT`: Pacific → UTC

#### Disbursement Item Entity
- `DATE_CREATED`: Pacific → UTC
- `DATE_UPDATED`: Pacific → UTC

### 4. Schedule Considerations

The Prefect deployment schedule is set to **America/Denver** time:
- Daily pipeline: 2 AM Denver time
- Incremental pipeline: Every 4 hours

This ensures consistent scheduling regardless of daylight saving time changes.

## Best Practices

1. **Always store timestamps in UTC** in the data warehouse
2. **Document timezone assumptions** in queries and reports
3. **Use timezone-aware functions** when displaying data to end users
4. **Test edge cases** around daylight saving time transitions

## Example Queries

### Converting UTC back to Pacific for reporting:
```sql
SELECT 
    CUSTOMER_ID,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', DATE_ADDED) as DATE_ADDED_PACIFIC,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', DATE_UPDATED) as DATE_UPDATED_PACIFIC
FROM STAGING_DB_DEV.FIELDROUTES.CUSTOMER_FACT;
```

### Filtering by Pacific time date range:
```sql
-- Find customers added yesterday (Pacific time)
SELECT * 
FROM STAGING_DB_DEV.FIELDROUTES.CUSTOMER_FACT
WHERE DATE_ADDED >= CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', '2025-06-08 00:00:00')
  AND DATE_ADDED < CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', '2025-06-09 00:00:00');
```

## Notes

- The `TRY_TO_TIMESTAMP_NTZ` function is used to handle invalid date formats gracefully
- PestRoutes sometimes returns '0000-00-00 00:00:00' for null dates, which are converted to NULL
- All staging table timestamps are stored as `TIMESTAMP_NTZ` (no timezone) in UTC