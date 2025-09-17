# RAW → CLEAN Transform Plan

## General Conventions
- Drop extractor metadata columns that start with `_` (e.g., `_entity_type`, `_extraction_*`, `_load_timestamp`).
- Convert all table and column names to snake_case to align with the generated DDLs in `clean_schema_ddls.sql`.
- Cast numeric strings to the numeric types defined in the CLEAN DDL (e.g., `NUMBER`, `NUMBER(p,s)`, `FLOAT`).
- Cast timestamp/date strings to their target Snowflake temporal types (`TIMESTAMP_NTZ`, `DATE`, `TIME`).
- Translate sentinel strings to `NULL` unless business logic dictates otherwise:
  - Dates of `'0000-00-00'` or timestamps `'0000-00-00 00:00:00'` → `NULL`.
  - Empty strings in nullable fields → `NULL`.
  - `-1`, `0`, or similar sentinel IDs noted below → `NULL` plus optional companion flag when helpful.
- Persist raw JSON arrays/objects as `ARRAY`/`VARIANT` columns where defined, and create supporting child tables where flattening is required.

## Entity-Specific Rules

### applied_payment
- Cast ID, amount, and timestamp fields from strings to the defined numeric/timestamp types.
- Treat `applied_by = -1` as a system-applied record; store a boolean/system flag or map to `NULL` in `applied_by` with a companion `is_system_applied` flag.

### appointment
- Cast `date` to `DATE`, time window fields to `TIME`, and check/time-in/out columns to `TIMESTAMP_NTZ` (convert `'0000-00-00 00:00:00'` to `NULL`).
- Convert signature flags and other integer indicators (`signed_by_customer`, etc.) from string to integer/boolean.
- Translate `due_date = '0000-00-00'` to `NULL`.
- `additional_techs` may arrive as `NULL`; keep as nullable `VARCHAR`.
- Ensure latitude/longitude fields load as `FLOAT`, defaulting to `NULL` when blank or zero-degree placeholders are used for “not tracked”.

### cancellation_reason
- Cast all numeric strings to integers.
- Interpret `office_id = -1` as a global reason (optionally expose a boolean `is_global` in analytics outputs).

### customer
- Cast ID fields and monetary columns to their numeric targets.
- Translate sentinel strings (`'0000-00-00'`, `'0000-00-00 00:00:00'`, `'0'`, `'-1'`) to `NULL` for date/time and preference fields (`due_date`, `preferred_billing_date`, `payment_hold_date`, etc.).
- Preserve `a_pay` as text; optionally classify autopay method codes downstream.
- `unit_ids`, `appointment_ids`, `ticket_ids`, `payment_ids`, `subscription_ids` remain comma-delimited `VARCHAR`; create helper views if splitting is required.
- Latitude/longitude columns become `FLOAT`; blank or zeroed coordinates → `NULL`.

### customer_source
- Cast numeric strings to integers.
- Interpret `office_id = -1` as global.

### disbursement & disbursement_item
- Cast IDs to integers, amounts to `NUMBER(18,2)`/`NUMBER(18,6)`.
- Timestamps convert to `TIMESTAMP_NTZ`.

### employee
- Cast ID fields and numeric flags to integers.
- `skills` column stores the raw map as `VARIANT` in CLEAN.
- Create auxiliary table `clean.employee_skill` by flattening `skills` (map key → `skill_id`, value → `skill_description`).
- Treat `linked_employee_ids = '0'` and `roaming_rep = '0'` as `NULL`.

### generic_flag & generic_flag_assignment
- Cast numeric fields to integers; timestamps to `TIMESTAMP_NTZ`.
- No special sentinel handling beyond standard metadata stripping.

### note
- Cast ID fields to integers; `office_ids` stored as nullable `VARCHAR` (comma-separated).
- `contact_type_categories` remains text.
- `notes` holds HTML; optionally persist a plain-text derivative column using `REGEXP_REPLACE` for analytics.

### office
- Cast coordinates to `FLOAT`.
- Retain `caution_statements` HTML; consider a plain-text helper column.

### payment
- Cast IDs and monetary fields to numeric types.
- Preserve `invoice_ids` as comma-separated `VARCHAR`.
- Load `payment_applications` variant as stored; additionally load into `clean.payment_application` by flattening each array item (provides `applied_amount`, `tax_collected`, `ticket_id`, `date_applied`).
- Interpret `original_payment_id = NULL` when absent.

### route
- Cast average geo metrics to `FLOAT`.
- Convert new metrics (`capacity_estimate_value`, `distance_score`, `total_distance`, `estimated_*`) to the numeric types defined in the DDL.
- `additional_techs` remains nullable `VARCHAR`.
- Translate zero-distance/zero-duration values to `NULL` only if they represent “not calculated” (confirm with business).

### service_type
- Cast monetary columns to `NUMBER(18,2)`.
- Convert `season_start`/`season_end = '0000-00-00'` to `NULL`.
- Preserve sentinel frequency values (-1, -3, etc.) per FieldRoutes definitions.

### subscription
- Cast monetary fields and counts to numeric types per DDL.
- Translate `'0000-00-00'` and `'0000-00-00 00:00:00'` across date-related columns (`custom_date`, `seasonal_start/end`, `next_billing_date`, `expiration_date`, `lead_date_*`, etc.) to `NULL`.
- Treat `preferred_days` blank string as `NULL`; retain comma-separated codes when populated.
- Flatten `recurring_ticket` and `add_ons` into child tables (`clean.subscription_recurring_ticket`, `clean.subscription_add_on`) if granular reporting is required; otherwise store as `VARIANT`/`ARRAY` per DDL.
- Interpret `max_monthly_charge`, `capacity_estimate` numeric strings as `NUMBER(18,2)`.
- Map sentinel salesperson IDs (`0` or `-1`) to `NULL` when they indicate “unassigned”.

### ticket
- Cast numeric strings to numeric types; `tax_rate` to `NUMBER(8,6)`.
- Replace `production_value = -1` with `NULL` or compute fallback to subtotal in analytics.
- Populate `clean.ticket_item` child table by flattening the `items` array (one row per line item).

### task
- Rename primary key to align with RAW (`task_ids`) during staging; optionally alias to `task_id` in downstream views.
- Cast `due_date`, `date_added`, `date_completed`, `date_updated` to `TIMESTAMP_NTZ`.
- Convert nullable phone and notes fields from blanks to `NULL`.

### door, knock, generic_flag (no sample provided)
- Apply standard casting rules: convert numeric strings to numerics, timestamps/dates to temporal types, `'0000-00-00'` to `NULL`, drop underscore-prefixed metadata.

## Additional Outputs
- `clean_schema_ddls.sql` contains `CREATE OR REPLACE TABLE` statements for all CLEAN tables under the `clean` schema.
- Child tables to generate during the transform process:
  - `clean.employee_skill` from `employee.skills` map.
  - `clean.payment_application` from `payment.payment_applications` array.
  - `clean.ticket_item` from `ticket.items` array.
  - Optional `clean.subscription_recurring_ticket`, `clean.subscription_add_on` if deeper subscription analysis is required.

## Implementation Notes
- Implement transforms as Snowflake SQL merge scripts or Python (e.g., dbt) jobs that:
  1. Stage RAW data with column renaming/casting.
  2. Handle sentinel replacements using `NULLIF`/`TRY_TO_DATE`/`TRY_TO_TIMESTAMP_NTZ`.
  3. MERGE into CLEAN tables using natural keys (e.g., `ticket_id`, `subscription_id`).
  4. Refresh child tables by re-flattening arrays and maps per load cycle.
