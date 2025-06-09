"""
Multi-Entity Snowflake Staging Layer Module
Handles transformation of raw JSON data to structured staging tables for multiple entities
"""
from typing import Dict, Any, Optional
from datetime import datetime
from snowflake_integration import SnowflakeConnector


# Staging table configurations for each entity
STAGING_CONFIG = {
    "disbursementItem": {
        "table_name": "DISBURSEMENTITEM_FACT",
        "raw_table": "DISBURSEMENTITEM_FACT",
        "primary_key": "GATEWAY_DISBURSEMENT_ENTRY_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "GATEWAY_DISBURSEMENT_ENTRY_ID": "RAW_JSON:gatewayDisbursementEntryID::NUMBER",
            "GATEWAY_DISBURSEMENT_ID": "RAW_JSON:gatewayDisbursementID::NUMBER",
            "GATEWAY_EVENT_ID": "RAW_JSON:gatewayEventID::NUMBER",
            "DATE_CREATED": "TO_TIMESTAMP_NTZ(RAW_JSON:dateCreated::STRING)",
            "DATE_UPDATED": "TO_TIMESTAMP_NTZ(RAW_JSON:dateUpdated::STRING)",
            "BILLING_FIRST_NAME": "RAW_JSON:billingFirstName::STRING",
            "BILLING_LAST_NAME": "RAW_JSON:billingLastName::STRING",
            "AMOUNT": "RAW_JSON:amount::NUMBER(10,2)",
            "ACTUAL_AMOUNT": "RAW_JSON:actualAmount::NUMBER(10,6)",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "IS_FEE": "CASE WHEN RAW_JSON:isFee::STRING = '1' THEN TRUE ELSE FALSE END",
            "GATEWAY_EVENT_TYPE": "RAW_JSON:gatewayEventType::STRING",
            "GATEWAY_EVENT_FEE_TYPE": "RAW_JSON:gatewayEventFeeType::STRING",
            "GATEWAY_EVENT_DESCRIPTION": "RAW_JSON:gatewayEventDescription::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.DISBURSEMENTITEM_FACT (
            -- Primary Key
            GATEWAY_DISBURSEMENT_ENTRY_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            GATEWAY_DISBURSEMENT_ID NUMBER,
            GATEWAY_EVENT_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Customer Information
            BILLING_FIRST_NAME VARCHAR(255),
            BILLING_LAST_NAME VARCHAR(255),
            
            -- Amounts
            AMOUNT NUMBER(10,2),
            ACTUAL_AMOUNT NUMBER(10,6),
            
            -- Descriptive Fields
            DESCRIPTION VARCHAR(1000),
            IS_FEE BOOLEAN,
            GATEWAY_EVENT_TYPE VARCHAR(255),
            GATEWAY_EVENT_FEE_TYPE VARCHAR(255),
            GATEWAY_EVENT_DESCRIPTION VARCHAR(1000),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "customer": {
        "table_name": "CUSTOMER_FACT",
        "raw_table": "CUSTOMER_FACT", 
        "primary_key": "CUSTOMER_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "FNAME": "RAW_JSON:fname::STRING",
            "LNAME": "RAW_JSON:lname::STRING",
            "EMAIL": "RAW_JSON:email::STRING",
            "PHONE": "RAW_JSON:phone::STRING",
            "BILLING_COMPANY_NAME": "RAW_JSON:billingCompanyName::STRING",
            "BILLING_FNAME": "RAW_JSON:billingFname::STRING",
            "BILLING_LNAME": "RAW_JSON:billingLname::STRING",
            "BILLING_ADDRESS": "RAW_JSON:billingAddress::STRING",
            "BILLING_CITY": "RAW_JSON:billingCity::STRING",
            "BILLING_STATE": "RAW_JSON:billingState::STRING",
            "BILLING_ZIP": "RAW_JSON:billingZip::STRING",
            "SERVICE_ADDRESS": "RAW_JSON:serviceAddress::STRING",
            "SERVICE_CITY": "RAW_JSON:serviceCity::STRING",
            "SERVICE_STATE": "RAW_JSON:serviceState::STRING",
            "SERVICE_ZIP": "RAW_JSON:serviceZip::STRING",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "DATE_CANCELLED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCancelled::STRING, '0000-00-00 00:00:00'))",
            "STATUS": "RAW_JSON:status::STRING",
            "CUSTOMER_LINK": "RAW_JSON:customerLink::STRING",
            "BALANCE": "RAW_JSON:balance::NUMBER(10,2)",
            "RESPONSIBLE_BALANCE": "RAW_JSON:responsibleBalance::NUMBER(10,2)",
            "IS_ACTIVE": "CASE WHEN RAW_JSON:status::STRING = '1' THEN TRUE ELSE FALSE END"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.CUSTOMER_FACT (
            -- Primary Key
            CUSTOMER_ID NUMBER PRIMARY KEY,
            
            -- Personal Information
            FNAME VARCHAR(255),
            LNAME VARCHAR(255),
            EMAIL VARCHAR(500),
            PHONE VARCHAR(50),
            
            -- Billing Information
            BILLING_COMPANY_NAME VARCHAR(255),
            BILLING_FNAME VARCHAR(255),
            BILLING_LNAME VARCHAR(255),
            BILLING_ADDRESS VARCHAR(500),
            BILLING_CITY VARCHAR(255),
            BILLING_STATE VARCHAR(10),
            BILLING_ZIP VARCHAR(20),
            
            -- Service Address
            SERVICE_ADDRESS VARCHAR(500),
            SERVICE_CITY VARCHAR(255),
            SERVICE_STATE VARCHAR(10),
            SERVICE_ZIP VARCHAR(20),
            
            -- Dates
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            DATE_CANCELLED TIMESTAMP_NTZ,
            
            -- Status and Links
            STATUS VARCHAR(50),
            CUSTOMER_LINK VARCHAR(500),
            IS_ACTIVE BOOLEAN,
            
            -- Financial
            BALANCE NUMBER(10,2),
            RESPONSIBLE_BALANCE NUMBER(10,2),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "appointment": {
        "table_name": "APPOINTMENT_FACT",
        "raw_table": "APPOINTMENT_FACT", 
        "primary_key": "APPOINTMENT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "APPOINTMENT_ID": "RAW_JSON:appointmentID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "SUBSCRIPTION_ID": "RAW_JSON:subscriptionID::NUMBER",
            "SUBSCRIPTION_REGION_ID": "RAW_JSON:subscriptionRegionID::NUMBER",
            "ROUTE_ID": "RAW_JSON:routeID::NUMBER",
            "SPOT_ID": "RAW_JSON:spotID::NUMBER",
            "APPOINTMENT_DATE": "TRY_TO_DATE(RAW_JSON:date::STRING)",
            "START_TIME": "RAW_JSON:start::STRING",
            "END_TIME": "RAW_JSON:end::STRING",
            "TIME_WINDOW": "RAW_JSON:timeWindow::STRING",
            "DURATION": "RAW_JSON:duration::NUMBER",
            "APPOINTMENT_TYPE": "RAW_JSON:type::STRING",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "DATE_COMPLETED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCompleted::STRING, '0000-00-00 00:00:00'))",
            "DATE_CANCELLED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCancelled::STRING, '0000-00-00 00:00:00'))",
            "DUE_DATE": "TRY_TO_DATE(RAW_JSON:dueDate::STRING)",
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "ASSIGNED_TECH": "RAW_JSON:assignedTech::STRING",
            "COMPLETED_BY": "RAW_JSON:completedBy::STRING",
            "SERVICED_BY": "RAW_JSON:servicedBy::STRING",
            "CANCELLED_BY": "RAW_JSON:cancelledBy::STRING",
            "STATUS": "RAW_JSON:status::STRING",
            "STATUS_TEXT": "RAW_JSON:statusText::STRING",
            "CALL_AHEAD": "CASE WHEN RAW_JSON:callAhead::STRING = '1' THEN TRUE ELSE FALSE END",
            "IS_INITIAL": "CASE WHEN RAW_JSON:isInitial::STRING = '1' THEN TRUE ELSE FALSE END",
            "SIGNED_BY_CUSTOMER": "CASE WHEN RAW_JSON:signedByCustomer::STRING = '1' THEN TRUE ELSE FALSE END",
            "SIGNED_BY_TECH": "CASE WHEN RAW_JSON:signedByTech::STRING = '1' THEN TRUE ELSE FALSE END",
            "SERVICED_INTERIOR": "CASE WHEN RAW_JSON:servicedInterior::STRING = '1' THEN TRUE ELSE FALSE END",
            "DO_INTERIOR": "CASE WHEN RAW_JSON:doInterior::STRING = '1' THEN TRUE ELSE FALSE END",
            "NOTES": "RAW_JSON:notes::STRING",
            "OFFICE_NOTES": "RAW_JSON:officeNotes::STRING",
            "APPOINTMENT_NOTES": "RAW_JSON:appointmentNotes::STRING",
            "TIME_IN": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:timeIn::STRING, '0000-00-00 00:00:00'))",
            "TIME_OUT": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:timeOut::STRING, '0000-00-00 00:00:00'))",
            "CHECK_IN": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:checkIn::STRING, '0000-00-00 00:00:00'))",
            "CHECK_OUT": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:checkOut::STRING, '0000-00-00 00:00:00'))",
            "WIND_SPEED": "RAW_JSON:windSpeed::NUMBER",
            "WIND_DIRECTION": "RAW_JSON:windDirection::STRING",
            "TEMPERATURE": "RAW_JSON:temperature::NUMBER(5,2)",
            "AMOUNT_COLLECTED": "RAW_JSON:amountCollected::NUMBER(10,2)",
            "PAYMENT_METHOD": "RAW_JSON:paymentMethod::STRING",
            "TICKET_ID": "RAW_JSON:ticketID::NUMBER",
            "CANCELLATION_REASON_ID": "RAW_JSON:cancellationReasonID::NUMBER",
            "RESCHEDULE_REASON_ID": "RAW_JSON:rescheduleReasonID::NUMBER",
            "RESERVICE_REASON_ID": "RAW_JSON:reserviceReasonID::NUMBER",
            "LAT_IN": "RAW_JSON:latIn::NUMBER(10,7)",
            "LAT_OUT": "RAW_JSON:latOut::NUMBER(10,7)",
            "LONG_IN": "RAW_JSON:longIn::NUMBER(10,7)",
            "LONG_OUT": "RAW_JSON:longOut::NUMBER(10,7)",
            "SEQUENCE": "RAW_JSON:sequence::NUMBER",
            "LOCKED_BY": "RAW_JSON:lockedBy::STRING",
            "ORIGINAL_APPOINTMENT_ID": "RAW_JSON:originalAppointmentID::NUMBER",
            "PRODUCTION_VALUE": "RAW_JSON:productionValue::NUMBER(10,2)",
            "GROUP_ID": "RAW_JSON:groupID::NUMBER",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.APPOINTMENT_FACT (
            -- Primary Key
            APPOINTMENT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            CUSTOMER_ID NUMBER,
            SUBSCRIPTION_ID NUMBER,
            SUBSCRIPTION_REGION_ID NUMBER,
            ROUTE_ID NUMBER,
            SPOT_ID NUMBER,
            EMPLOYEE_ID NUMBER,
            TICKET_ID NUMBER,
            CANCELLATION_REASON_ID NUMBER,
            RESCHEDULE_REASON_ID NUMBER,
            RESERVICE_REASON_ID NUMBER,
            ORIGINAL_APPOINTMENT_ID NUMBER,
            GROUP_ID NUMBER,
            
            -- Dates and Times
            APPOINTMENT_DATE DATE,
            START_TIME VARCHAR(10),
            END_TIME VARCHAR(10),
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            DATE_COMPLETED TIMESTAMP_NTZ,
            DATE_CANCELLED TIMESTAMP_NTZ,
            DUE_DATE DATE,
            TIME_IN TIMESTAMP_NTZ,
            TIME_OUT TIMESTAMP_NTZ,
            CHECK_IN TIMESTAMP_NTZ,
            CHECK_OUT TIMESTAMP_NTZ,
            
            -- Appointment Details
            TIME_WINDOW VARCHAR(10),
            DURATION NUMBER,
            APPOINTMENT_TYPE VARCHAR(50),
            STATUS VARCHAR(50),
            STATUS_TEXT VARCHAR(100),
            
            -- Personnel
            ASSIGNED_TECH VARCHAR(50),
            COMPLETED_BY VARCHAR(50),
            SERVICED_BY VARCHAR(50),
            CANCELLED_BY VARCHAR(50),
            
            -- Boolean Flags
            CALL_AHEAD BOOLEAN,
            IS_INITIAL BOOLEAN,
            SIGNED_BY_CUSTOMER BOOLEAN,
            SIGNED_BY_TECH BOOLEAN,
            SERVICED_INTERIOR BOOLEAN,
            DO_INTERIOR BOOLEAN,
            
            -- Text Fields
            NOTES VARCHAR(5000),
            OFFICE_NOTES VARCHAR(2000),
            APPOINTMENT_NOTES VARCHAR(2000),
            
            -- Environmental Data
            WIND_SPEED NUMBER,
            WIND_DIRECTION VARCHAR(10),
            TEMPERATURE NUMBER(5,2),
            
            -- Financial
            AMOUNT_COLLECTED NUMBER(10,2),
            PAYMENT_METHOD VARCHAR(50),
            PRODUCTION_VALUE NUMBER(10,2),
            
            -- Location Data
            LAT_IN NUMBER(10,7),
            LAT_OUT NUMBER(10,7),
            LONG_IN NUMBER(10,7),
            LONG_OUT NUMBER(10,7),
            
            -- Scheduling
            SEQUENCE NUMBER,
            LOCKED_BY VARCHAR(50),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "route": {
        "table_name": "ROUTE_FACT",
        "raw_table": "ROUTE_FACT", 
        "primary_key": "ROUTE_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "ROUTE_ID": "RAW_JSON:routeID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "TITLE": "RAW_JSON:title::STRING",
            "TEMPLATE_ID": "RAW_JSON:templateID::NUMBER",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "ADDED_BY": "RAW_JSON:addedBy::STRING",
            "GROUP_ID": "RAW_JSON:groupID::NUMBER",
            "GROUP_TITLE": "RAW_JSON:groupTitle::STRING",
            "ROUTE_DATE": "TRY_TO_DATE(RAW_JSON:date::STRING)",
            "DAY_NOTES": "RAW_JSON:dayNotes::STRING",
            "DAY_ALERT": "RAW_JSON:dayAlert::STRING",
            "DAY_ID": "RAW_JSON:dayID::NUMBER",
            "ASSIGNED_TECH": "RAW_JSON:assignedTech::STRING",
            "API_CAN_SCHEDULE": "CASE WHEN RAW_JSON:apiCanSchedule::STRING = '1' THEN TRUE ELSE FALSE END",
            "AVERAGE_LATITUDE": "RAW_JSON:averageLatitude::NUMBER(10,7)",
            "AVERAGE_LONGITUDE": "RAW_JSON:averageLongitude::NUMBER(10,7)",
            "AVERAGE_DISTANCE": "RAW_JSON:averageDistance::NUMBER(10,2)",
            "DISTANCE_SCORE": "RAW_JSON:distanceScore::NUMBER",
            "ESTIMATED_APPOINTMENTS_DURATION_MINUTES": "RAW_JSON:estimatedAppointmentsDurationMinutes::NUMBER",
            "ESTIMATED_DRIVING_DURATION_SECONDS": "RAW_JSON:estimatedDrivingDurationSeconds::NUMBER",
            "CAPACITY_ESTIMATE_VALUE": "RAW_JSON:capacityEstimateValue::NUMBER(10,2)",
            "LOCKED_ROUTE": "CASE WHEN RAW_JSON:lockedRoute::STRING = '1' THEN TRUE ELSE FALSE END",
            "TOTAL_DISTANCE": "RAW_JSON:totalDistance::NUMBER(10,2)",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.ROUTE_FACT (
            -- Primary Key
            ROUTE_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            TEMPLATE_ID NUMBER,
            GROUP_ID NUMBER,
            DAY_ID NUMBER,
            
            -- Basic Info
            TITLE VARCHAR(255),
            ROUTE_DATE DATE,
            GROUP_TITLE VARCHAR(100),
            ADDED_BY VARCHAR(50),
            ASSIGNED_TECH VARCHAR(50),
            
            -- Dates
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Notes and Alerts
            DAY_NOTES VARCHAR(2000),
            DAY_ALERT VARCHAR(2000),
            
            -- Scheduling
            API_CAN_SCHEDULE BOOLEAN,
            LOCKED_ROUTE BOOLEAN,
            
            -- Geographic Data
            AVERAGE_LATITUDE NUMBER(10,7),
            AVERAGE_LONGITUDE NUMBER(10,7),
            AVERAGE_DISTANCE NUMBER(10,2),
            TOTAL_DISTANCE NUMBER(10,2),
            
            -- Performance Metrics
            DISTANCE_SCORE NUMBER,
            ESTIMATED_APPOINTMENTS_DURATION_MINUTES NUMBER,
            ESTIMATED_DRIVING_DURATION_SECONDS NUMBER,
            CAPACITY_ESTIMATE_VALUE NUMBER(10,2),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "region": {
        "table_name": "REGION_DIM",
        "raw_table": "REGION_DIM",
        "primary_key": "REGION_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "REGION_ID": "RAW_JSON:regionID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:created::STRING, '0000-00-00 00:00:00'))",
            "DELETED": "RAW_JSON:deleted::STRING",
            "POINTS": "RAW_JSON:points::STRING",
            "TYPE": "RAW_JSON:type::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.REGION_DIM (
            -- Primary Key
            REGION_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            
            -- Dates
            CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Text Fields
            DESCRIPTION VARCHAR(255),
            DELETED VARCHAR(255),
            POINTS VARCHAR(255),
            TYPE VARCHAR(255),
            
            -- Boolean Fields
            ACTIVE BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "serviceType": {
        "table_name": "SERVICETYPE_DIM",
        "raw_table": "SERVICETYPE_DIM",
        "primary_key": "SERVICETYPE_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "SERVICETYPE_ID": "RAW_JSON:serviceTypeID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "TYPE": "RAW_JSON:type::STRING",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "CATEGORY": "RAW_JSON:category::STRING",
            "DEFAULT_CHARGE": "RAW_JSON:defaultCharge::NUMBER(10,2)",
            "SHOW_ON_AGREEMENT": "CASE WHEN RAW_JSON:showOnAgreement::STRING = '1' THEN TRUE ELSE FALSE END",
            "SERVICE_TIME_WINDOW": "RAW_JSON:serviceTimeWindow::STRING",
            "ESTIMATED_DURATION_MINUTES": "RAW_JSON:estimatedDurationMinutes::NUMBER",
            "IS_RECURRING": "CASE WHEN RAW_JSON:isRecurring::STRING = '1' THEN TRUE ELSE FALSE END",
            "IS_INITIAL": "CASE WHEN RAW_JSON:isInitial::STRING = '1' THEN TRUE ELSE FALSE END",
            "ALLOW_TECH_COMPLETE": "CASE WHEN RAW_JSON:allowTechComplete::STRING = '1' THEN TRUE ELSE FALSE END",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.SERVICETYPE_DIM (
            -- Primary Key
            SERVICETYPE_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Text Fields
            TYPE VARCHAR(255),
            DESCRIPTION VARCHAR(255),
            CATEGORY VARCHAR(255),
            SERVICE_TIME_WINDOW VARCHAR(255),
            
            -- Numeric Fields
            DEFAULT_CHARGE NUMBER(10,2),
            ESTIMATED_DURATION_MINUTES NUMBER,
            
            -- Boolean Fields
            ACTIVE BOOLEAN,
            SHOW_ON_AGREEMENT BOOLEAN,
            IS_RECURRING BOOLEAN,
            IS_INITIAL BOOLEAN,
            ALLOW_TECH_COMPLETE BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "genericFlag": {
        "table_name": "GENERICFLAG_DIM",
        "raw_table": "GENERICFLAG_DIM",
        "primary_key": "GENERICFLAG_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "GENERICFLAG_ID": "RAW_JSON:genericFlagID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "TITLE": "RAW_JSON:title::STRING",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "TYPE": "RAW_JSON:type::STRING",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.GENERICFLAG_DIM (
            -- Primary Key
            GENERICFLAG_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Text Fields
            TITLE VARCHAR(255),
            DESCRIPTION VARCHAR(255),
            TYPE VARCHAR(255),
            
            -- Boolean Fields
            ACTIVE BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "product": {
        "table_name": "PRODUCT_DIM",
        "raw_table": "PRODUCT_DIM",
        "primary_key": "PRODUCT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "PRODUCT_ID": "RAW_JSON:productID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "NAME": "RAW_JSON:name::STRING",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "UNIT": "RAW_JSON:unit::STRING",
            "UNIT_PRICE": "RAW_JSON:unitPrice::NUMBER(10,2)",
            "CONCENTRATION": "RAW_JSON:concentration::NUMBER(10,2)",
            "CONCENTRATION_UNIT": "RAW_JSON:concentrationUnit::STRING",
            "SIGNAL_WORD": "RAW_JSON:signalWord::STRING",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PRODUCT_DIM (
            -- Primary Key
            PRODUCT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Text Fields
            NAME VARCHAR(255),
            DESCRIPTION VARCHAR(255),
            UNIT VARCHAR(255),
            CONCENTRATION_UNIT VARCHAR(255),
            SIGNAL_WORD VARCHAR(255),
            
            -- Numeric Fields
            UNIT_PRICE NUMBER(10,2),
            CONCENTRATION NUMBER(10,2),
            
            -- Boolean Fields
            ACTIVE BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "employee": {
        "table_name": "EMPLOYEE_FACT",
        "raw_table": "EMPLOYEE_FACT",
        "primary_key": "EMPLOYEE_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "FNAME": "RAW_JSON:fname::STRING",
            "LNAME": "RAW_JSON:lname::STRING",
            "EMAIL": "RAW_JSON:email::STRING",
            "USERNAME": "RAW_JSON:username::STRING",
            "EMPLOYEE_TYPE": "RAW_JSON:employeeType::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "TEAM_ID": "RAW_JSON:teamID::NUMBER",
            "CREW_ID": "RAW_JSON:crewID::NUMBER",
            "DATE_HIRED": "TRY_TO_DATE(RAW_JSON:dateHired::STRING)",
            "DATE_TERMINATED": "TRY_TO_DATE(RAW_JSON:dateTerminated::STRING)",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "PHONE": "RAW_JSON:phone::STRING",
            "ADDRESS": "RAW_JSON:address::STRING",
            "CITY": "RAW_JSON:city::STRING",
            "STATE": "RAW_JSON:state::STRING",
            "ZIP": "RAW_JSON:zip::STRING",
            "PAY_RATE": "RAW_JSON:payRate::NUMBER(10,2)",
            "COMMISSION_RATE": "RAW_JSON:commissionRate::NUMBER(10,2)",
            "LICENSE_NUMBER": "RAW_JSON:licenseNumber::STRING",
            "LICENSE_EXPIRATION": "TRY_TO_DATE(RAW_JSON:licenseExpiration::STRING)",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.EMPLOYEE_FACT (
            -- Primary Key
            EMPLOYEE_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            TEAM_ID NUMBER,
            CREW_ID NUMBER,
            
            -- Personal Information
            FNAME VARCHAR(255),
            LNAME VARCHAR(255),
            EMAIL VARCHAR(500),
            USERNAME VARCHAR(255),
            PHONE VARCHAR(50),
            
            -- Address
            ADDRESS VARCHAR(500),
            CITY VARCHAR(255),
            STATE VARCHAR(10),
            ZIP VARCHAR(20),
            
            -- Employment
            EMPLOYEE_TYPE VARCHAR(100),
            DATE_HIRED DATE,
            DATE_TERMINATED DATE,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Compensation
            PAY_RATE NUMBER(10,2),
            COMMISSION_RATE NUMBER(10,2),
            
            -- Licensing
            LICENSE_NUMBER VARCHAR(255),
            LICENSE_EXPIRATION DATE,
            
            -- Status
            ACTIVE BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "subscription": {
        "table_name": "SUBSCRIPTION_FACT",
        "raw_table": "SUBSCRIPTION_FACT",
        "primary_key": "SUBSCRIPTION_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "SUBSCRIPTION_ID": "RAW_JSON:subscriptionID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "SERVICE_TYPE_ID": "RAW_JSON:serviceTypeID::NUMBER",
            "REGION_ID": "RAW_JSON:regionID::NUMBER",
            "FREQUENCY": "RAW_JSON:frequency::STRING",
            "AMOUNT": "RAW_JSON:amount::NUMBER(10,2)",
            "INITIAL_AMOUNT": "RAW_JSON:initialAmount::NUMBER(10,2)",
            "AGREEMENT_LENGTH": "RAW_JSON:agreementLength::NUMBER",
            "STATUS": "RAW_JSON:status::STRING",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "DATE_CANCELLED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCancelled::STRING, '0000-00-00 00:00:00'))",
            "NEXT_SERVICE": "TRY_TO_DATE(RAW_JSON:nextService::STRING)",
            "LAST_SERVICE": "TRY_TO_DATE(RAW_JSON:lastService::STRING)",
            "PREFERRED_TECH": "RAW_JSON:preferredTech::STRING",
            "PREFERRED_DAYS": "RAW_JSON:preferredDays::STRING",
            "SOURCE": "RAW_JSON:source::STRING",
            "ANNUAL_RECURRING_SERVICES": "RAW_JSON:annualRecurringServices::NUMBER",
            "SOLD_BY": "RAW_JSON:soldBy::STRING",
            "NOTES": "RAW_JSON:notes::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.SUBSCRIPTION_FACT (
            -- Primary Key
            SUBSCRIPTION_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            SERVICE_TYPE_ID NUMBER,
            REGION_ID NUMBER,
            
            -- Service Details
            FREQUENCY VARCHAR(50),
            AGREEMENT_LENGTH NUMBER,
            STATUS VARCHAR(50),
            SOURCE VARCHAR(255),
            ANNUAL_RECURRING_SERVICES NUMBER,
            
            -- Dates
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            DATE_CANCELLED TIMESTAMP_NTZ,
            NEXT_SERVICE DATE,
            LAST_SERVICE DATE,
            
            -- Financial
            AMOUNT NUMBER(10,2),
            INITIAL_AMOUNT NUMBER(10,2),
            
            -- Assignment
            PREFERRED_TECH VARCHAR(50),
            PREFERRED_DAYS VARCHAR(255),
            SOLD_BY VARCHAR(255),
            
            -- Notes
            NOTES VARCHAR(2000),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "ticket": {
        "table_name": "TICKET_FACT",
        "raw_table": "TICKET_FACT",
        "primary_key": "TICKET_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "TICKET_ID": "RAW_JSON:ticketID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "SUBSCRIPTION_ID": "RAW_JSON:subscriptionID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "SERVICE_TYPE_ID": "RAW_JSON:serviceTypeID::NUMBER",
            "APPOINTMENT_ID": "RAW_JSON:appointmentID::NUMBER",
            "DATE_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCreated::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "TOTAL": "RAW_JSON:total::NUMBER(10,2)",
            "TAX": "RAW_JSON:tax::NUMBER(10,2)",
            "BALANCE": "RAW_JSON:balance::NUMBER(10,2)",
            "PAYMENT_STATUS": "RAW_JSON:paymentStatus::STRING",
            "STATUS": "RAW_JSON:status::STRING",
            "NOTES": "RAW_JSON:notes::STRING",
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "SOLD_BY": "RAW_JSON:soldBy::STRING",
            "TAX_RATE": "RAW_JSON:taxRate::NUMBER(10,4)",
            "DISCOUNT_AMOUNT": "RAW_JSON:discountAmount::NUMBER(10,2)",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.TICKET_FACT (
            -- Primary Key
            TICKET_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            SUBSCRIPTION_ID NUMBER,
            OFFICE_ID NUMBER,
            SERVICE_TYPE_ID NUMBER,
            APPOINTMENT_ID NUMBER,
            EMPLOYEE_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Financial
            TOTAL NUMBER(10,2),
            TAX NUMBER(10,2),
            BALANCE NUMBER(10,2),
            TAX_RATE NUMBER(10,4),
            DISCOUNT_AMOUNT NUMBER(10,2),
            
            -- Status
            PAYMENT_STATUS VARCHAR(50),
            STATUS VARCHAR(50),
            
            -- Personnel
            SOLD_BY VARCHAR(255),
            
            -- Notes
            NOTES VARCHAR(2000),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "payment": {
        "table_name": "PAYMENT_FACT",
        "raw_table": "PAYMENT_FACT",
        "primary_key": "PAYMENT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "PAYMENT_ID": "RAW_JSON:paymentID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "AMOUNT": "RAW_JSON:amount::NUMBER(10,2)",
            "DATE_CREATED": "TRY_TO_DATE(RAW_JSON:date::STRING)",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "PAYMENT_METHOD": "RAW_JSON:paymentMethod::STRING",
            "STATUS": "RAW_JSON:status::STRING",
            "CHECK_NUMBER": "RAW_JSON:checkNumber::STRING",
            "NOTES": "RAW_JSON:notes::STRING",
            "COLLECTED_BY": "RAW_JSON:collectedBy::STRING",
            "DEPOSITED": "CASE WHEN RAW_JSON:deposited::STRING = '1' THEN TRUE ELSE FALSE END",
            "REFUNDED": "CASE WHEN RAW_JSON:refunded::STRING = '1' THEN TRUE ELSE FALSE END",
            "INVOICE_NUMBERS": "RAW_JSON:invoiceNumbers::STRING",
            "APPLIED_AMOUNT": "RAW_JSON:appliedAmount::NUMBER(10,2)",
            "UNAPPLIED_AMOUNT": "RAW_JSON:unappliedAmount::NUMBER(10,2)",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PAYMENT_FACT (
            -- Primary Key
            PAYMENT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_CREATED DATE,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Financial
            AMOUNT NUMBER(10,2),
            APPLIED_AMOUNT NUMBER(10,2),
            UNAPPLIED_AMOUNT NUMBER(10,2),
            
            -- Payment Details
            PAYMENT_METHOD VARCHAR(255),
            STATUS VARCHAR(50),
            CHECK_NUMBER VARCHAR(255),
            INVOICE_NUMBERS VARCHAR(255),
            
            -- Personnel
            COLLECTED_BY VARCHAR(255),
            
            -- Status Flags
            DEPOSITED BOOLEAN,
            REFUNDED BOOLEAN,
            
            -- Notes
            NOTES VARCHAR(2000),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    }
,
"appliedPayment": {
        "table_name": "APPLIEDPAYMENT_FACT",
        "raw_table": "APPLIEDPAYMENT_FACT",
        "primary_key": "APPLIEDPAYMENT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "APPLIEDPAYMENT_ID": "RAW_JSON:appliedPaymentID::NUMBER",
            "PAYMENT_ID": "RAW_JSON:paymentID::NUMBER",
            "INVOICE_ID": "RAW_JSON:invoiceID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "AMOUNT": "RAW_JSON:amount::NUMBER(10,2)",
            "DATE_APPLIED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateApplied::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "APPLIED_BY": "RAW_JSON:appliedBy::STRING",
            "NOTES": "RAW_JSON:notes::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.APPLIEDPAYMENT_FACT (
            -- Primary Key
            APPLIEDPAYMENT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            PAYMENT_ID NUMBER,
            INVOICE_ID NUMBER,
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_APPLIED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Financial
            AMOUNT NUMBER(10,2),
            
            -- Personnel
            APPLIED_BY VARCHAR(255),
            
            -- Notes
            NOTES VARCHAR(2000),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "note": {
        "table_name": "NOTE_FACT",
        "raw_table": "NOTE_FACT",
        "primary_key": "NOTE_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "NOTE_ID": "RAW_JSON:noteID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "SUBSCRIPTION_ID": "RAW_JSON:subscriptionID::NUMBER",
            "APPOINTMENT_ID": "RAW_JSON:appointmentID::NUMBER",
            "TICKET_ID": "RAW_JSON:ticketID::NUMBER",
            "TYPE": "RAW_JSON:type::STRING",
            "NOTE_TEXT": "RAW_JSON:note::STRING",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "ADDED_BY": "RAW_JSON:addedBy::STRING",
            "PINNED": "CASE WHEN RAW_JSON:pinned::STRING = '1' THEN TRUE ELSE FALSE END",
            "SHOW_ON_AGREEMENT": "CASE WHEN RAW_JSON:showOnAgreement::STRING = '1' THEN TRUE ELSE FALSE END",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.NOTE_FACT (
            -- Primary Key
            NOTE_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            EMPLOYEE_ID NUMBER,
            SUBSCRIPTION_ID NUMBER,
            APPOINTMENT_ID NUMBER,
            TICKET_ID NUMBER,
            
            -- Dates
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Content
            TYPE VARCHAR(100),
            NOTE_TEXT VARCHAR(2000),
            
            -- Personnel
            ADDED_BY VARCHAR(255),
            
            -- Flags
            PINNED BOOLEAN,
            SHOW_ON_AGREEMENT BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "task": {
        "table_name": "TASK_FACT",
        "raw_table": "TASK_FACT",
        "primary_key": "TASK_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "TASK_ID": "RAW_JSON:taskID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "ASSIGNED_TO": "RAW_JSON:assignedTo::NUMBER",
            "TITLE": "RAW_JSON:title::STRING",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "TYPE": "RAW_JSON:type::STRING",
            "STATUS": "RAW_JSON:status::STRING",
            "PRIORITY": "RAW_JSON:priority::STRING",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "DATE_DUE": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateDue::STRING, '0000-00-00 00:00:00'))",
            "DATE_COMPLETED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCompleted::STRING, '0000-00-00 00:00:00'))",
            "COMPLETED_BY": "RAW_JSON:completedBy::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.TASK_FACT (
            -- Primary Key
            TASK_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            EMPLOYEE_ID NUMBER,
            ASSIGNED_TO NUMBER,
            
            -- Dates
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            DATE_DUE TIMESTAMP_NTZ,
            DATE_COMPLETED TIMESTAMP_NTZ,
            
            -- Content
            TITLE VARCHAR(255),
            DESCRIPTION VARCHAR(2000),
            TYPE VARCHAR(100),
            STATUS VARCHAR(50),
            PRIORITY VARCHAR(50),
            
            -- Personnel
            COMPLETED_BY VARCHAR(255),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "door": {
        "table_name": "DOOR_FACT",
        "raw_table": "DOOR_FACT",
        "primary_key": "DOOR_ID",
        "date_updated_field": "TIME_CREATED",
        "columns": {
            "DOOR_ID": "RAW_JSON:doorID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "APPOINTMENT_ID": "RAW_JSON:appointmentID::NUMBER",
            "TIME_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:timeCreated::STRING, '0000-00-00 00:00:00'))",
            "LATITUDE": "RAW_JSON:latitude::NUMBER(10,7)",
            "LONGITUDE": "RAW_JSON:longitude::NUMBER(10,7)",
            "ADDRESS": "RAW_JSON:address::STRING",
            "NOTES": "RAW_JSON:notes::STRING",
            "PHOTO_URL": "RAW_JSON:photoUrl::STRING",
            "STATUS": "RAW_JSON:status::STRING",
            "KNOCKS": "RAW_JSON:knocks::NUMBER",
            "TYPE": "RAW_JSON:type::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.DOOR_FACT (
            -- Primary Key
            DOOR_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            EMPLOYEE_ID NUMBER,
            APPOINTMENT_ID NUMBER,
            
            -- Dates
            TIME_CREATED TIMESTAMP_NTZ,
            
            -- Location
            LATITUDE NUMBER(10,7),
            LONGITUDE NUMBER(10,7),
            ADDRESS VARCHAR(500),
            
            -- Content
            NOTES VARCHAR(2000),
            PHOTO_URL VARCHAR(255),
            STATUS VARCHAR(50),
            TYPE VARCHAR(100),
            
            -- Metrics
            KNOCKS NUMBER,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "disbursement": {
        "table_name": "DISBURSEMENT_FACT",
        "raw_table": "DISBURSEMENT_FACT",
        "primary_key": "GATEWAY_DISBURSEMENT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "GATEWAY_DISBURSEMENT_ID": "RAW_JSON:gatewayDisbursementID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "TOTAL_AMOUNT": "RAW_JSON:totalAmount::NUMBER(10,2)",
            "DATE_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCreated::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "STATUS": "RAW_JSON:status::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.DISBURSEMENT_FACT (
            -- Primary Key
            GATEWAY_DISBURSEMENT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Financial
            TOTAL_AMOUNT NUMBER(10,2),
            
            -- Status
            STATUS VARCHAR(50),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "genericFlagAssignment": {
        "table_name": "GENERICFLAGASSIGNMENT_FACT",
        "raw_table": "GENERICFLAGASSIGNMENT_FACT",
        "primary_key": "GENERICFLAGASSIGNMENT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "GENERICFLAGASSIGNMENT_ID": "RAW_JSON:genericFlagAssignmentID::NUMBER",
            "GENERICFLAG_ID": "RAW_JSON:genericFlagID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "DATE_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCreated::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "ASSIGNED_BY": "RAW_JSON:assignedBy::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.GENERICFLAGASSIGNMENT_FACT (
            -- Primary Key
            GENERICFLAGASSIGNMENT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            GENERICFLAG_ID NUMBER,
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Personnel
            ASSIGNED_BY VARCHAR(255),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "knock": {
        "table_name": "KNOCK_FACT",
        "raw_table": "KNOCK_FACT",
        "primary_key": "KNOCK_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "KNOCK_ID": "RAW_JSON:knockID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "EMPLOYEE_ID": "RAW_JSON:employeeID::NUMBER",
            "DATE_ADDED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateAdded::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "LATITUDE": "RAW_JSON:latitude::NUMBER(10,7)",
            "LONGITUDE": "RAW_JSON:longitude::NUMBER(10,7)",
            "NOTES": "RAW_JSON:notes::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.KNOCK_FACT (
            -- Primary Key
            KNOCK_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            EMPLOYEE_ID NUMBER,
            
            -- Dates
            DATE_ADDED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Location
            LATITUDE NUMBER(10,7),
            LONGITUDE NUMBER(10,7),
            
            -- Content
            NOTES VARCHAR(2000),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "paymentProfile": {
        "table_name": "PAYMENTPROFILE_FACT",
        "raw_table": "PAYMENTPROFILE_FACT",
        "primary_key": "PAYMENTPROFILE_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "PAYMENTPROFILE_ID": "RAW_JSON:paymentProfileID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "PAYMENT_METHOD": "RAW_JSON:paymentMethod::STRING",
            "CARD_TYPE": "RAW_JSON:cardType::STRING",
            "CARD_LAST_FOUR": "RAW_JSON:cardLastFour::STRING",
            "CARD_EXPIRATION": "RAW_JSON:cardExpiration::STRING",
            "BILLING_FNAME": "RAW_JSON:billingFname::STRING",
            "BILLING_LNAME": "RAW_JSON:billingLname::STRING",
            "BILLING_ADDRESS": "RAW_JSON:billingAddress::STRING",
            "BILLING_CITY": "RAW_JSON:billingCity::STRING",
            "BILLING_STATE": "RAW_JSON:billingState::STRING",
            "BILLING_ZIP": "RAW_JSON:billingZip::STRING",
            "BANK_NAME": "RAW_JSON:bankName::STRING",
            "ACCOUNT_TYPE": "RAW_JSON:accountType::STRING",
            "ACCOUNT_LAST_FOUR": "RAW_JSON:accountLastFour::STRING",
            "ROUTING_NUMBER": "RAW_JSON:routingNumber::STRING",
            "DATE_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCreated::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.PAYMENTPROFILE_FACT (
            -- Primary Key
            PAYMENTPROFILE_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Payment Method
            PAYMENT_METHOD VARCHAR(50),
            
            -- Card Information
            CARD_TYPE VARCHAR(50),
            CARD_LAST_FOUR VARCHAR(4),
            CARD_EXPIRATION VARCHAR(10),
            
            -- Bank Information
            BANK_NAME VARCHAR(255),
            ACCOUNT_TYPE VARCHAR(50),
            ACCOUNT_LAST_FOUR VARCHAR(4),
            ROUTING_NUMBER VARCHAR(20),
            
            -- Billing Information
            BILLING_FNAME VARCHAR(255),
            BILLING_LNAME VARCHAR(255),
            BILLING_ADDRESS VARCHAR(500),
            BILLING_CITY VARCHAR(255),
            BILLING_STATE VARCHAR(10),
            BILLING_ZIP VARCHAR(20),
            
            -- Status
            ACTIVE BOOLEAN,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "appointmentReminder": {
        "table_name": "APPOINTMENTREMINDER_FACT",
        "raw_table": "APPOINTMENTREMINDER_FACT",
        "primary_key": "APPOINTMENTREMINDER_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "APPOINTMENTREMINDER_ID": "RAW_JSON:appointmentReminderID::NUMBER",
            "APPOINTMENT_ID": "RAW_JSON:appointmentID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "OFFICE_ID": "RAW_JSON:officeID::NUMBER",
            "REMINDER_TYPE": "RAW_JSON:reminderType::STRING",
            "REMINDER_METHOD": "RAW_JSON:reminderMethod::STRING",
            "DATE_SENT": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateSent::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "STATUS": "RAW_JSON:status::STRING",
            "MESSAGE": "RAW_JSON:message::STRING",
            "PHONE_NUMBER": "RAW_JSON:phoneNumber::STRING",
            "EMAIL_ADDRESS": "RAW_JSON:emailAddress::STRING",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.APPOINTMENTREMINDER_FACT (
            -- Primary Key
            APPOINTMENTREMINDER_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            APPOINTMENT_ID NUMBER,
            CUSTOMER_ID NUMBER,
            OFFICE_ID NUMBER,
            
            -- Dates
            DATE_SENT TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Reminder Details
            REMINDER_TYPE VARCHAR(100),
            REMINDER_METHOD VARCHAR(50),
            STATUS VARCHAR(50),
            MESSAGE VARCHAR(2000),
            
            -- Contact Information
            PHONE_NUMBER VARCHAR(50),
            EMAIL_ADDRESS VARCHAR(500),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "cancellationReason": {
        "table_name": "CANCELLATIONREASON_DIM",
        "raw_table": "CANCELLATIONREASON_DIM",
        "primary_key": "CANCELLATIONREASON_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "CANCELLATIONREASON_ID": "RAW_JSON:cancellationReasonID::NUMBER",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.CANCELLATIONREASON_DIM (
            -- Primary Key
            CANCELLATIONREASON_ID NUMBER PRIMARY KEY,
            
            -- Content
            DESCRIPTION VARCHAR(255),
            
            -- Status
            ACTIVE BOOLEAN,
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "reserviceReason": {
        "table_name": "RESERVICEREASON_DIM",
        "raw_table": "RESERVICEREASON_DIM",
        "primary_key": "RESERVICEREASON_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "RESERVICEREASON_ID": "RAW_JSON:reserviceReasonID::NUMBER",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.RESERVICEREASON_DIM (
            -- Primary Key
            RESERVICEREASON_ID NUMBER PRIMARY KEY,
            
            -- Content
            DESCRIPTION VARCHAR(255),
            
            -- Status
            ACTIVE BOOLEAN,
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "customerSource": {
        "table_name": "CUSTOMERSOURCE_DIM",
        "raw_table": "CUSTOMERSOURCE_DIM",
        "primary_key": "CUSTOMERSOURCE_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "CUSTOMERSOURCE_ID": "RAW_JSON:customerSourceID::NUMBER",
            "DESCRIPTION": "RAW_JSON:description::STRING",
            "ACTIVE": "CASE WHEN RAW_JSON:active::STRING = '1' THEN TRUE ELSE FALSE END",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.CUSTOMERSOURCE_DIM (
            -- Primary Key
            CUSTOMERSOURCE_ID NUMBER PRIMARY KEY,
            
            -- Content
            DESCRIPTION VARCHAR(255),
            
            -- Status
            ACTIVE BOOLEAN,
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "ticketItem": {
        "table_name": "TICKETITEM_FACT",
        "raw_table": "TICKETITEM_FACT",
        "primary_key": "TICKETITEM_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "TICKETITEM_ID": "RAW_JSON:ticketItemID::NUMBER",
            "TICKET_ID": "RAW_JSON:ticketID::NUMBER",
            "SERVICE_TYPE_ID": "RAW_JSON:serviceTypeID::NUMBER",
            "PRODUCT_ID": "RAW_JSON:productID::NUMBER",
            "QUANTITY": "RAW_JSON:quantity::NUMBER",
            "UNIT_PRICE": "RAW_JSON:unitPrice::NUMBER(10,2)",
            "TOTAL_PRICE": "RAW_JSON:totalPrice::NUMBER(10,2)",
            "DATE_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCreated::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.TICKETITEM_FACT (
            -- Primary Key
            TICKETITEM_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            TICKET_ID NUMBER,
            SERVICE_TYPE_ID NUMBER,
            PRODUCT_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Item Details
            QUANTITY NUMBER,
            UNIT_PRICE NUMBER(10,2),
            TOTAL_PRICE NUMBER(10,2),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "chargeback": {
        "table_name": "CHARGEBACK_FACT",
        "raw_table": "CHARGEBACK_FACT",
        "primary_key": "CHARGEBACK_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "CHARGEBACK_ID": "RAW_JSON:chargebackID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "PAYMENT_ID": "RAW_JSON:paymentID::NUMBER",
            "AMOUNT": "RAW_JSON:amount::NUMBER(10,2)",
            "REASON": "RAW_JSON:reason::STRING",
            "STATUS": "RAW_JSON:status::STRING",
            "DATE_CREATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateCreated::STRING, '0000-00-00 00:00:00'))",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.CHARGEBACK_FACT (
            -- Primary Key
            CHARGEBACK_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            PAYMENT_ID NUMBER,
            
            -- Dates
            DATE_CREATED TIMESTAMP_NTZ,
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Financial
            AMOUNT NUMBER(10,2),
            
            -- Details
            REASON VARCHAR(255),
            STATUS VARCHAR(50),
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
    "additionalContacts": {
        "table_name": "ADDITIONALCONTACT_FACT",
        "raw_table": "ADDITIONALCONTACT_FACT",
        "primary_key": "ADDITIONALCONTACT_ID",
        "date_updated_field": "DATE_UPDATED",
        "columns": {
            "ADDITIONALCONTACT_ID": "RAW_JSON:additionalContactID::NUMBER",
            "CUSTOMER_ID": "RAW_JSON:customerID::NUMBER",
            "FIRST_NAME": "RAW_JSON:firstName::STRING",
            "LAST_NAME": "RAW_JSON:lastName::STRING",
            "PHONE": "RAW_JSON:phone::STRING",
            "EMAIL": "RAW_JSON:email::STRING",
            "RELATIONSHIP": "RAW_JSON:relationship::STRING",
            "DATE_UPDATED": "TRY_TO_TIMESTAMP_NTZ(NULLIF(RAW_JSON:dateUpdated::STRING, '0000-00-00 00:00:00'))",
            "OFFICE_ID_SOURCE": "RAW_JSON:_office_id::STRING",
            "OFFICE_NAME_SOURCE": "RAW_JSON:_office_name::STRING"
        },
        "table_ddl": """
        CREATE TABLE IF NOT EXISTS {database}.{schema}.ADDITIONALCONTACT_FACT (
            -- Primary Key
            ADDITIONALCONTACT_ID NUMBER PRIMARY KEY,
            
            -- Foreign Keys
            CUSTOMER_ID NUMBER,
            
            -- Personal Information
            FIRST_NAME VARCHAR(255),
            LAST_NAME VARCHAR(255),
            PHONE VARCHAR(50),
            EMAIL VARCHAR(500),
            RELATIONSHIP VARCHAR(100),
            
            -- Dates
            DATE_UPDATED TIMESTAMP_NTZ,
            
            -- Source Tracking
            OFFICE_ID_SOURCE VARCHAR(50),
            OFFICE_NAME_SOURCE VARCHAR(100),
            
            -- Metadata
            RAW_LOAD_TIMESTAMP TIMESTAMP_NTZ,
            STAGING_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            BATCH_ID VARCHAR(255),
            SOURCE_FILE VARCHAR(255)
        )
        """
    },
}


class MultiEntityStagingProcessor:
    """Processes raw JSON data into staging tables for multiple entities"""
    
    def __init__(self, snowflake_conn: SnowflakeConnector, 
                 raw_database: str = "RAW_DB_DEV",
                 staging_database: str = "STAGING_DB_DEV",
                 schema: str = "FIELDROUTES"):
        self.snowflake = snowflake_conn
        self.raw_database = raw_database
        self.staging_database = staging_database
        self.schema = schema
    
    def create_staging_schema(self) -> bool:
        """Create staging schema if it doesn't exist"""
        try:
            self.snowflake.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.staging_database}")
            self.snowflake.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.staging_database}.{self.schema}")
            print(f"Staging schema {self.staging_database}.{self.schema} is ready")
            return True
        except Exception as e:
            print(f"Error creating staging schema: {e}")
            return False
    
    def create_entity_staging_table(self, entity: str) -> bool:
        """Create staging table for specified entity"""
        
        if entity not in STAGING_CONFIG:
            print(f"Entity '{entity}' not supported. Available: {list(STAGING_CONFIG.keys())}")
            return False
        
        config = STAGING_CONFIG[entity]
        table_ddl = config["table_ddl"].format(
            database=self.staging_database,
            schema=self.schema
        )
        
        try:
            self.snowflake.cursor.execute(table_ddl)
            print(f"Staging table {config['table_name']} created successfully")
            return True
        except Exception as e:
            print(f"Error creating staging table for {entity}: {e}")
            return False
    
    def load_entity_to_staging(self, entity: str, batch_id: Optional[str] = None) -> Dict[str, Any]:
        """Load and transform entity data from raw to staging"""
        
        if entity not in STAGING_CONFIG:
            return {
                "success": False, 
                "error": f"Entity '{entity}' not supported. Available: {list(STAGING_CONFIG.keys())}"
            }
        
        if not self.create_staging_schema():
            return {"success": False, "error": "Failed to create staging schema"}
        
        if not self.create_entity_staging_table(entity):
            return {"success": False, "error": f"Failed to create staging table for {entity}"}
        
        config = STAGING_CONFIG[entity]
        
        # Build the SELECT clause for transformations
        select_columns = []
        for column_name, transformation in config["columns"].items():
            select_columns.append(f"{transformation} AS {column_name}")
        
        select_clause = ",\n                ".join(select_columns)
        
        # Build the MERGE statement
        merge_sql = f"""
        MERGE INTO {self.staging_database}.{self.schema}.{config['table_name']} AS target
        USING (
            SELECT DISTINCT
                {select_clause},
                LOAD_TIMESTAMP AS RAW_LOAD_TIMESTAMP,
                BATCH_ID,
                SOURCE_FILE
            FROM {self.raw_database}.{self.schema}.{config['raw_table']}
            {f"WHERE BATCH_ID = '{batch_id}'" if batch_id else ""}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {config['columns'][config['primary_key']]} 
                ORDER BY LOAD_TIMESTAMP DESC
            ) = 1
        ) AS source
        ON target.{config['primary_key']} = source.{config['primary_key']}
        WHEN MATCHED AND (
            target.{config['date_updated_field']} < source.{config['date_updated_field']} OR
            target.RAW_LOAD_TIMESTAMP < source.RAW_LOAD_TIMESTAMP
        ) THEN UPDATE SET
            {self._build_update_clause(config)}
        WHEN NOT MATCHED THEN INSERT (
            {', '.join(config['columns'].keys())},
            RAW_LOAD_TIMESTAMP,
            BATCH_ID,
            SOURCE_FILE
        ) VALUES (
            {', '.join([f"source.{col}" for col in config['columns'].keys()])},
            source.RAW_LOAD_TIMESTAMP,
            source.BATCH_ID,
            source.SOURCE_FILE
        )
        """
        
        try:
            print(f"Starting merge operation for {entity} from RAW to STAGING...")
            result = self.snowflake.cursor.execute(merge_sql)
            
            # Get merge statistics
            for row in result:
                rows_inserted = row[0]
                rows_updated = row[1]
                total_rows = rows_inserted + rows_updated
                
            self.snowflake.commit()
            
            print(f"Merge completed successfully for {entity}:")
            print(f"  - Rows inserted: {rows_inserted}")
            print(f"  - Rows updated: {rows_updated}")
            print(f"  - Total rows processed: {total_rows}")
            
            return {
                "success": True,
                "entity": entity,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "total_rows_processed": total_rows
            }
            
        except Exception as e:
            print(f"Error during merge operation for {entity}: {e}")
            self.snowflake.connection.rollback()
            return {"success": False, "entity": entity, "error": str(e)}
    
    def _build_update_clause(self, config: Dict[str, Any]) -> str:
        """Build the UPDATE SET clause for MERGE statement"""
        update_assignments = []
        
        for column_name in config["columns"].keys():
            update_assignments.append(f"target.{column_name} = source.{column_name}")
        
        # Add metadata columns
        update_assignments.extend([
            "target.RAW_LOAD_TIMESTAMP = source.RAW_LOAD_TIMESTAMP",
            "target.STAGING_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()",
            "target.BATCH_ID = source.BATCH_ID",
            "target.SOURCE_FILE = source.SOURCE_FILE"
        ])
        
        return ",\n            ".join(update_assignments)
    
    def get_entity_staging_statistics(self, entity: str) -> Dict[str, Any]:
        """Get statistics about the staging table for a specific entity"""
        
        if entity not in STAGING_CONFIG:
            return {
                "success": False,
                "error": f"Entity '{entity}' not supported. Available: {list(STAGING_CONFIG.keys())}"
            }
        
        config = STAGING_CONFIG[entity]
        table_name = config["table_name"]
        
        try:
            # Base statistics
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN({config['date_updated_field']}) as earliest_update,
                MAX({config['date_updated_field']}) as latest_update,
                COUNT(DISTINCT BATCH_ID) as unique_batches
            FROM {self.staging_database}.{self.schema}.{table_name}
            """
            
            self.snowflake.cursor.execute(stats_sql)
            result = self.snowflake.cursor.fetchone()
            
            base_stats = {
                "total_records": result[0],
                "date_range": {
                    "earliest": str(result[1]) if result[1] else None,
                    "latest": str(result[2]) if result[2] else None
                },
                "unique_batches": result[3]
            }
            
            # Entity-specific statistics
            entity_stats = {}
            
            if entity == "disbursementItem":
                fee_stats_sql = f"""
                SELECT 
                    SUM(CASE WHEN IS_FEE THEN 1 ELSE 0 END) as fee_records,
                    SUM(CASE WHEN NOT IS_FEE THEN 1 ELSE 0 END) as non_fee_records,
                    COUNT(DISTINCT GATEWAY_EVENT_TYPE) as unique_event_types,
                    COUNT(DISTINCT GATEWAY_DISBURSEMENT_ID) as unique_disbursements
                FROM {self.staging_database}.{self.schema}.{table_name}
                """
                
                self.snowflake.cursor.execute(fee_stats_sql)
                fee_result = self.snowflake.cursor.fetchone()
                
                entity_stats = {
                    "fee_records": fee_result[0],
                    "non_fee_records": fee_result[1],
                    "unique_event_types": fee_result[2],
                    "unique_disbursements": fee_result[3]
                }
            
            elif entity == "customer":
                customer_stats_sql = f"""
                SELECT 
                    SUM(CASE WHEN IS_ACTIVE THEN 1 ELSE 0 END) as active_customers,
                    SUM(CASE WHEN NOT IS_ACTIVE THEN 1 ELSE 0 END) as inactive_customers,
                    COUNT(DISTINCT SERVICE_STATE) as unique_states,
                    AVG(BALANCE) as avg_balance
                FROM {self.staging_database}.{self.schema}.{table_name}
                """
                
                self.snowflake.cursor.execute(customer_stats_sql)
                customer_result = self.snowflake.cursor.fetchone()
                
                entity_stats = {
                    "active_customers": customer_result[0],
                    "inactive_customers": customer_result[1],
                    "unique_states": customer_result[2],
                    "avg_balance": float(customer_result[3]) if customer_result[3] else 0.0
                }
            
            return {
                "success": True,
                "entity": entity,
                "statistics": {**base_stats, **entity_stats}
            }
            
        except Exception as e:
            return {"success": False, "entity": entity, "error": str(e)}
    
    def load_all_entities_to_staging(self, batch_id: Optional[str] = None) -> Dict[str, Any]:
        """Load all supported entities from raw to staging"""
        
        results = {
            "success": True,
            "entities_processed": [],
            "summary": {
                "total_entities": len(STAGING_CONFIG),
                "successful": 0,
                "failed": 0
            }
        }
        
        for entity in STAGING_CONFIG.keys():
            print(f"\n{'='*50}")
            print(f"PROCESSING ENTITY: {entity.upper()}")
            print(f"{'='*50}")
            
            result = self.load_entity_to_staging(entity, batch_id)
            results["entities_processed"].append(result)
            
            if result["success"]:
                results["summary"]["successful"] += 1
            else:
                results["summary"]["failed"] += 1
                results["success"] = False
        
        return results


# Example usage
if __name__ == "__main__":
    # Snowflake credentials
    snowflake_config = {
        "account": "AUEDKKB-TH88792",
        "user": "EVANUNICK",
        "password": "SnowflakePword1!",
        "warehouse": "ALTAPESTANALYTICS"
    }
    
    print("Connecting to Snowflake...")
    print("=" * 60)
    
    # Connect to Snowflake
    snowflake_conn = SnowflakeConnector(
        account=snowflake_config["account"],
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        warehouse=snowflake_config["warehouse"]
    )
    
    if snowflake_conn.connect():
        print("\nProcessing all entities from RAW to STAGING...")
        print("=" * 60)
        
        # Create multi-entity staging processor
        staging_processor = MultiEntityStagingProcessor(snowflake_conn)
        
        # Load all entities to staging
        result = staging_processor.load_all_entities_to_staging()
        
        if result["success"]:
            print("\n✅ Multi-entity staging load successful!")
            print(f"   - Entities processed: {result['summary']['successful']}/{result['summary']['total_entities']}")
            
            # Get statistics for each entity
            print("\n📊 Entity Statistics:")
            for entity in STAGING_CONFIG.keys():
                stats = staging_processor.get_entity_staging_statistics(entity)
                
                if stats["success"]:
                    statistics = stats["statistics"]
                    print(f"\n{entity.upper()}:")
                    print(f"   - Total records: {statistics['total_records']:,}")
                    print(f"   - Date range: {statistics['date_range']['earliest']} to {statistics['date_range']['latest']}")
                    print(f"   - Unique batches: {statistics['unique_batches']}")
                    
                    # Entity-specific stats
                    if entity == "disbursementItem":
                        print(f"   - Fee records: {statistics['fee_records']:,}")
                        print(f"   - Non-fee records: {statistics['non_fee_records']:,}")
                        print(f"   - Unique event types: {statistics['unique_event_types']}")
                        print(f"   - Unique disbursements: {statistics['unique_disbursements']:,}")
                    elif entity == "customer":
                        print(f"   - Active customers: {statistics['active_customers']:,}")
                        print(f"   - Inactive customers: {statistics['inactive_customers']:,}")
                        print(f"   - Unique states: {statistics['unique_states']}")
                        print(f"   - Average balance: ${statistics['avg_balance']:,.2f}")
        else:
            print(f"\n❌ Multi-entity staging load failed!")
            print(f"   - Successful: {result['summary']['successful']}")
            print(f"   - Failed: {result['summary']['failed']}")
        
        snowflake_conn.disconnect()
    else:
        print("Failed to connect to Snowflake")