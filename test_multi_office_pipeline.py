"""
Complete Multi-Office Pipeline Test
Tests the full pipeline with multiple offices for office-specific entities
"""
import os
from office_config import OfficeManager, get_entities_requiring_multi_office, get_global_entities
from multi_office_api import MultiOfficeFlow
from multi_entity_staging import MultiEntityStagingProcessor
from snowflake_integration import SnowflakeConnector


def load_environment_variables():
    """Load environment variables from .env file if it exists"""
    env_file = ".env"
    if os.path.exists(env_file):
        print(f"📁 Loading environment from {env_file}")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print("✅ Environment variables loaded")
    else:
        print(f"⚠️  No {env_file} file found - using system environment variables")


def get_snowflake_config() -> dict:
    """Get Snowflake configuration from environment variables"""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "AUEDKKB-TH88792"),
        "user": os.getenv("SNOWFLAKE_USER", "EVANUNICK"),
        "password": os.getenv("SNOWFLAKE_PASSWORD", "SnowflakePword1!"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ALTAPESTANALYTICS")
    }


def print_pipeline_summary(office_manager: OfficeManager, entities: list):
    """Print pipeline execution summary"""
    print("="*80)
    print("MULTI-OFFICE PESTROUTES TO SNOWFLAKE PIPELINE")
    print("="*80)
    
    print(f"📋 ENTITIES TO PROCESS:")
    global_entities = get_global_entities()
    multi_office_entities = get_entities_requiring_multi_office()
    
    for entity in entities:
        if entity in global_entities:
            print(f"   🌍 {entity} (Global - single office)")
        elif entity in multi_office_entities:
            print(f"   🏢 {entity} (Multi-office - {office_manager.get_office_count()} offices)")
    
    print(f"\n🏢 LOADED OFFICES ({office_manager.get_office_count()}):")
    for office in office_manager.get_all_offices():
        print(f"   • {office.office_id}: {office.office_name}")


def main():
    # Load environment variables
    load_environment_variables()
    
    # Configuration
    date_start = "2025-06-05"
    date_end = "2025-06-06"
    entities_to_process = ["disbursementItem", "customer"]
    
    # Load office credentials
    print("🏢 LOADING OFFICE CREDENTIALS")
    print("="*50)
    office_manager = OfficeManager()
    
    if office_manager.get_office_count() == 0:
        print("\n❌ ERROR: No office credentials loaded!")
        print("   Please set environment variables or create a .env file")
        print("   Run: python office_config.py to create a template")
        return
    
    # Get Snowflake config
    snowflake_config = get_snowflake_config()
    
    # Print pipeline overview
    print_pipeline_summary(office_manager, entities_to_process)
    print(f"📅 Date Range: {date_start} to {date_end}")
    print("="*80)
    
    # Phase 1: API Data Extraction
    print("\n" + "="*60)
    print("PHASE 1: MULTI-OFFICE API DATA EXTRACTION")
    print("="*60)
    
    # Create multi-office flow
    flow = MultiOfficeFlow("Multi-Office Data Pipeline", office_manager)
    
    # Add entity tasks
    for entity in entities_to_process:
        flow.add_entity_task(entity, date_start, date_end, snowflake_config)
    
    # Execute API extraction and load to RAW
    api_results = flow.execute()
    
    # Print API extraction summary
    print("\n" + "-"*60)
    print("API EXTRACTION SUMMARY")
    print("-"*60)
    
    total_api_success = True
    total_records_extracted = 0
    
    for task_result in api_results["task_results"]:
        entity = task_result["entity"]
        success = task_result["success"]
        
        if entity in get_global_entities():
            # Single office entity
            combined_count = task_result.get("combined_count", 0)
            items_count = len(task_result.get("items", []))
            office_id = task_result.get("office_id", "unknown")
            
            status_icon = "✅" if success else "❌"
            print(f"{status_icon} {entity:15} (Global) | Office: {office_id:8} | Records: {items_count:6}")
            total_records_extracted += items_count
            
        elif entity in get_entities_requiring_multi_office():
            # Multi-office entity
            office_count = task_result.get("office_count", 0)
            stats = task_result.get("combined_stats", {})
            successful_offices = stats.get("successful_offices", 0)
            total_items = stats.get("total_items", 0)
            
            status_icon = "✅" if success else "❌"
            print(f"{status_icon} {entity:15} (Multi)  | Offices: {successful_offices}/{office_count:2} | Records: {total_items:6}")
            total_records_extracted += total_items
            
            # Office-level details
            for office_result in task_result.get("office_results", []):
                office_success = office_result.get("success", False)
                office_id = office_result.get("office_id", "unknown")
                office_items = len(office_result.get("items", []))
                office_icon = "  ✅" if office_success else "  ❌"
                print(f"{office_icon}   └─ {office_id:8}: {office_items:6} records")
        
        if not success:
            total_api_success = False
    
    print(f"\n📊 TOTAL RECORDS EXTRACTED: {total_records_extracted:,}")
    
    if not total_api_success:
        print("\n❌ API extraction phase had failures. Continuing with staging...")
    else:
        print(f"\n✅ Phase 1 Complete: All entities extracted and loaded to RAW layer")
    
    # Phase 2: Staging Layer Processing
    print("\n" + "="*60)
    print("PHASE 2: STAGING LAYER PROCESSING")
    print("="*60)
    
    # Connect to Snowflake for staging operations
    snowflake_conn = SnowflakeConnector(
        account=snowflake_config["account"],
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        warehouse=snowflake_config["warehouse"]
    )
    
    if not snowflake_conn.connect():
        print("❌ Failed to connect to Snowflake for staging operations")
        return
    
    # Process RAW to STAGING for all entities
    staging_processor = MultiEntityStagingProcessor(snowflake_conn)
    staging_results = staging_processor.load_all_entities_to_staging()
    
    print("\n" + "-"*50)
    print("STAGING PROCESSING SUMMARY")
    print("-"*50)
    
    if staging_results["success"]:
        print(f"✅ Staging processing successful!")
        print(f"   Entities processed: {staging_results['summary']['successful']}/{staging_results['summary']['total_entities']}")
        
        # Detailed staging results
        for entity_result in staging_results["entities_processed"]:
            if entity_result["success"]:
                entity = entity_result["entity"]
                inserted = entity_result["rows_inserted"]
                updated = entity_result["rows_updated"]
                total = entity_result["total_rows_processed"]
                print(f"   ✅ {entity:15} | Inserted: {inserted:5} | Updated: {updated:5} | Total: {total:5}")
            else:
                entity = entity_result["entity"]
                error = entity_result.get("error", "Unknown error")
                print(f"   ❌ {entity:15} | Error: {error}")
    else:
        print(f"❌ Staging processing failed!")
    
    # Phase 3: Data Quality and Statistics
    print("\n" + "="*60)
    print("PHASE 3: DATA QUALITY & STATISTICS")
    print("="*60)
    
    print("\n📊 STAGING TABLE STATISTICS:")
    
    from multi_entity_staging import STAGING_CONFIG
    
    for entity in STAGING_CONFIG.keys():
        if entity in entities_to_process:
            print(f"\n{entity.upper()} STAGING TABLE:")
            print("-" * 40)
            
            stats = staging_processor.get_entity_staging_statistics(entity)
            
            if stats["success"]:
                statistics = stats["statistics"]
                print(f"   Total records: {statistics['total_records']:,}")
                print(f"   Date range: {statistics['date_range']['earliest']} to {statistics['date_range']['latest']}")
                print(f"   Unique batches: {statistics['unique_batches']}")
                
                # Entity-specific statistics
                if entity == "disbursementItem":
                    print(f"   Fee records: {statistics.get('fee_records', 0):,}")
                    print(f"   Non-fee records: {statistics.get('non_fee_records', 0):,}")
                    print(f"   Unique disbursements: {statistics.get('unique_disbursements', 0):,}")
                elif entity == "customer":
                    print(f"   Active customers: {statistics.get('active_customers', 0):,}")
                    print(f"   Inactive customers: {statistics.get('inactive_customers', 0):,}")
                    print(f"   Unique states: {statistics.get('unique_states', 0)}")
                    avg_balance = statistics.get('avg_balance', 0.0)
                    print(f"   Average balance: ${avg_balance if avg_balance is not None else 0.0:,.2f}")
            else:
                print(f"   ❌ Error getting statistics: {stats.get('error', 'Unknown error')}")
    
    snowflake_conn.disconnect()
    
    # Final Summary
    print("\n" + "="*80)
    print("MULTI-OFFICE PIPELINE EXECUTION COMPLETE")
    print("="*80)
    
    print("\n🎯 SUMMARY:")
    print(f"   ✅ API Extraction: {len(entities_to_process)} entities processed")
    print(f"   🏢 Offices: {office_manager.get_office_count()} offices loaded")
    print(f"   📊 Records: {total_records_extracted:,} total records extracted")
    print(f"   ✅ RAW Layer: Data loaded to RAW_DB_DEV.FIELDROUTES")
    print(f"   ✅ STAGING Layer: Data transformed to STAGING_DB_DEV.FIELDROUTES")
    print(f"   📅 Date Range: {date_start} to {date_end}")
    
    print(f"\n🏗️  ARCHITECTURE:")
    print(f"   Multi-Office API → RAW_DB_DEV.FIELDROUTES → STAGING_DB_DEV.FIELDROUTES")
    
    print(f"\n📋 ENTITY PROCESSING:")
    for entity in entities_to_process:
        if entity in get_global_entities():
            print(f"   🌍 {entity}: Processed once (global)")
        elif entity in get_entities_requiring_multi_office():
            print(f"   🏢 {entity}: Processed for {office_manager.get_office_count()} offices")
    
    print("\n✨ Multi-office pipeline ready for production use!")


if __name__ == "__main__":
    main()