"""
PestRoutes to Snowflake Main Pipeline Flows
Orchestrates the complete ETL process using Prefect with secret blocks
"""
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.blocks.core import Block
from prefect_snowflake.database import SnowflakeConnector as PrefectSnowflakeConnector

from office_config import MULTI_OFFICE_ENTITY_CONFIG, get_entities_requiring_multi_office, get_global_entities, OfficeManager, OfficeCredentials
from multi_office_api import MultiOfficePestRoutesAPI, MultiOfficeEntityTask
from snowflake_integration import SnowflakeConnector as CustomSnowflakeConnector
from multi_entity_staging import MultiEntityStagingProcessor


@task(name="load-credentials")
async def load_credentials() -> Dict[str, Any]:
    """Load all credentials from Prefect secret blocks or environment variables"""
    logger = get_run_logger()
    logger.info("Loading credentials...")
    
    # Try to load from Prefect secret blocks first, fall back to environment variables
    snowflake_config = {}
    office_credentials = {}
    
    try:
        # Try Prefect secret blocks (for Prefect Cloud deployment)
        logger.info("Attempting to load from Prefect secret blocks...")
        prefect_snowflake_connector = await PrefectSnowflakeConnector.load("snowflake-altapestdb")
        
        snowflake_config = {
            "account": prefect_snowflake_connector.credentials.account,
            "user": prefect_snowflake_connector.credentials.user,
            "password": prefect_snowflake_connector.credentials.password.get_secret_value(),
            "warehouse": getattr(prefect_snowflake_connector.credentials, 'warehouse', os.getenv("SNOWFLAKE_WAREHOUSE", "ALTAPESTANALYTICS")),
            "database": getattr(prefect_snowflake_connector, 'database', None),
            "schema": getattr(prefect_snowflake_connector, 'schema_', None)
        }
        
        logger.info("✅ Loaded Snowflake credentials from secret blocks")
        
        # Office name mappings for secret blocks
        office_mappings = {
            "office_1": "seattle",
            "office_3": "spokane", 
            "office_4": "okc",
            "office_5": "tulsa",
            "office_6": "wichita",
            "office_7": "austin",
            "office_8": "dc",
            "office_9": "dallas",
            "office_10": "lasvegas",
            "office_12": "nashville",
            "office_13": "virginiabeach",
            "office_14": "kansascity",
            "office_15": "charlotte",
            "office_16": "sanantonio",
            "office_17": "knoxville",
            "office_18": "ftworth",
            "office_19": "tricities"
        }
        
        # Load PestRoutes API credentials for each office
        for office_id, office_name in office_mappings.items():
            try:
                # Load secrets using Prefect 3.x Secret blocks
                api_key_block = await Secret.load(f"fieldroutes-{office_name}-auth-key")
                token_block = await Secret.load(f"fieldroutes-{office_name}-auth-token")
                
                office_credentials[office_id] = {
                    "api_key": api_key_block.get(),
                    "token": token_block.get(),
                    "office_name": office_name.replace("_", " ").title()
                }
                logger.info(f"✅ Loaded credentials for {office_name}")
                
            except Exception as e:
                logger.warning(f"⚠️  Could not load credentials for {office_name}: {e}")
    
    except Exception as e:
        # Fallback to environment variables (for local testing)
        logger.warning(f"Could not load from secret blocks: {e}")
        logger.info("Falling back to environment variables...")
        
        # Load Snowflake from environment
        snowflake_config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", "AUEDKKB-TH88792"),
            "user": os.getenv("SNOWFLAKE_USER", "EVANUNICK"),
            "password": os.getenv("SNOWFLAKE_PASSWORD", "SnowflakePword1!"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ALTAPESTANALYTICS"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA")
        }
    
    # Always try to load office credentials from environment if not loaded from blocks
    if not office_credentials:
        logger.info("Loading office credentials from environment variables...")
        for i in range(1, 20):  # offices 1-19
            api_key = os.getenv(f"PESTROUTES_OFFICE_{i}_API_KEY")
            token = os.getenv(f"PESTROUTES_OFFICE_{i}_TOKEN")
            office_name = os.getenv(f"PESTROUTES_OFFICE_{i}_NAME", f"Office {i}")
            
            if api_key and token:
                office_id = f"office_{i}"
                office_credentials[office_id] = {
                    "api_key": api_key,
                    "token": token,
                    "office_name": office_name
                }
                logger.info(f"✅ Loaded credentials for {office_name} from environment")
    
    credentials = {
        "snowflake": snowflake_config,
        "offices": office_credentials
    }
    
    logger.info(f"Loaded credentials for {len(office_credentials)} offices")
    return credentials


@task(name="process-entity")
async def process_entity_for_offices(
    entity: str,
    start_date: str,
    end_date: str,
    credentials: Dict[str, Any],
    office_ids: Optional[List[str]] = None,
    batch_size: int = 5000
) -> Dict[str, Any]:
    """Process a single entity across specified offices"""
    logger = get_run_logger()
    logger.info(f"Processing entity: {entity}")
    
    entity_config = MULTI_OFFICE_ENTITY_CONFIG[entity]
    requires_multi_office = entity_config.get("requires_multi_office", False)
    
    # Determine which offices to process
    available_offices = list(credentials["offices"].keys())
    
    if not available_offices:
        logger.warning(f"No office credentials available for entity {entity}")
        return {
            "success": False,
            "entity": entity,
            "error": "No office credentials available",
            "total_records": 0,
            "offices_processed": 0,
            "office_results": {}
        }
    
    if requires_multi_office:
        target_offices = office_ids or available_offices
    else:
        # Global entities: use first available office
        target_offices = [available_offices[0]]
    
    # Create office manager and populate it with credentials from pipeline
    office_manager = OfficeManager()
    office_manager.offices = {}  # Clear any auto-loaded credentials
    
    # Populate office manager with credentials from pipeline
    for office_id, office_creds in credentials["offices"].items():
        office_manager.offices[office_id] = OfficeCredentials(
            office_id=office_id,
            office_name=office_creds["office_name"],
            api_key=office_creds["api_key"],
            token=office_creds["token"]
        )
    
    # Create Snowflake configuration for the pipeline
    snowflake_config = credentials["snowflake"]
    
    # Create and execute the entity pipeline
    logger.info(f"Creating MultiOfficeEntityTask for {entity} from {start_date} to {end_date}")
    
    pipeline = MultiOfficeEntityTask(
        entity=entity,
        date_start=start_date,
        date_end=end_date,
        office_manager=office_manager,
        snowflake_config=snowflake_config
    )
    
    # Execute the pipeline
    result = pipeline.execute()
    
    # Transform result to match expected format
    if result.get("success"):
        total_records = result.get("combined_stats", {}).get("total_items", 0)
        offices_processed = result.get("combined_stats", {}).get("successful_offices", 0)
        logger.info(f"✅ {entity}: {total_records} records processed across {offices_processed} offices")
        
        return {
            "success": True,
            "entity": entity,
            "total_records": total_records,
            "offices_processed": offices_processed,
            "office_results": result
        }
    else:
        logger.error(f"❌ {entity}: {result.get('error', 'Unknown error')}")
        return {
            "success": False,
            "entity": entity,
            "error": result.get("error", "Unknown error"),
            "total_records": 0,
            "offices_processed": 0,
            "office_results": result
        }


@task(name="run-staging-transformation")
async def run_staging_transformation(
    entities: List[str],
    credentials: Dict[str, Any],
    batch_id: Optional[str] = None
) -> Dict[str, Any]:
    """Run staging transformations for specified entities"""
    logger = get_run_logger()
    logger.info(f"Running staging transformation for {len(entities)} entities")
    
    # Initialize Snowflake connection
    snowflake_config = credentials["snowflake"]
    snowflake_conn = CustomSnowflakeConnector(
        account=snowflake_config["account"],
        user=snowflake_config["user"],
        password=snowflake_config["password"],
        warehouse=snowflake_config["warehouse"],
        schema=snowflake_config["schema"]
    )
    
    if not snowflake_conn.connect():
        raise Exception("Failed to connect to Snowflake")
    
    try:
        # Create staging processor
        staging_processor = MultiEntityStagingProcessor(snowflake_conn)
        
        # Process each entity
        results = {
            "success": True,
            "entities_processed": [],
            "summary": {
                "total_entities": len(entities),
                "successful": 0,
                "failed": 0
            }
        }
        
        for entity in entities:
            logger.info(f"Processing staging for entity: {entity}")
            
            result = staging_processor.load_entity_to_staging(entity, batch_id)
            results["entities_processed"].append(result)
            
            if result["success"]:
                results["summary"]["successful"] += 1
                logger.info(f"✅ {entity}: {result.get('total_rows_processed', 0)} rows processed")
            else:
                results["summary"]["failed"] += 1
                results["success"] = False
                logger.error(f"❌ {entity}: {result.get('error', 'Unknown error')}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in staging transformation: {e}")
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        snowflake_conn.disconnect()


@flow(name="pestroutes-full-pipeline", log_prints=True)
async def pestroutes_full_pipeline(
    start_date: str,
    end_date: str,
    batch_size: int = 5000,
    max_retries: int = 3,
    run_staging: bool = True,
    entities_to_process: Optional[List[str]] = None,
    offices_to_process: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Full PestRoutes to Snowflake pipeline processing all entities
    
    Args:
        start_date: Start date for data extraction (YYYY-MM-DD)
        end_date: End date for data extraction (YYYY-MM-DD)
        batch_size: Number of records to process per batch
        max_retries: Maximum number of retries for failed operations
        run_staging: Whether to run staging transformations
        entities_to_process: List of specific entities to process (None = all)
        offices_to_process: List of specific office IDs to process (None = all)
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting PestRoutes Full Pipeline")
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    
    # Load credentials
    credentials = await load_credentials()
    
    # Determine entities to process
    target_entities = entities_to_process or list(MULTI_OFFICE_ENTITY_CONFIG.keys())
    logger.info(f"📋 Processing {len(target_entities)} entities")
    
    # Generate batch ID for this run
    batch_id = f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Process each entity
    entity_results = []
    successful_entities = []
    
    for entity in target_entities:
        logger.info(f"🔄 Processing entity: {entity}")
        
        result = await process_entity_for_offices(
            entity=entity,
            start_date=start_date,
            end_date=end_date,
            credentials=credentials,
            office_ids=offices_to_process,
            batch_size=batch_size
        )
        
        entity_results.append(result)
        
        if result["success"]:
            successful_entities.append(entity)
            logger.info(f"✅ {entity}: {result['total_records']} records processed")
        else:
            logger.error(f"❌ {entity}: {result.get('error', 'Unknown error')}")
    
    # Run staging transformations if requested
    staging_result = None
    if run_staging and successful_entities:
        logger.info("🏗️  Running staging transformations...")
        staging_result = await run_staging_transformation(
            entities=successful_entities,
            credentials=credentials,
            batch_id=batch_id
        )
    
    # Compile final results
    pipeline_result = {
        "success": len(successful_entities) > 0,
        "batch_id": batch_id,
        "date_range": f"{start_date} to {end_date}",
        "summary": {
            "total_entities": len(target_entities),
            "successful_entities": len(successful_entities),
            "failed_entities": len(target_entities) - len(successful_entities),
            "total_records": sum(r.get("total_records", 0) for r in entity_results if r.get("success"))
        },
        "entity_results": entity_results,
        "staging_result": staging_result
    }
    
    logger.info(f"🎉 Pipeline completed!")
    logger.info(f"📊 Summary: {pipeline_result['summary']}")
    
    return pipeline_result


@flow(name="pestroutes-incremental-pipeline", log_prints=True)
async def pestroutes_incremental_pipeline(
    hours_lookback: int = 6,
    batch_size: int = 1000,
    max_retries: int = 2,
    run_staging: bool = True
) -> Dict[str, Any]:
    """
    Incremental PestRoutes pipeline for processing recent updates
    
    Args:
        hours_lookback: Number of hours to look back for data
        batch_size: Number of records to process per batch
        max_retries: Maximum number of retries for failed operations
        run_staging: Whether to run staging transformations
    """
    logger = get_run_logger()
    logger.info(f"🔄 Starting PestRoutes Incremental Pipeline")
    logger.info(f"⏰ Looking back {hours_lookback} hours")
    
    # Calculate date range
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours_lookback)
    
    start_date = start_time.strftime('%Y-%m-%d')
    end_date = end_time.strftime('%Y-%m-%d')
    
    # Run full pipeline with calculated dates
    return await pestroutes_full_pipeline(
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        max_retries=max_retries,
        run_staging=run_staging
    )


@flow(name="pestroutes-single-entity-pipeline", log_prints=True)
async def pestroutes_single_entity_pipeline(
    entity: str,
    start_date: str,
    end_date: str,
    office_ids: Optional[List[str]] = None,
    run_staging: bool = True
) -> Dict[str, Any]:
    """
    Single entity pipeline for testing or processing specific entities
    
    Args:
        entity: Entity name to process
        start_date: Start date for data extraction (YYYY-MM-DD)
        end_date: End date for data extraction (YYYY-MM-DD)
        office_ids: List of specific office IDs to process (None = all applicable)
        run_staging: Whether to run staging transformations
    """
    logger = get_run_logger()
    logger.info(f"🎯 Starting Single Entity Pipeline for: {entity}")
    
    # Run full pipeline with single entity
    return await pestroutes_full_pipeline(
        start_date=start_date,
        end_date=end_date,
        entities_to_process=[entity],
        offices_to_process=office_ids,
        run_staging=run_staging
    )


if __name__ == "__main__":
    import asyncio
    
    # Example local run
    result = asyncio.run(pestroutes_single_entity_pipeline(
        entity="customer",
        start_date="2025-06-08",
        end_date="2025-06-09",
        run_staging=True
    ))
    
    print(f"Pipeline result: {result}")