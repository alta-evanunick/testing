"""
Multi-Office PestRoutes API Client
Handles multiple office credentials and entity-specific office requirements
"""
import requests
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from office_config import OfficeManager, OfficeCredentials, MULTI_OFFICE_ENTITY_CONFIG

# Import Snowflake integration if available
try:
    from snowflake_integration import SnowflakeConnector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("Warning: Snowflake integration not available. Install snowflake-connector-python to enable.")


class MultiOfficePestRoutesAPI:
    """Enhanced API client that handles multiple office credentials"""
    
    def __init__(self, office_credentials: OfficeCredentials):
        self.office = office_credentials
        self.base_url = "https://alta.pestroutes.com/api"
        self.headers = {
            "authenticationToken": self.office.token,
            "authenticationKey": self.office.api_key,
        }
        # API call tracking
        self.api_call_stats = {
            "search_calls": 0,
            "get_calls": 0,
            "total_calls": 0,
            "failed_calls": 0
        }
    
    def search_entity(self, entity: str, date_start: str, date_end: str, date_field: str) -> Dict[str, Any]:
        """Search any entity by date range with pagination support"""
        if entity not in MULTI_OFFICE_ENTITY_CONFIG:
            return {"error": f"Entity '{entity}' not supported. Available: {list(MULTI_OFFICE_ENTITY_CONFIG.keys())}"}
        
        config = MULTI_OFFICE_ENTITY_CONFIG[entity]
        id_field = config["id_field"]
        all_ids = []
        last_id = None
        page_num = 1
        
        while True:
            # Build filters
            filters = {
                date_field: {
                    "operator": "BETWEEN",
                    "value": [date_start, date_end]
                }
            }
            
            # Add primary key filter for pagination
            if last_id is not None:
                filters[id_field] = {
                    "operator": ">",
                    "value": last_id
                }
            
            params = {}
            for key, value in filters.items():
                params[key] = json.dumps(value)
            
            url = f"{self.base_url}/{entity}/search"
            
            try:
                print(f"[{self.office.office_id}] Searching {entity} - Page {page_num} (after ID: {last_id})...")
                self.api_call_stats["search_calls"] += 1
                self.api_call_stats["total_calls"] += 1
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                result = response.json()
                
                if not result.get("success"):
                    self.api_call_stats["failed_calls"] += 1
                    print(f"[{self.office.office_id}] Search failed: {result.get('errorMessage', 'Unknown error')}")
                    break
                
                # Get IDs from this page
                id_key = f"{entity}IDs"
                page_ids = result.get(id_key, [])
                all_ids.extend(page_ids)
                
                print(f"  [{self.office.office_id}] Page {page_num}: Retrieved {len(page_ids)} {entity} IDs (Total: {len(all_ids)})")
                
                # Check if we need to continue pagination
                if len(page_ids) < 50000:
                    break
                else:
                    last_id = str(page_ids[-1])
                    page_num += 1
                    
            except requests.exceptions.RequestException as e:
                self.api_call_stats["failed_calls"] += 1
                print(f"[{self.office.office_id}] API request failed: {e}")
                return {"error": str(e)}
        
        return {
            "success": True,
            "office_id": self.office.office_id,
            "entity": entity,
            "date_field": date_field,
            "count": len(all_ids),
            f"{entity}IDs": all_ids,
            "totalPages": page_num,
            "api_call_stats": self.api_call_stats.copy()
        }
    
    def get_entity_details(self, entity: str, item_ids: list) -> Dict[str, Any]:
        """Get entity details by IDs with batching"""
        if entity not in MULTI_OFFICE_ENTITY_CONFIG:
            return {"error": f"Entity '{entity}' not supported. Available: {list(MULTI_OFFICE_ENTITY_CONFIG.keys())}"}
        
        config = MULTI_OFFICE_ENTITY_CONFIG[entity]
        id_name = config["id_name"]
        batch_size = 1000
        all_results = []
        total_batches = (len(item_ids) + batch_size - 1) // batch_size
        
        print(f"[{self.office.office_id}] Processing {len(item_ids)} {entity} IDs in {total_batches} batches...")
        
        for i in range(0, len(item_ids), batch_size):
            batch_ids = item_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            url = f"{self.base_url}/{entity}/get"
            data = {id_name: batch_ids}
            
            try:
                print(f"  [{self.office.office_id}] Fetching {entity} batch {batch_num}/{total_batches} ({len(batch_ids)} IDs)...")
                self.api_call_stats["get_calls"] += 1
                self.api_call_stats["total_calls"] += 1
                response = requests.post(url, headers=self.headers, json=data)
                response.raise_for_status()
                result = response.json()
                
                if result.get("success"):
                    entity_key = f"{entity}s"
                    if entity_key in result:
                        # Add office identification to each record
                        for record in result[entity_key]:
                            record["_office_id"] = self.office.office_id
                            record["_office_name"] = self.office.office_name
                        
                        all_results.extend(result[entity_key])
                        print(f"    [{self.office.office_id}] Batch {batch_num}: Retrieved {len(result[entity_key])} {entity}s")
                    else:
                        print(f"    [{self.office.office_id}] Batch {batch_num}: No data key '{entity_key}' found")
                else:
                    self.api_call_stats["failed_calls"] += 1
                    print(f"    [{self.office.office_id}] Batch {batch_num}: Failed - {result.get('errorMessage', 'Unknown error')}")
                    
            except requests.exceptions.RequestException as e:
                self.api_call_stats["failed_calls"] += 1
                print(f"    [{self.office.office_id}] Batch {batch_num}: Request failed - {e}")
                return {"error": str(e), "batch": batch_num}
        
        return {
            "success": True,
            "office_id": self.office.office_id,
            "entity": entity,
            "total_batches": total_batches,
            "total_items": len(all_results),
            f"{entity}s": all_results,
            "api_call_stats": self.api_call_stats.copy()
        }


class MultiOfficeEntityTask:
    """Task to fetch entity data across multiple offices as needed"""
    
    def __init__(self, entity: str, date_start: str, date_end: str, 
                 office_manager: OfficeManager, snowflake_config: Dict[str, str] = None):
        self.entity = entity
        self.date_start = date_start
        self.date_end = date_end
        self.office_manager = office_manager
        self.snowflake_config = snowflake_config
    
    def execute(self) -> Dict[str, Any]:
        """Execute entity extraction based on office requirements"""
        if self.entity not in MULTI_OFFICE_ENTITY_CONFIG:
            return {
                "success": False,
                "error": f"Entity '{self.entity}' not supported. Available: {list(MULTI_OFFICE_ENTITY_CONFIG.keys())}"
            }
        
        config = MULTI_OFFICE_ENTITY_CONFIG[self.entity]
        requires_multi_office = config.get("requires_multi_office", False)
        
        if requires_multi_office:
            return self._execute_multi_office()
        else:
            return self._execute_single_office()
    
    def _execute_single_office(self) -> Dict[str, Any]:
        """Execute for global entities (single office)"""
        print(f"\n🌍 GLOBAL ENTITY: {self.entity} (using first working office)")
        
        # Find first working office (skip office_1 if it has credential issues)
        offices = self.office_manager.get_all_offices()
        if not offices:
            return {"success": False, "error": "No office credentials available"}
        
        # Try to find a working office (prefer non-office_1 due to credential issues)
        primary_office = None
        for office in offices:
            if office.office_id != "office_1":  # Skip office_1 due to credential issues
                primary_office = office
                break
        
        # If no non-office_1 found, fall back to first office
        if primary_office is None:
            primary_office = offices[0]
        
        print(f"   Using {primary_office.office_name} ({primary_office.office_id})")
        api_client = MultiOfficePestRoutesAPI(primary_office)
        
        return self._process_entity_for_office(api_client, primary_office)
    
    def _execute_multi_office(self) -> Dict[str, Any]:
        """Execute for office-specific entities (all offices)"""
        offices = self.office_manager.get_all_offices()
        office_count = len(offices)
        
        print(f"\n🏢 MULTI-OFFICE ENTITY: {self.entity} (processing {office_count} offices)")
        
        combined_results = {
            "success": True,
            "entity": self.entity,
            "office_count": office_count,
            "office_results": [],
            "combined_stats": {
                "total_ids": 0,
                "total_items": 0,
                "successful_offices": 0,
                "failed_offices": 0
            },
            "api_call_stats": {
                "total_search_calls": 0,
                "total_get_calls": 0,
                "total_api_calls": 0,
                "total_failed_calls": 0
            }
        }
        
        for i, office in enumerate(offices, 1):
            print(f"\n  📍 Processing Office {i}/{office_count}: {office.office_name}")
            print(f"      Office ID: {office.office_id}")
            
            api_client = MultiOfficePestRoutesAPI(office)
            office_result = self._process_entity_for_office(api_client, office)
            
            combined_results["office_results"].append(office_result)
            
            # Aggregate API call stats
            if "api_call_stats" in office_result:
                office_stats = office_result["api_call_stats"]
                combined_results["api_call_stats"]["total_search_calls"] += office_stats.get("search_calls", 0)
                combined_results["api_call_stats"]["total_get_calls"] += office_stats.get("get_calls", 0)
                combined_results["api_call_stats"]["total_api_calls"] += office_stats.get("total_calls", 0)
                combined_results["api_call_stats"]["total_failed_calls"] += office_stats.get("failed_calls", 0)
            
            if office_result.get("success"):
                combined_results["combined_stats"]["successful_offices"] += 1
                combined_results["combined_stats"]["total_ids"] += office_result.get("combined_count", 0)
                combined_results["combined_stats"]["total_items"] += len(office_result.get("items", []))
            else:
                combined_results["combined_stats"]["failed_offices"] += 1
                combined_results["success"] = False
        
        return combined_results
    
    def _process_entity_for_office(self, api_client: MultiOfficePestRoutesAPI, office: OfficeCredentials) -> Dict[str, Any]:
        """Process a single entity for a single office"""
        config = MULTI_OFFICE_ENTITY_CONFIG[self.entity]
        date_fields = config["date_fields"]
        
        # Search by each date field
        search_results = {}
        combined_ids = set()
        total_api_calls = 0
        total_search_calls = 0
        total_get_calls = 0
        total_failed_calls = 0
        
        for date_field in date_fields:
            result = api_client.search_entity(
                self.entity, self.date_start, self.date_end, date_field
            )
            
            # Aggregate API call stats
            if "api_call_stats" in result:
                stats = result["api_call_stats"]
                total_search_calls += stats.get("search_calls", 0)
                total_api_calls += stats.get("total_calls", 0)
                total_failed_calls += stats.get("failed_calls", 0)
            
            search_results[date_field] = {
                "count": result.get("count", 0),
                "success": result.get("success", False),
                "api_calls": result.get("api_call_stats", {}).get("search_calls", 0)
            }
            
            if result.get("success") and f"{self.entity}IDs" in result:
                combined_ids.update(result[f"{self.entity}IDs"])
        
        # Convert to sorted list
        combined_id_list = sorted(list(combined_ids))
        
        # Fetch full item details
        items_result = {}
        if combined_id_list:
            # Reset API client stats for GET phase
            api_client.api_call_stats = {
                "search_calls": total_search_calls,
                "get_calls": 0,
                "total_calls": total_api_calls,
                "failed_calls": total_failed_calls
            }
            
            items_result = api_client.get_entity_details(self.entity, combined_id_list)
            
            # Update totals with GET stats
            if "api_call_stats" in items_result:
                final_stats = items_result["api_call_stats"]
                total_get_calls = final_stats.get("get_calls", 0)
                total_api_calls = final_stats.get("total_calls", 0)
                total_failed_calls = final_stats.get("failed_calls", 0)
        
        # Push to Snowflake if configured
        snowflake_status = None
        if self.snowflake_config and SNOWFLAKE_AVAILABLE and items_result.get("success"):
            entity_data = items_result.get(f"{self.entity}s", [])
            snowflake_status = self._push_to_snowflake(entity_data, office)
        
        return {
            "success": True,
            "entity": self.entity,
            "office_id": office.office_id,
            "office_name": office.office_name,
            "date_range": {
                "start": self.date_start,
                "end": self.date_end
            },
            "search_results": search_results,
            "combined_count": len(combined_id_list),
            f"{self.entity}IDs": combined_id_list,
            "items": items_result.get(f"{self.entity}s", []) if items_result.get("success") else [],
            "fetch_details": items_result,
            "snowflake_status": snowflake_status,
            "api_call_stats": {
                "search_calls": total_search_calls,
                "get_calls": total_get_calls,
                "total_calls": total_api_calls,
                "failed_calls": total_failed_calls
            }
        }
    
    def _push_to_snowflake(self, items: List[Dict[str, Any]], office: OfficeCredentials) -> Dict[str, Any]:
        """Push items to Snowflake RAW database"""
        try:
            from snowflake_integration import SnowflakeConnector
            
            # Connect to Snowflake
            snowflake_conn = SnowflakeConnector(
                account=self.snowflake_config["account"].replace(".snowflakecomputing.com", ""),
                user=self.snowflake_config["user"],
                password=self.snowflake_config["password"],
                database=self.snowflake_config.get("database", "RAW_DB_DEV"),
                schema=self.snowflake_config.get("schema", "FIELDROUTES"),
                warehouse=self.snowflake_config.get("warehouse", "ALTAPESTANALYTICS")
            )
            
            if not snowflake_conn.connect():
                return {"success": False, "error": "Failed to connect to Snowflake"}
            
            # Load data using enhanced loader
            loader = MultiOfficeSnowflakeLoader(snowflake_conn)
            config = MULTI_OFFICE_ENTITY_CONFIG[self.entity]
            success = loader.load_entity_data(
                self.entity,
                config["table_name"],
                items,
                {"start": self.date_start, "end": self.date_end},
                office
            )
            
            snowflake_conn.disconnect()
            
            return {
                "success": success,
                "records_loaded": len(items) if success else 0,
                "table": config["table_name"],
                "office_id": office.office_id
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "office_id": office.office_id
            }


class MultiOfficeSnowflakeLoader:
    """Enhanced loader that handles office identification"""
    
    def __init__(self, snowflake_conn: SnowflakeConnector):
        self.snowflake = snowflake_conn
    
    def load_entity_data(self, entity: str, table_name: str, items: List[Dict[str, Any]], 
                        date_range: Dict[str, str], office: OfficeCredentials) -> bool:
        """Load entity data with office identification"""
        
        if not self.snowflake.create_raw_table_if_not_exists(table_name):
            return False
        
        # Generate batch ID with office and date range info
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_id += f"_{office.office_id}_{date_range['start']}_{date_range['end']}"
        
        # Insert in chunks
        chunk_size = 5000
        total_inserted = 0
        
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            inserted = self.snowflake.insert_raw_json_batch(
                table_name, 
                chunk,
                batch_id=batch_id,
                source_file=f"pestroutes_api_{entity}_{office.office_id}"
            )
            total_inserted += inserted
            
            self.snowflake.commit()
            print(f"  [{office.office_id}] Progress: {total_inserted}/{len(items)} {entity} records loaded")
        
        print(f"  [{office.office_id}] Successfully loaded {total_inserted} {entity} records to Snowflake")
        return total_inserted == len(items)


class MultiOfficeFlow:
    """Flow to orchestrate multi-office entity processing"""
    
    def __init__(self, name: str, office_manager: OfficeManager):
        self.name = name
        self.office_manager = office_manager
        self.tasks = []
    
    def add_entity_task(self, entity: str, date_start: str, date_end: str, 
                       snowflake_config: Dict[str, str] = None):
        """Add an entity task to the flow"""
        task = MultiOfficeEntityTask(entity, date_start, date_end, self.office_manager, snowflake_config)
        self.tasks.append(task)
    
    def execute(self) -> Dict[str, Any]:
        """Execute all entity tasks with appropriate office handling"""
        results = {
            "flow_name": self.name,
            "office_count": self.office_manager.get_office_count(),
            "task_results": []
        }
        
        for i, task in enumerate(self.tasks):
            print(f"\n{'='*80}")
            print(f"EXECUTING TASK {i+1}/{len(self.tasks)}: {task.entity.upper()}")
            print(f"{'='*80}")
            
            result = task.execute()
            results["task_results"].append(result)
        
        return results


if __name__ == "__main__":
    # Example usage - requires environment variables to be set
    from office_config import OfficeManager
    import os
    
    # Load office manager
    office_manager = OfficeManager()
    office_manager.list_offices()
    
    if office_manager.get_office_count() == 0:
        print("\n❌ No office credentials loaded!")
        print("   Set environment variables or create .env file")
        exit(1)
    
    # Snowflake configuration from environment
    snowflake_config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "AUEDKKB-TH88792"),
        "user": os.getenv("SNOWFLAKE_USER", "EVANUNICK"),
        "password": os.getenv("SNOWFLAKE_PASSWORD", "SnowflakePword1!"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ALTAPESTANALYTICS")
    }
    
    # Create multi-office flow
    flow = MultiOfficeFlow("Multi-Office Data Pipeline", office_manager)
    
    # Add entities (global and multi-office)
    flow.add_entity_task("disbursementItem", "2025-06-05", "2025-06-06", snowflake_config)
    flow.add_entity_task("customer", "2025-06-05", "2025-06-06", snowflake_config)
    
    # Execute
    results = flow.execute()
    
    print(f"\n{'='*80}")
    print("MULTI-OFFICE PIPELINE COMPLETE")
    print(f"{'='*80}")