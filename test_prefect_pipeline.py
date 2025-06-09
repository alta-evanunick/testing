"""
Local Test Script for Prefect Pipeline
Tests the exact same flows that will run in Prefect Cloud using secret blocks
"""
import asyncio
import sys
import os
from datetime import datetime, timedelta

# Set up test environment variables first
from setup_test_env import setup_test_environment
setup_test_environment()

# Import the flows
from flows.main_pipeline import (
    pestroutes_full_pipeline,
    pestroutes_incremental_pipeline, 
    pestroutes_single_entity_pipeline,
    load_credentials
)


async def test_credentials_loading():
    """Test loading credentials from Prefect secret blocks"""
    print("🔐 Testing credential loading...")
    
    try:
        credentials = await load_credentials()
        
        print(f"✅ Snowflake credentials loaded")
        print(f"✅ Office credentials loaded for {len(credentials['offices'])} offices:")
        
        for office_id, office_data in credentials['offices'].items():
            print(f"   - {office_id}: {office_data['office_name']}")
        
        return credentials
        
    except Exception as e:
        print(f"❌ Error loading credentials: {e}")
        return None


async def test_single_entity():
    """Test single entity pipeline"""
    print("\n🎯 Testing Single Entity Pipeline...")
    
    try:
        result = await pestroutes_single_entity_pipeline(
            entity="customer",
            start_date="2025-06-08", 
            end_date="2025-06-09",
            office_ids=["office_3"],  # Just Spokane for testing
            run_staging=True
        )
        
        if result["success"]:
            print(f"✅ Single entity test successful!")
            print(f"📊 Summary: {result['summary']}")
        else:
            print(f"❌ Single entity test failed")
            
        return result
        
    except Exception as e:
        print(f"❌ Error in single entity test: {e}")
        return None


async def test_incremental_pipeline():
    """Test incremental pipeline"""
    print("\n🔄 Testing Incremental Pipeline...")
    
    try:
        result = await pestroutes_incremental_pipeline(
            hours_lookback=24,  # Look back 24 hours for testing
            batch_size=1000,
            run_staging=True
        )
        
        if result["success"]:
            print(f"✅ Incremental pipeline test successful!")
            print(f"📊 Summary: {result['summary']}")
        else:
            print(f"❌ Incremental pipeline test failed")
            
        return result
        
    except Exception as e:
        print(f"❌ Error in incremental pipeline test: {e}")
        return None


async def test_full_pipeline_subset():
    """Test full pipeline with a subset of entities"""
    print("\n🚀 Testing Full Pipeline (subset)...")
    
    try:
        # Test with just a few entities to validate the full pipeline logic
        test_entities = ["customer", "appointment", "region"]
        
        result = await pestroutes_full_pipeline(
            start_date="2025-06-08",
            end_date="2025-06-09", 
            entities_to_process=test_entities,
            offices_to_process=["office_3"],  # Just Spokane for testing
            batch_size=1000,
            run_staging=True
        )
        
        if result["success"]:
            print(f"✅ Full pipeline subset test successful!")
            print(f"📊 Summary: {result['summary']}")
        else:
            print(f"❌ Full pipeline subset test failed")
            
        return result
        
    except Exception as e:
        print(f"❌ Error in full pipeline subset test: {e}")
        return None


async def test_full_pipeline_complete():
    """Test full pipeline with ALL entities"""
    print("\n🚀 Testing Complete Full Pipeline (ALL 27 entities)...")
    
    try:
        result = await pestroutes_full_pipeline(
            start_date="2025-06-08",
            end_date="2025-06-09", 
            entities_to_process=None,  # Process ALL entities
            offices_to_process=["office_1", "office_3"],  # Seattle and Spokane for testing
            batch_size=1000,
            run_staging=True
        )
        
        if result["success"]:
            print(f"✅ Complete full pipeline test successful!")
            print(f"📊 Summary: {result['summary']}")
            print(f"📊 Processed {result['summary']['total_entities']} entities across {len(result.get('entity_results', []))} entity types")
        else:
            print(f"❌ Complete full pipeline test failed")
            
        return result
        
    except Exception as e:
        print(f"❌ Error in complete full pipeline test: {e}")
        return None


async def run_all_tests():
    """Run all pipeline tests"""
    print("🧪 PREFECT PIPELINE TESTING")
    print("=" * 60)
    print("Testing the exact same flows that will run in Prefect Cloud")
    print()
    
    # Test 1: Credential loading
    credentials = await test_credentials_loading()
    if not credentials:
        print("❌ Cannot proceed without credentials")
        return
    
    # Test 2: Single entity pipeline
    single_result = await test_single_entity()
    
    # Test 3: Incremental pipeline  
    incremental_result = await test_incremental_pipeline()
    
    # Test 4: Full pipeline subset
    full_result = await test_full_pipeline_subset()
    
    # Test 5: Complete full pipeline (all entities)
    complete_result = await test_full_pipeline_complete()
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 TEST SUMMARY")
    print("=" * 60)
    
    tests = [
        ("Credential Loading", credentials is not None),
        ("Single Entity Pipeline", single_result and single_result.get("success")),
        ("Incremental Pipeline", incremental_result and incremental_result.get("success")),
        ("Full Pipeline Subset", full_result and full_result.get("success")),
        ("Complete Full Pipeline", complete_result and complete_result.get("success"))
    ]
    
    for test_name, success in tests:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:25} {status}")
    
    all_passed = all(success for _, success in tests)
    
    if all_passed:
        print("\n🎉 All tests passed! Your pipeline is ready for Prefect deployment.")
        print("\n📋 Next steps:")
        print("   1. Run: prefect deploy --all")
        print("   2. Trigger deployment in Prefect UI or CLI")
        print("   3. Monitor flows in Prefect dashboard")
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
    
    return all_passed


def run_quick_test():
    """Run a quick test with minimal data"""
    print("🏃 Quick Test Mode")
    print("Testing single entity with minimal data...")
    
    return asyncio.run(test_single_entity())


def run_full_test():
    """Run comprehensive tests"""
    print("🧪 Full Test Mode") 
    print("Running comprehensive pipeline tests...")
    
    return asyncio.run(run_all_tests())


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "quick":
            run_quick_test()
        elif sys.argv[1] == "full":
            run_full_test()
        else:
            print("Usage: python test_prefect_pipeline.py [quick|full]")
            print("  quick: Run single entity test")
            print("  full:  Run all comprehensive tests")
    else:
        # Default to quick test
        run_quick_test()