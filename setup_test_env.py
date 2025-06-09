#!/usr/bin/env python3
"""
Setup script for test environment
Sets up environment variables for local testing when secret blocks aren't available
"""
import os

def setup_test_environment():
    """Set up test environment variables for local pipeline testing"""
    print("🔧 Setting up test environment variables...")
    
    # Test Snowflake credentials (using the same values as fallback)
    test_env = {
        "SNOWFLAKE_ACCOUNT": "AUEDKKB-TH88792",
        "SNOWFLAKE_USER": "EVANUNICK", 
        "SNOWFLAKE_PASSWORD": "SnowflakePword1!",
        "SNOWFLAKE_WAREHOUSE": "ALTAPESTANALYTICS",
        
        # Test office credentials (dummy values for testing)
        "PESTROUTES_OFFICE_3_API_KEY": "test_spokane_key",
        "PESTROUTES_OFFICE_3_TOKEN": "test_spokane_token",
        "PESTROUTES_OFFICE_3_NAME": "Spokane Test Office",
        
        "PESTROUTES_OFFICE_1_API_KEY": "test_seattle_key", 
        "PESTROUTES_OFFICE_1_TOKEN": "test_seattle_token",
        "PESTROUTES_OFFICE_1_NAME": "Seattle Test Office",
    }
    
    # Set environment variables
    for key, value in test_env.items():
        os.environ[key] = value
        print(f"   ✅ Set {key}")
    
    print(f"✅ Test environment setup complete! {len(test_env)} variables set.")
    return test_env

if __name__ == "__main__":
    setup_test_environment()