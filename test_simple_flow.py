"""
Simple test flow to verify Prefect deployment works
"""
from prefect import flow

@flow(name="simple-test-flow")
def simple_test():
    """Simple test flow"""
    print("✅ Simple test flow executed successfully!")
    return {"success": True, "message": "Test completed"}

if __name__ == "__main__":
    simple_test()