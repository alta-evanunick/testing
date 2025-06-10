#!/usr/bin/env python3
"""
Simple deployment script for Prefect Cloud
Uses the simplified YAML without Jinja2 templates
"""
import subprocess
import sys


def main():
    print("🚀 Deploying PestRoutes Pipeline to Prefect Cloud")
    print("=" * 60)
    
    # Check if authenticated
    print("🔐 Checking Prefect authentication...")
    result = subprocess.run("prefect config view", shell=True, capture_output=True, text=True)
    
    if "PREFECT_API_URL" not in result.stdout or "cloud" not in result.stdout:
        print("❌ Not authenticated with Prefect Cloud")
        print("Please run: prefect cloud login")
        return False
    
    print("✅ Authenticated with Prefect Cloud")
    
    # Deploy using the simplified YAML
    print("\n📦 Deploying flows...")
    cmd = "prefect deploy --all --prefect-file prefect_cloud_simple.yaml"
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, text=True)
        print("✅ Deployment successful!")
        
        print("\n📋 Deployed flows:")
        print("  1. pestroutes-full-pipeline (daily at 2 AM MT)")
        print("  2. pestroutes-incremental (every 4 hours)")
        print("  3. pestroutes-single-entity (manual/testing)")
        
        print("\n🎯 Next steps:")
        print("  1. View in Prefect Cloud UI")
        print("  2. Trigger a test run:")
        print("     prefect deployment run 'pestroutes-full-pipeline/pestroutes-full-pipeline'")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Deployment failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)