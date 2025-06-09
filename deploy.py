"""
Prefect Deployment Script
Handles deployment setup and management for the PestRoutes pipeline
"""
import subprocess
import sys
import os
from typing import List


def run_command(command: str, description: str = None) -> bool:
    """Run a shell command and return success status"""
    if description:
        print(f"📝 {description}")
    
    print(f"💻 Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        return False


def check_prefect_auth():
    """Check if user is authenticated with Prefect Cloud"""
    print("🔐 Checking Prefect authentication...")
    
    result = subprocess.run("prefect config view", shell=True, capture_output=True, text=True)
    
    if "PREFECT_API_URL" in result.stdout and "cloud" in result.stdout:
        print("✅ Connected to Prefect Cloud")
        return True
    else:
        print("⚠️  Not connected to Prefect Cloud")
        print("Please run: prefect cloud login")
        return False


def check_secret_blocks():
    """Check if required secret blocks exist"""
    print("🔍 Checking for required secret blocks...")
    
    required_blocks = [
        "snowflake-altapestanalytics",
        "github-repo", 
        "github-evanunick"
    ]
    
    # Office-specific blocks
    offices = [
        "seattle", "spokane", "okc", "tulsa", "wichita", "austin", 
        "dc", "dallas", "lasvegas", "nashville", "virginiabeach",
        "kansascity", "charlotte", "sanantonio", "knoxville", 
        "ftworth", "tricities"
    ]
    
    for office in offices:
        required_blocks.extend([
            f"fieldroutes-{office}-auth-key",
            f"fieldroutes-{office}-auth-token"
        ])
    
    missing_blocks = []
    
    for block in required_blocks:
        # Check if block exists
        result = subprocess.run(
            f"prefect block ls | grep {block}", 
            shell=True, 
            capture_output=True, 
            text=True
        )
        
        if result.returncode != 0:
            missing_blocks.append(block)
    
    if missing_blocks:
        print(f"❌ Missing {len(missing_blocks)} secret blocks:")
        for block in missing_blocks[:10]:  # Show first 10
            print(f"   - {block}")
        if len(missing_blocks) > 10:
            print(f"   ... and {len(missing_blocks) - 10} more")
        return False
    else:
        print(f"✅ All {len(required_blocks)} secret blocks found")
        return True


def deploy_flows():
    """Deploy flows to Prefect Cloud"""
    print("🚀 Deploying flows to Prefect Cloud...")
    
    # Deploy all flows defined in prefect.yaml
    if not run_command("prefect deploy --all", "Deploying all flows"):
        return False
    
    print("✅ Flows deployed successfully!")
    return True


def test_deployment():
    """Test the deployment by running a quick flow"""
    print("🧪 Testing deployment...")
    
    # Run a quick test flow
    command = (
        "prefect deployment run "
        "'pestroutes-single-entity-pipeline/pestroutes-single-entity' "
        "--param entity=customer "
        "--param start_date=2025-06-08 "
        "--param end_date=2025-06-09 "
        "--param office_ids='[\"office_3\"]' "
        "--param run_staging=false"
    )
    
    if run_command(command, "Running test deployment"):
        print("✅ Test deployment successful!")
        return True
    else:
        print("❌ Test deployment failed")
        return False


def setup_work_pool():
    """Setup work pool for deployments"""
    print("🏊 Setting up work pool...")
    
    # Create default work pool if it doesn't exist
    if run_command(
        "prefect work-pool create default-work-pool --type process", 
        "Creating default work pool"
    ):
        print("✅ Work pool created/confirmed")
        return True
    else:
        # Pool might already exist, check
        result = subprocess.run(
            "prefect work-pool ls | grep default-work-pool", 
            shell=True, 
            capture_output=True
        )
        if result.returncode == 0:
            print("✅ Work pool already exists")
            return True
        else:
            print("❌ Could not create or find work pool")
            return False


def main():
    """Main deployment process"""
    print("🎯 PREFECT DEPLOYMENT SETUP")
    print("=" * 60)
    print("Setting up PestRoutes to Snowflake pipeline deployment")
    print()
    
    steps = [
        ("Check Prefect Authentication", check_prefect_auth),
        ("Check Secret Blocks", check_secret_blocks),
        ("Setup Work Pool", setup_work_pool),
        ("Deploy Flows", deploy_flows)
    ]
    
    for step_name, step_func in steps:
        print(f"\n📋 Step: {step_name}")
        print("-" * 40)
        
        if not step_func():
            print(f"\n❌ Deployment failed at step: {step_name}")
            print("Please resolve the issues above and try again.")
            return False
    
    print("\n" + "=" * 60)
    print("🎉 DEPLOYMENT SUCCESSFUL!")
    print("=" * 60)
    
    print("\n📋 Your deployments are now available:")
    print("   1. pestroutes-full-pipeline - Complete daily pipeline")
    print("   2. pestroutes-incremental - Incremental 4-hour pipeline") 
    print("   3. pestroutes-single-entity - Testing/ad-hoc pipeline")
    
    print("\n🎮 Next steps:")
    print("   1. View deployments: prefect deployment ls")
    print("   2. Start a worker: prefect worker start --pool default-work-pool")
    print("   3. Trigger a run: prefect deployment run <deployment-name>")
    print("   4. Monitor in Prefect UI: https://app.prefect.cloud")
    
    # Ask if user wants to run a test
    try:
        test_choice = input("\n🧪 Would you like to run a test deployment? (y/n): ").lower().strip()
        if test_choice in ['y', 'yes']:
            test_deployment()
    except KeyboardInterrupt:
        print("\nSkipping test.")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)