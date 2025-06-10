"""
Office Configuration and Credential Management
Handles multiple office API credentials and entity-specific office requirements
"""
import os
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class OfficeCredentials:
    """Office API credentials"""
    office_id: str
    office_name: str
    api_key: str
    token: str


class OfficeManager:
    """Manages multiple office credentials and determines which entities need multi-office processing"""
    
    def __init__(self, load_from_env: bool = True):
        self.offices = {}
        if load_from_env:
            self._load_office_credentials()
    
    def _load_office_credentials(self):
        """Load office credentials from environment variables"""
        # Load ALL offices from environment variables (1-19)
        # Pattern: PESTROUTES_OFFICE_N_API_KEY and PESTROUTES_OFFICE_N_TOKEN
        for i in range(1, 20):  # offices 1-19
            key_env = f"PESTROUTES_OFFICE_{i}_API_KEY"
            token_env = f"PESTROUTES_OFFICE_{i}_TOKEN"
            name_env = f"PESTROUTES_OFFICE_{i}_NAME"
            
            api_key = os.getenv(key_env)
            token = os.getenv(token_env)
            office_name = os.getenv(name_env, f"Office {i}")
            
            if api_key and token:
                office_id = f"office_{i}"
                self.offices[office_id] = OfficeCredentials(
                    office_id=office_id,
                    office_name=office_name,
                    api_key=api_key,
                    token=token
                )
                print(f"✅ Loaded credentials for {office_name} ({office_id})")
            else:
                print(f"⚠️  No credentials found for {key_env}/{token_env}")
    
    def get_office_credentials(self, office_id: str) -> Optional[OfficeCredentials]:
        """Get credentials for a specific office"""
        return self.offices.get(office_id)
    
    def get_all_offices(self) -> List[OfficeCredentials]:
        """Get all loaded office credentials"""
        return list(self.offices.values())
    
    def get_office_count(self) -> int:
        """Get number of loaded offices"""
        return len(self.offices)
    
    def list_offices(self):
        """Print all loaded offices"""
        print(f"\n📋 LOADED OFFICES ({len(self.offices)}):")
        print("-" * 50)
        for office in self.offices.values():
            print(f"   {office.office_id}: {office.office_name}")


# Entity configuration with office requirements
# 
# TO ADD NEW ENTITIES:
# 1. Add configuration block below
# 2. Set requires_multi_office: False (global) or True (office-specific)
# 3. Define date_fields to search by
# 4. Specify API field names (id_field, id_name) 
# 5. Set table_name for Snowflake
# 6. Add staging configuration in multi_entity_staging.py if needed
#
MULTI_OFFICE_ENTITY_CONFIG = {
    # Global entities
    "disbursementItem": {
        "requires_multi_office": False,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "gatewayDisbursementEntryID",
        "id_name": "gatewayDisbursementEntryIDs",
        "table_name": "DISBURSEMENTITEM_FACT"
    },
    "cancellationReason": {
        "requires_multi_office": False,  
        "date_fields": [],
        "id_field": "cancellationReasonID",
        "id_name": "cancellationReasonIDs",
        "table_name": "CANCELLATIONREASON_DIM"
    },
    "reserviceReason": {
        "requires_multi_office": False,  
        "date_fields": [],
        "id_field": "reserviceReasonID",
        "id_name": "reserviceReasonIDs",
        "table_name": "RESERVICEREASON_DIM"
    },
    "customerSource": {
        "requires_multi_office": False,  
        "date_fields": [],
        "id_field": "customerSourceID",
        "id_name": "customerSourceIDs",
        "table_name": "CUSTOMERSOURCE_DIM"
    },
    # Office-specific entities
    "customer": {
        "requires_multi_office": True,   
        "date_fields": ["dateUpdated", "dateAdded"],
        "id_field": "customerID",
        "id_name": "customerIDs", 
        "table_name": "CUSTOMER_FACT"
    },
    "appointment": {
        "requires_multi_office": True,   
        "date_fields": ["dateUpdated", "dateAdded"],
        "id_field": "appointmentID",
        "id_name": "appointmentIDs",
        "table_name": "APPOINTMENT_FACT"
    },
    "route": {
        "requires_multi_office": True,
        "date_fields": ["dateUpdated", "date"],
        "id_field": "routeID", 
        "id_name": "routeIDs",
        "table_name": "ROUTE_FACT"
    },
    "region": {
        "requires_multi_office": True,  
        "date_fields": [],
        "id_field": "regionID",
        "id_name": "regionIDs",
        "table_name": "REGION_DIM"
    },
    "serviceType": {
        "requires_multi_office": True,  
        "date_fields": [],
        "id_field": "serviceTypeID",
        "id_name": "serviceTypeIDs",
        "table_name": "SERVICETYPE_DIM"
    },
    "genericFlag": {
        "requires_multi_office": True,  
        "date_fields": [],
        "id_field": "genericFlagID",
        "id_name": "genericFlagIDs",
        "table_name": "GENERICFLAG_DIM"
    },
    "product": {
        "requires_multi_office": True,  
        "date_fields": [],
        "id_field": "productID",
        "id_name": "productIDs",
        "table_name": "PRODUCT_DIM"
    },
    "employee": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated"],
        "id_field": "employeeID",
        "id_name": "employeeIDs",
        "table_name": "EMPLOYEE_FACT"
    },
    "subscription": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateAdded"],
        "id_field": "subscriptionID",
        "id_name": "subscriptionIDs",
        "table_name": "SUBSCRIPTION_FACT"
    },
    "ticket": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "ticketID",
        "id_name": "ticketIDs",
        "table_name": "TICKET_FACT"
    },
    "ticketItem": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "ticketItemID",
        "id_name": "ticketItemIDs",
        "table_name": "TICKETITEM_FACT"
    },
    "payment": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "date"],
        "id_field": "paymentID",
        "id_name": "paymentIDs",
        "table_name": "PAYMENT_FACT"
    },
    "appliedPayment": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateApplied"],
        "id_field": "appliedPaymentID",
        "id_name": "appliedPaymentIDs",
        "table_name": "APPLIEDPAYMENT_FACT"
    },
    "note": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateAdded"],
        "id_field": "noteID",
        "id_name": "noteIDs",
        "table_name": "NOTE_FACT"
    },
    "task": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateAdded"],
        "id_field": "taskID",
        "id_name": "taskIDs",
        "table_name": "TASK_FACT"
    },
    "door": {
        "requires_multi_office": True,  
        "date_fields": ["timeCreated"],
        "id_field": "doorID",
        "id_name": "doorIDs",
        "table_name": "DOOR_FACT"
    },
    "disbursement": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "gatewayDisbursementID",
        "id_name": "gatewayDisbursementIDs",
        "table_name": "DISBURSEMENT_FACT"
    },
    "chargeback": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "chargebackID",
        "id_name": "chargebackIDs",
        "table_name": "CHARGEBACK_FACT"
    },
    "additionalContacts": {
        "requires_multi_office": True,  
        "date_fields": [],
        "id_field": "additionalContactID",
        "id_name": "additionalContactIDs",
        "table_name": "ADDITIONALCONTACT_FACT"
    },
    "genericFlagAssignment": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "genericFlagAssignmentID",
        "id_name": "genericFlagAssignmentIDs",
        "table_name": "GENERICFLAGASSIGNMENT_FACT"
    },
    "knock": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateAdded"],
        "id_field": "knockID",
        "id_name": "knockIDs",
        "table_name": "KNOCK_FACT"
    },
    "paymentProfile": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated", "dateCreated"],
        "id_field": "paymentProfileID",
        "id_name": "paymentProfileIDs",
        "table_name": "PAYMENTPROFILE_FACT"
    },
    "appointmentReminder": {
        "requires_multi_office": True,  
        "date_fields": ["dateUpdated"],
        "id_field": "appointmentReminderID",
        "id_name": "appointmentReminderIDs",
        "table_name": "APPOINTMENTREMINDER_FACT"
    }
}


# Example of adding a new entity:
# "newEntity": {
#     "requires_multi_office": True,   # Office-specific
#     "date_fields": ["dateUpdated", "dateCreated"],
#     "id_field": "newEntityID",
#     "id_name": "newEntityIDs",
#     "table_name": "NEWENTITY_FACT"
# }


def get_entities_requiring_multi_office() -> List[str]:
    """Get list of entities that require multi-office processing"""
    return [
        entity for entity, config in MULTI_OFFICE_ENTITY_CONFIG.items()
        if config.get("requires_multi_office", False)
    ]


def get_global_entities() -> List[str]:
    """Get list of entities that are processed globally (single office)"""
    return [
        entity for entity, config in MULTI_OFFICE_ENTITY_CONFIG.items()
        if not config.get("requires_multi_office", False)
    ]


def create_env_template():
    """Create a template .env file for all office credentials"""
    template = """# PestRoutes Office Credentials
# Copy this template and fill in your actual credentials

# Primary Office (Alta Pest Control - Main)
PESTROUTES_OFFICE_1_API_KEY=784onqelmht4hrt9i2p15novnja792d8btfjevgmassojvdao0f1d1akueifc3dj
PESTROUTES_OFFICE_1_TOKEN=7q23r7k2ta6r4s5rfim4v61p23r40jgmchkokr7mditft3r6mbbqa1df3bjcf49m
PESTROUTES_OFFICE_1_NAME=Alta Pest Control - Main

# Additional Offices (fill in as needed)
# PESTROUTES_OFFICE_2_API_KEY=your_office_2_api_key_here
# PESTROUTES_OFFICE_2_TOKEN=your_office_2_token_here
# PESTROUTES_OFFICE_2_NAME=Office 2 Name

# PESTROUTES_OFFICE_3_API_KEY=your_office_3_api_key_here
# PESTROUTES_OFFICE_3_TOKEN=your_office_3_token_here
# PESTROUTES_OFFICE_3_NAME=Office 3 Name

# ... continue for offices 4-18 as needed

# Snowflake Credentials
SNOWFLAKE_ACCOUNT=AUEDKKB-TH88792
SNOWFLAKE_USER=EVANUNICK
SNOWFLAKE_PASSWORD=your_snowflake_password_here
SNOWFLAKE_WAREHOUSE=ALTAPESTANALYTICS
"""
    
    with open('.env.template', 'w') as f:
        f.write(template)
    
    print("📝 Created .env.template file - copy to .env and fill in your credentials")


if __name__ == "__main__":
    # Test the office manager
    print("🏢 TESTING OFFICE MANAGER")
    print("=" * 50)
    
    office_manager = OfficeManager()
    office_manager.list_offices()
    
    print(f"\n📊 ENTITY CONFIGURATION:")
    print("-" * 30)
    print(f"Multi-office entities: {get_entities_requiring_multi_office()}")
    print(f"Global entities: {get_global_entities()}")
    
    # Create template file
    print(f"\n📝 ENVIRONMENT SETUP:")
    print("-" * 30)
    create_env_template()