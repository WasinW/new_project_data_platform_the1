"""
Dataplex management utility for data lake, zones, and asset creation and tracking.
"""

import logging
from typing import Dict, Any, List, Optional
from google.cloud import dataplex_v1
from google.api_core import exceptions
import time


class DataplexManager:
    """Manager for Dataplex lakes, zones, and assets"""
    
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.dataplex_client = dataplex_v1.DataplexServiceClient()
        
        self.parent = f"projects/{project_id}/locations/{location}"
        logging.info(f"Initialized DataplexManager for project {project_id}, location {location}")
    
    def create_data_lake(self, lake_config: Dict[str, Any]) -> str:
        """
        Create a Dataplex data lake
        
        Args:
            lake_config: Configuration for the data lake
            
        Returns:
            Full name of the created lake
        """
        lake_id = lake_config['lake_id']
        lake_name = f"{self.parent}/lakes/{lake_id}"
        
        try:
            # Check if lake already exists
            self.dataplex_client.get_lake(name=lake_name)
            logging.info(f"Data lake {lake_id} already exists")
            return lake_name
            
        except exceptions.NotFound:
            # Create new lake
            lake = dataplex_v1.Lake(
                display_name=lake_config.get('lake_display_name', lake_id),
                description=f"Data lake for domain: {lake_config.get('domain', 'unknown')}",
                labels={
                    "environment": lake_config.get('environment', 'dev'),
                    "domain": lake_config.get('domain', 'unknown'),
                    "created_by": "data-platform-pipeline"
                }
            )
            
            operation = self.dataplex_client.create_lake(
                parent=self.parent,
                lake_id=lake_id,
                lake=lake
            )
            
            # Wait for operation to complete
            result = operation.result(timeout=300)
            logging.info(f"Created data lake: {result.name}")
            return result.name
    
    def create_data_zone(self, lake_id: str, zone_config: Dict[str, Any]) -> str:
        """
        Create a Dataplex zone within a lake
        
        Args:
            lake_id: ID of the parent lake
            zone_config: Configuration for the zone
            
        Returns:
            Full name of the created zone
        """
        zone_id = zone_config['zone_id']
        zone_name = f"{self.parent}/lakes/{lake_id}/zones/{zone_id}"
        
        try:
            # Check if zone already exists
            self.dataplex_client.get_zone(name=zone_name)
            logging.info(f"Data zone {zone_id} already exists in lake {lake_id}")
            return zone_name
            
        except exceptions.NotFound:
            # Create new zone
            zone_type = getattr(
                dataplex_v1.Zone.Type, 
                zone_config.get('type', 'CURATED')
            )
            
            zone = dataplex_v1.Zone(
                display_name=zone_config.get('zone_display_name', zone_id),
                description=f"Data zone for {zone_config.get('type', 'curated')} data",
                type_=zone_type,
                resource_spec=dataplex_v1.Zone.ResourceSpec(
                    location_type=dataplex_v1.Zone.ResourceSpec.LocationType.SINGLE_REGION
                ),
                labels={
                    "environment": zone_config.get('environment', 'dev'),
                    "zone_type": zone_config.get('type', 'curated').lower(),
                    "created_by": "data-platform-pipeline"
                }
            )
            
            operation = self.dataplex_client.create_zone(
                parent=f"{self.parent}/lakes/{lake_id}",
                zone_id=zone_id,
                zone=zone
            )
            
            # Wait for operation to complete
            result = operation.result(timeout=300)
            logging.info(f"Created data zone: {result.name}")
            return result.name
    
    def create_asset(self, lake_id: str, zone_id: str, asset_config: Dict[str, Any]) -> str:
        """
        Create a Dataplex asset
        
        Args:
            lake_id: ID of the parent lake
            zone_id: ID of the parent zone
            asset_config: Configuration for the asset
            
        Returns:
            Full name of the created asset
        """
        asset_id = asset_config['asset_id']
        asset_name = f"{self.parent}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}"
        
        try:
            # Check if asset already exists
            self.dataplex_client.get_asset(name=asset_name)
            logging.info(f"Asset {asset_id} already exists")
            return asset_name
            
        except exceptions.NotFound:
            # Create new asset
            resource_spec = dataplex_v1.Asset.ResourceSpec()
            
            if asset_config['resource_type'] == 'BIGQUERY_DATASET':
                resource_spec.type_ = dataplex_v1.Asset.ResourceSpec.Type.BIGQUERY_DATASET
                resource_spec.name = f"projects/{self.project_id}/datasets/{asset_config['dataset_id']}"
            elif asset_config['resource_type'] == 'STORAGE_BUCKET':
                resource_spec.type_ = dataplex_v1.Asset.ResourceSpec.Type.STORAGE_BUCKET
                resource_spec.name = f"projects/{self.project_id}/buckets/{asset_config['bucket_name']}"
            
            asset = dataplex_v1.Asset(
                display_name=asset_config.get('display_name', asset_id),
                description=asset_config.get('description', f"Asset for {asset_id}"),
                resource_spec=resource_spec,
                labels={
                    "environment": asset_config.get('environment', 'dev'),
                    "asset_type": asset_config['resource_type'].lower(),
                    "created_by": "data-platform-pipeline"
                }
            )
            
            operation = self.dataplex_client.create_asset(
                parent=f"{self.parent}/lakes/{lake_id}/zones/{zone_id}",
                asset_id=asset_id,
                asset=asset
            )
            
            # Wait for operation to complete
            result = operation.result(timeout=300)
            logging.info(f"Created asset: {result.name}")
            return result.name
    
    def setup_dataplex_for_domain(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Setup complete Dataplex structure for a domain
        
        Args:
            config: Domain configuration including dataplex settings
            
        Returns:
            Dictionary mapping zone types to their full names
        """
        dataplex_config = config.get('secrets', {}).get('dataplex', {})
        if not dataplex_config:
            logging.warning("No Dataplex configuration found")
            return {}
        
        domain = config.get('domain', 'unknown')
        environment = config.get('environment', 'dev')
        
        # Add domain and environment to config
        dataplex_config['domain'] = domain
        dataplex_config['environment'] = environment
        
        results = {}
        
        # Create data lake
        lake_name = self.create_data_lake(dataplex_config)
        lake_id = dataplex_config['lake_id']
        
        # Create zones
        zones_config = dataplex_config.get('zones', {})
        for zone_type, zone_config in zones_config.items():
            zone_config['environment'] = environment
            zone_name = self.create_data_zone(lake_id, zone_config)
            results[zone_type] = zone_name
        
        logging.info(f"Dataplex setup completed for domain {domain}")
        return results


def setup_dataplex_from_config(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Setup Dataplex infrastructure based on configuration
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Dictionary with Dataplex setup results or None if no config
    """
    project_id = config.get('project')
    region = config.get('region')
    
    if not project_id or not region:
        logging.error("Project ID and region are required for Dataplex setup")
        return None
    
    dataplex_manager = DataplexManager(project_id, region)
    
    try:
        # Setup lake and zones
        zones = dataplex_manager.setup_dataplex_for_domain(config)
        
        return {
            'zones': zones,
            'status': 'success',
            'message': 'Dataplex setup completed successfully'
        }
        
    except Exception as e:
        logging.error(f"Failed to setup Dataplex: {e}")
        return {
            'status': 'error',
            'message': str(e)
        }
