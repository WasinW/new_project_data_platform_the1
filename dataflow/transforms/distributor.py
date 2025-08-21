# dataflow/transforms/distributor.py
import apache_beam as beam
from typing import Dict, List
from datetime import datetime
import logging

class DataDistributor(beam.DoFn):
    """
    Distribute data to multiple target tables based on mapping configuration.
    Each table gets only the columns specified in the mapping.
    """
    
    def __init__(self, distribution_mapping: Dict[str, List[str]]):
        """
        Initialize with distribution mapping.
        
        Args:
            distribution_mapping: Dict mapping table names to list of columns
        """
        self.distribution_mapping = distribution_mapping
        logging.info(f"DataDistributor initialized with {len(distribution_mapping)} tables")
    
    def process(self, element):
        """
        Process each element and distribute to target tables.
        
        Args:
            element: Input record dictionary
        """
        element_id = element.get('id', element.get('member_id', 'unknown'))
        
        for table, columns in self.distribution_mapping.items():
            # Create record with only specified columns
            table_record = {}
            missing_columns = []
            
            for col in columns:
                if col in element:
                    table_record[col] = element[col]
                else:
                    missing_columns.append(col)
                    table_record[col] = None
            
            # Add metadata
            table_record['_source_timestamp'] = element.get('_timestamp', datetime.utcnow().isoformat())
            table_record['_ingestion_timestamp'] = datetime.utcnow().isoformat()
            table_record['_element_id'] = element_id
            table_record['_target_table'] = table
            
            # Log missing columns for monitoring
            if missing_columns:
                logging.warning(f"Missing columns for table {table}, element {element_id}: {missing_columns}")
            
            yield beam.pvalue.TaggedOutput(table, table_record)

class SmartDistributor(beam.DoFn):
    """
    Advanced distributor with conditional logic and data validation.
    """
    
    def __init__(self, distribution_config: Dict):
        """
        Initialize with advanced distribution configuration.
        
        Args:
            distribution_config: Advanced configuration with conditions and validations
        """
        self.distribution_config = distribution_config
        self.table_conditions = distribution_config.get('conditions', {})
        self.validation_rules = distribution_config.get('validations', {})
        self.default_tables = distribution_config.get('default_tables', [])
    
    def process(self, element):
        """
        Process element with smart distribution logic.
        """
        element_id = element.get('id', element.get('member_id', 'unknown'))
        distributed_tables = []
        
        # Check conditions for each table
        for table, config in self.distribution_config.get('tables', {}).items():
            columns = config.get('columns', [])
            condition = config.get('condition')
            
            # Evaluate condition
            if self._evaluate_condition(element, condition):
                # Validate data
                if self._validate_data(element, table):
                    table_record = self._create_table_record(element, table, columns)
                    distributed_tables.append(table)
                    yield beam.pvalue.TaggedOutput(table, table_record)
                else:
                    # Validation failed
                    yield beam.pvalue.TaggedOutput('validation_error', {
                        'element_id': element_id,
                        'table': table,
                        'error': 'Data validation failed',
                        'timestamp': datetime.utcnow().isoformat()
                    })
        
        # If no tables matched, try default tables
        if not distributed_tables and self.default_tables:
            for table in self.default_tables:
                config = self.distribution_config['tables'][table]
                table_record = self._create_table_record(element, table, config['columns'])
                yield beam.pvalue.TaggedOutput(table, table_record)
    
    def _evaluate_condition(self, element: Dict, condition: str) -> bool:
        """Evaluate condition expression safely"""
        if not condition:
            return True
        
        try:
            # Simple condition evaluation (extend as needed)
            return eval(condition, {'element': element, 'len': len, 'str': str})
        except Exception as e:
            logging.error(f"Condition evaluation failed: {condition}, error: {e}")
            return False
    
    def _validate_data(self, element: Dict, table: str) -> bool:
        """Validate data against rules"""
        rules = self.validation_rules.get(table, [])
        
        for rule in rules:
            if not self._check_rule(element, rule):
                return False
        
        return True
    
    def _check_rule(self, element: Dict, rule: Dict) -> bool:
        """Check individual validation rule"""
        rule_type = rule.get('type')
        field = rule.get('field')
        
        if rule_type == 'required':
            return field in element and element[field] is not None
        elif rule_type == 'not_empty':
            return field in element and str(element[field]).strip() != ''
        elif rule_type == 'length':
            value = str(element.get(field, ''))
            min_len = rule.get('min', 0)
            max_len = rule.get('max', float('inf'))
            return min_len <= len(value) <= max_len
        elif rule_type == 'pattern':
            import re
            value = str(element.get(field, ''))
            pattern = rule.get('pattern')
            return bool(re.match(pattern, value)) if pattern else True
        
        return True
    
    def _create_table_record(self, element: Dict, table: str, columns: List[str]) -> Dict:
        """Create record for specific table"""
        table_record = {}
        
        for col in columns:
            table_record[col] = element.get(col)
        
        # Add metadata
        table_record['_source_timestamp'] = element.get('_timestamp', datetime.utcnow().isoformat())
        table_record['_ingestion_timestamp'] = datetime.utcnow().isoformat()
        table_record['_target_table'] = table
        table_record['_element_id'] = element.get('id', element.get('member_id', 'unknown'))
        
        return table_record
