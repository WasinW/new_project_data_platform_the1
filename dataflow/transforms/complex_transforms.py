# dataflow/transforms/complex_transforms.py
import apache_beam as beam
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import importlib
import json

class ComplexTransform(beam.DoFn):
    """
    Apply complex business logic transformations.
    Supports dynamic module loading and custom transformation logic.
    """
    
    def __init__(self, transform_module: str, params: Dict):
        self.transform_module = transform_module
        self.params = params
        self.transformer = None
        logging.info(f"ComplexTransform initialized with module: {transform_module}")
    
    def setup(self):
        """Setup method to initialize the transformer"""
        try:
            # Dynamically import transform module
            module_path, class_name = self.transform_module.rsplit('.', 1)
            module = importlib.import_module(module_path)
            self.transform_class = getattr(module, class_name)
            self.transformer = self.transform_class(self.params)
            logging.info(f"Successfully loaded transformer: {self.transform_module}")
        except Exception as e:
            logging.error(f"Failed to load transformer {self.transform_module}: {e}")
            raise
    
    def process(self, element):
        """Process element through complex transformation"""
        try:
            if not self.transformer:
                raise ValueError("Transformer not initialized")
            
            # Apply transformation
            transformed = self.transformer.transform(element)
            
            # Add transformation metadata
            if isinstance(transformed, dict):
                transformed['_transform_module'] = self.transform_module
                transformed['_transform_timestamp'] = datetime.utcnow().isoformat()
            
            yield transformed
            
        except Exception as e:
            logging.error(f"Transform failed for {self.transform_module}: {e}")
            yield beam.pvalue.TaggedOutput('transform_error', {
                'input': element,
                'error': str(e),
                'transform': self.transform_module,
                'timestamp': datetime.utcnow().isoformat()
            })

class BaseTransformer:
    """Base class for all custom transformers"""
    
    def __init__(self, params: Dict):
        self.params = params
        self.setup()
    
    def setup(self):
        """Override this method for custom setup logic"""
        pass
    
    def transform(self, element: Dict) -> Dict:
        """Override this method to implement transformation logic"""
        raise NotImplementedError("Transform method must be implemented")

class MemberProfileEnrichment(BaseTransformer):
    """
    Example complex transformer for member profile enrichment.
    Adds additional data from lookup tables and applies business rules.
    """
    
    def setup(self):
        from google.cloud import bigquery
        self.bq_client = bigquery.Client()
        self.lookup_table = self.params.get('lookup_table')
        self.join_key = self.params.get('join_key', 'member_id')
        self.cache = {}
    
    def transform(self, element: Dict) -> Dict:
        """Enrich member profile with additional data"""
        member_id = element.get(self.join_key)
        
        if not member_id:
            logging.warning(f"No {self.join_key} found in element")
            return element
        
        # Get enrichment data
        enrichment_data = self._get_enrichment_data(member_id)
        
        # Apply business rules
        enriched_element = self._apply_business_rules(element, enrichment_data)
        
        return enriched_element
    
    def _get_enrichment_data(self, member_id: str) -> Dict:
        """Get enrichment data from lookup table"""
        if member_id in self.cache:
            return self.cache[member_id]
        
        query = f"""
            SELECT *
            FROM `{self.lookup_table}`
            WHERE {self.join_key} = @member_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("member_id", "STRING", member_id)
            ]
        )
        
        try:
            result = self.bq_client.query(query, job_config=job_config).result()
            enrichment_data = {}
            for row in result:
                enrichment_data = dict(row)
                break
            
            # Cache result
            self.cache[member_id] = enrichment_data
            return enrichment_data
            
        except Exception as e:
            logging.error(f"Failed to get enrichment data for {member_id}: {e}")
            return {}
    
    def _apply_business_rules(self, element: Dict, enrichment_data: Dict) -> Dict:
        """Apply business rules for member profile"""
        result = element.copy()
        
        # Add segment information
        if 'segment' in enrichment_data:
            result['member_segment'] = enrichment_data['segment']
        
        # Calculate member score
        result['member_score'] = self._calculate_member_score(element, enrichment_data)
        
        # Add tier information
        result['member_tier'] = self._determine_member_tier(result['member_score'])
        
        # Add enrichment metadata
        result['enriched_timestamp'] = datetime.utcnow().isoformat()
        result['enrichment_source'] = self.lookup_table
        
        return result
    
    def _calculate_member_score(self, element: Dict, enrichment_data: Dict) -> int:
        """Calculate member score based on various factors"""
        score = 0
        
        # Base score from transaction value
        transaction_value = element.get('transaction_value', 0)
        score += min(transaction_value / 100, 50)  # Max 50 points from transactions
        
        # Score from membership duration
        if 'membership_start_date' in enrichment_data:
            # Calculate membership duration and add points
            score += 10  # Simplified
        
        # Score from segment
        segment_scores = {'premium': 30, 'gold': 20, 'silver': 10, 'bronze': 5}
        segment = enrichment_data.get('segment', 'bronze')
        score += segment_scores.get(segment, 0)
        
        return min(int(score), 100)  # Cap at 100
    
    def _determine_member_tier(self, score: int) -> str:
        """Determine member tier based on score"""
        if score >= 80:
            return 'platinum'
        elif score >= 60:
            return 'gold'
        elif score >= 40:
            return 'silver'
        else:
            return 'bronze'

class MemberAggregation(BaseTransformer):
    """
    Example aggregation transformer that groups and summarizes member data.
    """
    
    def setup(self):
        self.group_by_fields = self.params.get('group_by', [])
        self.metric_fields = self.params.get('metrics', [])
        self.aggregation_window = self.params.get('window_days', 30)
    
    def transform(self, element: Dict) -> Dict:
        """Create aggregated member summary"""
        # Create grouping key
        group_key = self._create_group_key(element)
        
        # Calculate metrics
        metrics = self._calculate_metrics(element)
        
        # Create aggregated record
        result = {
            'group_key': group_key,
            'aggregation_date': datetime.utcnow().date().isoformat(),
            'aggregation_window_days': self.aggregation_window,
            **metrics
        }
        
        # Add group-by fields
        for field in self.group_by_fields:
            result[field] = element.get(field)
        
        return result
    
    def _create_group_key(self, element: Dict) -> str:
        """Create unique key for grouping"""
        key_parts = []
        for field in self.group_by_fields:
            value = element.get(field, 'unknown')
            key_parts.append(f"{field}:{value}")
        return '|'.join(key_parts)
    
    def _calculate_metrics(self, element: Dict) -> Dict:
        """Calculate aggregation metrics"""
        metrics = {}
        
        for metric in self.metric_fields:
            if metric == 'total_value':
                metrics['total_value'] = element.get('transaction_value', 0)
            elif metric == 'transaction_count':
                metrics['transaction_count'] = 1
            elif metric == 'unique_members':
                metrics['unique_members'] = 1 if element.get('member_id') else 0
            else:
                # Generic sum for other numeric fields
                metrics[metric] = element.get(metric, 0)
        
        return metrics

class DataValidationTransform(beam.DoFn):
    """
    Data validation and quality transformation.
    Applies various validation rules and data quality checks.
    """
    
    def __init__(self, validation_config: Dict):
        self.validation_config = validation_config
        self.rules = validation_config.get('rules', [])
        self.quality_checks = validation_config.get('quality_checks', [])
    
    def process(self, element):
        """Process element with validation and quality checks"""
        validation_results = {
            'element_id': element.get('id', element.get('member_id', 'unknown')),
            'validation_timestamp': datetime.utcnow().isoformat(),
            'passed_rules': [],
            'failed_rules': [],
            'quality_score': 0,
            'quality_issues': []
        }
        
        # Apply validation rules
        for rule in self.rules:
            if self._check_validation_rule(element, rule):
                validation_results['passed_rules'].append(rule['name'])
            else:
                validation_results['failed_rules'].append(rule['name'])
        
        # Apply quality checks
        quality_score, quality_issues = self._check_data_quality(element)
        validation_results['quality_score'] = quality_score
        validation_results['quality_issues'] = quality_issues
        
        # Determine if element passes validation
        min_quality_score = self.validation_config.get('min_quality_score', 70)
        max_failed_rules = self.validation_config.get('max_failed_rules', 0)
        
        if (quality_score >= min_quality_score and 
            len(validation_results['failed_rules']) <= max_failed_rules):
            # Validation passed
            element['_validation_results'] = validation_results
            yield element
        else:
            # Validation failed
            yield beam.pvalue.TaggedOutput('validation_failed', {
                'element': element,
                'validation_results': validation_results
            })
    
    def _check_validation_rule(self, element: Dict, rule: Dict) -> bool:
        """Check individual validation rule"""
        rule_type = rule.get('type')
        field = rule.get('field')
        value = element.get(field)
        
        if rule_type == 'required':
            return value is not None and str(value).strip() != ''
        elif rule_type == 'type_check':
            expected_type = rule.get('expected_type')
            if expected_type == 'int':
                try:
                    int(value)
                    return True
                except (ValueError, TypeError):
                    return False
            elif expected_type == 'float':
                try:
                    float(value)
                    return True
                except (ValueError, TypeError):
                    return False
            elif expected_type == 'email':
                import re
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                return bool(re.match(email_pattern, str(value)))
        elif rule_type == 'range':
            try:
                num_value = float(value)
                min_val = rule.get('min', float('-inf'))
                max_val = rule.get('max', float('inf'))
                return min_val <= num_value <= max_val
            except (ValueError, TypeError):
                return False
        elif rule_type == 'length':
            str_value = str(value) if value is not None else ''
            min_len = rule.get('min', 0)
            max_len = rule.get('max', float('inf'))
            return min_len <= len(str_value) <= max_len
        
        return True
    
    def _check_data_quality(self, element: Dict) -> tuple[int, List[str]]:
        """Check data quality and return score and issues"""
        quality_score = 100
        quality_issues = []
        
        for check in self.quality_checks:
            check_type = check.get('type')
            field = check.get('field')
            value = element.get(field)
            
            if check_type == 'completeness':
                if value is None or str(value).strip() == '':
                    quality_score -= check.get('penalty', 10)
                    quality_issues.append(f"Missing value for {field}")
            
            elif check_type == 'format_consistency':
                pattern = check.get('pattern')
                if pattern and value:
                    import re
                    if not re.match(pattern, str(value)):
                        quality_score -= check.get('penalty', 5)
                        quality_issues.append(f"Format inconsistency in {field}")
            
            elif check_type == 'outlier_detection':
                if value and isinstance(value, (int, float)):
                    min_val = check.get('min_expected', float('-inf'))
                    max_val = check.get('max_expected', float('inf'))
                    if not (min_val <= value <= max_val):
                        quality_score -= check.get('penalty', 15)
                        quality_issues.append(f"Outlier value in {field}: {value}")
        
        return max(quality_score, 0), quality_issues
