# Pipeline OOP Design Documentation

## 📋 Overview
This document provides detailed Object-Oriented Programming (OOP) design analysis for all data pipelines in the platform. Each pipeline follows specific design patterns and architectural principles for maintainability, scalability, and testability.

---

## 🏗️ Overall Architecture Principles

### Design Patterns Used
- **Strategy Pattern**: Different processing modes (batch/realtime) using the same pipeline code
- **Template Method Pattern**: Base pipeline structure with customizable steps
- **Factory Pattern**: Creating appropriate window functions and triggers
- **Observer Pattern**: Audit logging and monitoring across all pipelines
- **Dependency Injection**: Configuration and client management
- **Builder Pattern**: Pipeline configuration and options setup

### SOLID Principles Implementation
- **Single Responsibility**: Each class has one clear purpose
- **Open/Closed**: Extensible through configuration, closed for modification
- **Liskov Substitution**: DoFn implementations are interchangeable
- **Interface Segregation**: Focused interfaces for specific functionality
- **Dependency Inversion**: Depend on abstractions, not concrete implementations

---

## 🔄 1. Hybrid Pipeline (hybrid_pipeline.py)

### Class Hierarchy

```
HybridPipeline
├── HybridPipelineOptions (extends PipelineOptions)
├── NativeDependencyChecker (extends beam.DoFn)
├── NativeDataTransform (extends beam.DoFn)
└── Configuration Management
```

### 🎯 HybridPipelineOptions Class
```python
class HybridPipelineOptions(PipelineOptions):
```

**Purpose**: Configuration container for pipeline runtime options
**Design Pattern**: Builder Pattern
**Responsibilities**:
- Define command-line arguments for pipeline execution
- Validate input parameters
- Provide type-safe access to configuration values

**Key Features**:
- Mode selection (batch/realtime)
- Domain-specific configuration
- Resource allocation settings

**Inheritance**: Extends Apache Beam's `PipelineOptions`

### 🔍 NativeDependencyChecker Class
```python
class NativeDependencyChecker(beam.DoFn):
```

**Purpose**: Validates upstream dependencies before processing data
**Design Pattern**: Strategy Pattern + Template Method
**Responsibilities**:
- Check dependency conditions using BigQuery Native I/O
- Route elements based on dependency status
- Handle dependency failures gracefully

**Key Methods**:
- `process(element)`: Main processing logic with dependency validation
- `_validate_dependency()`: Core dependency checking logic

**Output Streams**:
- `main`: Elements that pass dependency checks
- `failed_dependency`: Elements that fail dependency validation

### 🔄 NativeDataTransform Class
```python
class NativeDataTransform(beam.DoFn):
```

**Purpose**: Applies business logic transformations to data elements
**Design Pattern**: Strategy Pattern + Command Pattern
**Responsibilities**:
- Apply column mappings and data type conversions
- Handle transformation errors gracefully
- Add processing metadata

**Key Methods**:
- `process(element)`: Main transformation pipeline
- `_apply_transforms(element)`: Apply configured transformations
- `_convert_type(value, target_type)`: Type conversion logic

**Configuration-Driven**:
- Column mapping rules
- Data type conversion specifications
- Error handling strategies

### 🏗️ HybridPipeline Main Class
```python
class HybridPipeline:
```

**Purpose**: Orchestrates the entire pipeline execution
**Design Pattern**: Facade Pattern + Template Method
**Responsibilities**:
- Pipeline mode selection (batch/realtime)
- Configuration management
- I/O coordination using Native Beam I/O

**Key Methods**:
- `__init__(options)`: Initialize with configuration
- `_load_config()`: Load and validate configuration
- `run_realtime_pipeline(pipeline)`: Execute streaming mode
- `run_batch_pipeline(pipeline)`: Execute batch mode

**Architecture Benefits**:
- **Zero Client Management**: Uses Apache Beam Native I/O
- **10x Performance**: Storage Write API for BigQuery
- **Code Reuse**: Same codebase for batch and streaming
- **Native Connection Pooling**: Automatic resource management

---

## 🔀 2. Reconciliation Pipeline (reconciliation_pipeline.py)

### Class Hierarchy

```
ReconciliationPipeline
├── ReconciliationOptions (extends PipelineOptions)
├── RecordMatcher (extends beam.DoFn, DataflowClientMixin)
├── StatisticsAggregator (extends beam.DoFn)
└── Comparison Engine
```

### ⚙️ ReconciliationOptions Class
```python
class ReconciliationOptions(PipelineOptions):
```

**Purpose**: Configuration for reconciliation pipeline parameters
**Design Pattern**: Builder Pattern
**Responsibilities**:
- Define S3 external table and BigQuery native table
- Configure comparison columns and key columns
- Set tolerance parameters for data comparison

**Key Configuration**:
- Source table specifications
- Comparison logic parameters
- Output table configurations

### 🔍 RecordMatcher Class
```python
class RecordMatcher(beam.DoFn, DataflowClientMixin):
```

**Purpose**: Core reconciliation logic for comparing records
**Design Pattern**: Strategy Pattern + Template Method
**Responsibilities**:
- Match records between S3 and BigQuery
- Perform detailed value comparisons
- Generate reconciliation results

**Key Methods**:
- `process(element)`: Main record matching logic
- `_create_result()`: Standardized result generation
- `_records_match_exactly()`: Exact match validation
- `_find_best_match()`: Intelligent record matching
- `_compare_records()`: Detailed field-by-field comparison
- `_values_match()`: Configurable value comparison with tolerance

**Output Types**:
- `MATCH`: Records that match exactly
- `MISMATCH`: Records with differences
- `MISSING_IN_NATIVE`: Records only in S3
- `MISSING_IN_S3`: Records only in BigQuery

**Advanced Features**:
- Tolerance-based comparisons (numeric, string)
- Case-insensitive string matching
- Percentage-based numeric tolerance
- Type-aware comparisons

### 📊 StatisticsAggregator Class
```python
class StatisticsAggregator(beam.DoFn):
```

**Purpose**: Generate summary statistics for reconciliation results
**Design Pattern**: Observer Pattern
**Responsibilities**:
- Aggregate reconciliation status counts
- Calculate percentage distributions
- Generate timestamped statistics

**Key Features**:
- Status-based grouping
- Percentage calculations
- Time-series tracking

---

## 🔄 3. Data Transforms (transforms/ directory)

### 📤 DataDistributor Class (distributor.py)

```python
class DataDistributor(beam.DoFn):
```

**Purpose**: Route data to multiple target tables based on configuration
**Design Pattern**: Router Pattern + Strategy Pattern
**Responsibilities**:
- Multi-table data distribution
- Column-specific filtering
- Metadata enrichment

**Key Features**:
- Configurable table mappings
- Missing column handling
- Automatic metadata addition

### 🧠 SmartDistributor Class (distributor.py)

```python
class SmartDistributor(beam.DoFn):
```

**Purpose**: Advanced distribution with conditional logic
**Design Pattern**: Strategy Pattern + Rules Engine
**Responsibilities**:
- Conditional data routing
- Data validation before distribution
- Error handling and logging

**Advanced Features**:
- Rule-based routing conditions
- Data validation pipelines
- Fallback and error handling

### 🔗 DependencyChecker Class (dependency_checker.py)

```python
class DependencyChecker(beam.DoFn):
```

**Purpose**: Validate upstream dependencies with caching
**Design Pattern**: Strategy Pattern + Cache Pattern
**Responsibilities**:
- Multi-dependency validation
- Result caching for performance
- Graceful failure handling

**Key Methods**:
- `process(element)`: Main dependency validation
- `_check_dependency()`: Individual dependency check with caching
- `_perform_dependency_check()`: Actual BigQuery validation
- `_create_dependency_key()`: Cache key generation
- `_clean_cache()`: Cache maintenance

**Performance Features**:
- Intelligent caching (5-minute default TTL)
- Cache key optimization
- Automatic cache cleanup

---

## 🪟 4. Windowing Utilities (utils/windowing.py)

### ⚙️ WindowingConfig Class

```python
class WindowingConfig:
```

**Purpose**: Centralized windowing configuration management
**Design Pattern**: Factory Pattern + Strategy Pattern
**Responsibilities**:
- Window function creation
- Trigger configuration
- Step-specific windowing

**Key Methods**:
- `apply_window(pcoll, step_name)`: Apply windowing to PCollection
- `_create_window_fn(config)`: Factory for window functions
- `_create_trigger(trigger_config)`: Factory for triggers
- `_get_accumulation_mode(config)`: Accumulation mode selection

**Supported Window Types**:
- **Fixed Windows**: Regular time intervals
- **Sliding Windows**: Overlapping time windows
- **Session Windows**: Gap-based grouping

**Trigger Types**:
- **After Watermark**: Based on event time
- **After Count**: Based on element count
- **After Processing Time**: Based on processing time
- **Repeatedly**: Repeated triggering

### 🔍 WindowedDependencyChecker Class

```python
class WindowedDependencyChecker(beam.DoFn):
```

**Purpose**: Window-aware dependency validation
**Design Pattern**: Decorator Pattern + Strategy Pattern
**Responsibilities**:
- Window context preservation
- Time-aware dependency checks
- Window metadata tracking

---

## 📝 5. Audit Logging (utils/audit_logger.py)

### 📊 AuditLogger Class

```python
class AuditLogger(beam.DoFn):
```

**Purpose**: Comprehensive pipeline audit and lineage tracking
**Design Pattern**: Observer Pattern + Strategy Pattern
**Responsibilities**:
- Processing audit logs
- Data lineage tracking
- Performance metrics collection

**Key Methods**:
- `process(element)`: Main audit processing
- `_create_audit_record()`: Audit log generation
- `_create_lineage_record()`: Data lineage tracking
- `_create_metrics_record()`: Performance metrics
- `_flush_batches()`: Batch processing for performance

**Audit Dimensions**:
- **Processing Logs**: Execution tracking
- **Data Lineage**: Source-to-target mapping
- **Processing Metrics**: Performance measurements
- **Quality Metrics**: Data validation results

**Performance Features**:
- Batched processing (100 records default)
- Asynchronous flushing
- Error isolation (audit failures don't break pipeline)

---

## 🎛️ 6. Airflow DAG Structure (airflow/dags/)

### 🔄 Batch Pipeline DAG

**Purpose**: Orchestrate batch data processing workflows
**Design Pattern**: Template Method + Factory Pattern
**Architecture**:
- **Dynamic DAG Generation**: `create_batch_dag(domain)`
- **Configuration Management**: Runtime parameter preparation
- **Native Operator Usage**: DataflowCreatePythonJobOperator

**Key Components**:
- Configuration preparation tasks
- Dataflow job orchestration
- Error handling and retries

---

## 🔧 Design Benefits

### 1. **Modularity**
- Each class has a single, well-defined responsibility
- Clear separation between configuration, processing, and I/O
- Pluggable components through dependency injection

### 2. **Scalability**
- Apache Beam native scaling capabilities
- Configurable resource allocation
- Efficient memory and CPU utilization

### 3. **Maintainability**
- Clear inheritance hierarchies
- Consistent naming conventions
- Comprehensive error handling

### 4. **Testability**
- Dependency injection enables easy mocking
- Clear input/output contracts
- Isolated component testing

### 5. **Performance**
- Native I/O eliminates client management overhead
- Storage Write API for 10x BigQuery performance
- Intelligent caching and batching

### 6. **Observability**
- Comprehensive audit logging
- Data lineage tracking
- Performance monitoring
- Error tracking and alerting

---

## 🚀 Usage Patterns

### Pipeline Execution
```python
# Realtime mode
python hybrid_pipeline.py \
  --mode=realtime \
  --domain=member \
  --config_path=gs://config/member.yaml \
  --streaming

# Batch mode
python hybrid_pipeline.py \
  --mode=batch \
  --domain=member \
  --batch_window_hours=1 \
  --config_path=gs://config/member.yaml
```

### Configuration-Driven Development
- All behavior controlled through YAML configuration
- Runtime parameter injection
- Environment-specific configurations

### Error Handling Strategy
- Graceful degradation with error side outputs
- Comprehensive error logging
- Automatic retries with exponential backoff

---

## 📈 Performance Optimizations

### 1. **Native I/O Usage**
- Zero client creation and management
- Built-in connection pooling
- Automatic retry logic

### 2. **Storage Write API**
- 10x faster BigQuery writes
- Reduced latency and cost
- Automatic schema detection

### 3. **Intelligent Caching**
- Dependency result caching
- TTL-based cache invalidation
- Memory-efficient cache cleanup

### 4. **Batched Processing**
- Audit log batching for performance
- Configurable batch sizes
- Asynchronous processing

---

This architecture provides a robust, scalable, and maintainable foundation for data pipeline development with clear separation of concerns and comprehensive observability.
