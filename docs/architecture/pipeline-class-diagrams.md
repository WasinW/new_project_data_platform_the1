# Pipeline Class Diagrams & UML

## 📊 Class Relationship Diagrams

### 1. Hybrid Pipeline Architecture

```mermaid
classDiagram
    class HybridPipelineOptions {
        +String mode
        +String config_path
        +String domain
        +int batch_window_hours
        +boolean enable_windowing
        +_add_argparse_args(parser)
    }

    class HybridPipeline {
        -HybridPipelineOptions options
        -Dict config
        +__init__(options)
        +_load_config() Dict
        +run_realtime_pipeline(pipeline)
        +run_batch_pipeline(pipeline)
    }

    class NativeDependencyChecker {
        -String dependency_query_template
        +__init__(dependency_query_template)
        +process(element)
        -_validate_dependency(element)
    }

    class NativeDataTransform {
        -Dict transform_config
        +__init__(transform_config)
        +process(element)
        -_apply_transforms(element) Dict
        -_convert_type(value, target_type)
    }

    HybridPipeline --> HybridPipelineOptions : uses
    HybridPipeline --> NativeDependencyChecker : creates
    HybridPipeline --> NativeDataTransform : creates
    PipelineOptions <|-- HybridPipelineOptions
    DoFn <|-- NativeDependencyChecker
    DoFn <|-- NativeDataTransform
```

### 2. Reconciliation Pipeline Architecture

```mermaid
classDiagram
    class ReconciliationOptions {
        +String s3_external_table
        +String native_table
        +String output_table
        +String key_columns
        +String comparison_columns
        +String tolerance_config
        +_add_argparse_args(parser)
    }

    class RecordMatcher {
        -List~String~ key_columns
        -List~String~ comparison_columns
        -Dict tolerance_config
        +__init__(key_columns, comparison_columns, tolerance_config)
        +process(element)
        -_create_result(key, status, s3_record, native_record) Dict
        -_records_match_exactly(s3_record, native_record) boolean
        -_find_best_match(s3_record, native_records) Dict
        -_compare_records(s3_record, native_record) List
        -_values_match(column, val1, val2) boolean
        -_get_difference_type(val1, val2) String
    }

    class StatisticsAggregator {
        +process(element)
        -_calculate_percentages(stats) Dict
    }

    class DataflowClientMixin {
        <<interface>>
        +setup_client()
        +cleanup_client()
    }

    PipelineOptions <|-- ReconciliationOptions
    DoFn <|-- RecordMatcher
    DoFn <|-- StatisticsAggregator
    DataflowClientMixin <|-- RecordMatcher
```

### 3. Data Distribution Architecture

```mermaid
classDiagram
    class DataDistributor {
        -Dict distribution_mapping
        +__init__(distribution_mapping)
        +process(element)
        -_create_table_record(element, table, columns) Dict
        -_add_metadata(record) Dict
    }

    class SmartDistributor {
        -Dict distribution_config
        -Dict table_conditions
        -Dict validation_rules
        -List default_tables
        +__init__(distribution_config)
        +process(element)
        -_evaluate_condition(element, condition) boolean
        -_validate_data(element, table) boolean
        -_create_table_record(element, table, columns) Dict
    }

    DoFn <|-- DataDistributor
    DoFn <|-- SmartDistributor
    DataDistributor <|-- SmartDistributor : extends
```

### 4. Dependency Checking Architecture

```mermaid
classDiagram
    class DependencyChecker {
        -List~Dict~ dependencies
        -timedelta cache_duration
        -Dict dependency_cache
        -BigQueryClient bq_client
        +__init__(dependencies, cache_duration_minutes)
        +setup()
        +process(element)
        -_check_dependency(dep, element) boolean
        -_create_dependency_key(dep, element) String
        -_perform_dependency_check(dep, element) boolean
        -_clean_cache(current_time)
    }

    class WindowedDependencyChecker {
        -List~Dict~ dependencies
        -WindowingConfig windowing_config
        +__init__(dependencies, windowing_config)
        +process(element, window)
        -_check_windowed_dependency(dep, element, window) boolean
        -_get_window_context(window) Dict
    }

    DoFn <|-- DependencyChecker
    DoFn <|-- WindowedDependencyChecker
    DependencyChecker <|-- WindowedDependencyChecker : extends
```

### 5. Windowing System Architecture

```mermaid
classDiagram
    class WindowingConfig {
        -Dict config
        -boolean enabled
        +__init__(config)
        +apply_window(pcoll, step_name) PCollection
        -_create_window_fn(config) WindowFn
        -_create_trigger(trigger_config) Trigger
        -_get_accumulation_mode(config) AccumulationMode
    }

    class WindowedAggregator {
        -WindowingConfig windowing_config
        -Dict aggregation_config
        +__init__(windowing_config, aggregation_config)
        +process(element, window)
        -_aggregate_by_window(elements, window) Dict
    }

    class BatchWindowProcessor {
        -WindowingConfig windowing_config
        -int batch_size
        +__init__(windowing_config, batch_size)
        +process(element, window)
        -_process_batch(batch, window) List
    }

    class WindowAuditLogger {
        -WindowingConfig windowing_config
        -AuditLogger audit_logger
        +__init__(windowing_config, audit_logger)
        +process(element, window)
        -_log_window_metrics(window, metrics)
    }

    DoFn <|-- WindowedAggregator
    DoFn <|-- BatchWindowProcessor
    DoFn <|-- WindowAuditLogger
    WindowingConfig --> WindowedAggregator : configures
    WindowingConfig --> BatchWindowProcessor : configures
    WindowingConfig --> WindowAuditLogger : configures
```

### 6. Audit & Monitoring Architecture

```mermaid
classDiagram
    class AuditLogger {
        -Dict pipeline_config
        -String audit_table
        -String lineage_table
        -String metrics_table
        -BigQueryClient bq_client
        -int batch_size
        -List audit_batch
        -List lineage_batch
        -List metrics_batch
        +__init__(pipeline_config)
        +setup()
        +process(element)
        +finish_bundle()
        -_create_audit_record(element) Dict
        -_create_lineage_record(element) Dict
        -_create_metrics_record(element) Dict
        -_flush_batches()
        -_extract_record_id(element) String
        -_calculate_data_size(element) int
        -_classify_data(element) String
    }

    class ClientManager {
        <<interface>>
        +setup_clients()
        +cleanup_clients()
        +get_bigquery_client() BigQueryClient
        +get_pubsub_client() PubSubClient
    }

    class DataflowClientMixin {
        <<interface>>
        +setup()
        +teardown()
    }

    DoFn <|-- AuditLogger
    ClientManager <|-- DataflowClientMixin
    AuditLogger --> DataflowClientMixin : implements
```

## 🔄 Sequence Diagrams

### 1. Hybrid Pipeline Execution Flow

```mermaid
sequenceDiagram
    participant Client
    participant HybridPipeline
    participant Config
    participant Dependencies
    participant Transform
    participant BigQuery

    Client->>HybridPipeline: run(mode="realtime")
    HybridPipeline->>Config: _load_config()
    Config-->>HybridPipeline: pipeline_config
    
    HybridPipeline->>Dependencies: NativeDependencyChecker.process()
    Dependencies->>BigQuery: ReadFromBigQuery(dependency_query)
    BigQuery-->>Dependencies: dependency_results
    Dependencies-->>HybridPipeline: validated_elements
    
    HybridPipeline->>Transform: NativeDataTransform.process()
    Transform-->>HybridPipeline: transformed_data, errors
    
    HybridPipeline->>BigQuery: WriteToBigQuery(output_table)
    HybridPipeline->>BigQuery: WriteToBigQuery(error_table)
```

### 2. Reconciliation Pipeline Flow

```mermaid
sequenceDiagram
    participant Scheduler
    participant ReconciliationPipeline
    participant S3ExternalTable
    participant BigQueryNative
    participant RecordMatcher
    participant OutputTable

    Scheduler->>ReconciliationPipeline: run_reconciliation()
    
    par Read Sources
        ReconciliationPipeline->>S3ExternalTable: ReadFromBigQuery(s3_table)
        ReconciliationPipeline->>BigQueryNative: ReadFromBigQuery(native_table)
    end
    
    S3ExternalTable-->>ReconciliationPipeline: s3_data
    BigQueryNative-->>ReconciliationPipeline: native_data
    
    ReconciliationPipeline->>RecordMatcher: CoGroupByKey()
    RecordMatcher->>RecordMatcher: _compare_records()
    RecordMatcher-->>ReconciliationPipeline: comparison_results
    
    ReconciliationPipeline->>OutputTable: WriteToBigQuery(results)
    ReconciliationPipeline->>OutputTable: WriteToBigQuery(statistics)
```

### 3. Data Distribution Flow

```mermaid
sequenceDiagram
    participant Pipeline
    participant SmartDistributor
    participant ValidationEngine
    participant TableA
    participant TableB
    participant ErrorTable

    Pipeline->>SmartDistributor: process(element)
    SmartDistributor->>SmartDistributor: _evaluate_condition()
    
    alt Condition Met for Table A
        SmartDistributor->>ValidationEngine: _validate_data(element, tableA)
        ValidationEngine-->>SmartDistributor: validation_passed
        SmartDistributor->>TableA: TaggedOutput("tableA", record)
    end
    
    alt Condition Met for Table B
        SmartDistributor->>ValidationEngine: _validate_data(element, tableB)
        ValidationEngine-->>SmartDistributor: validation_failed
        SmartDistributor->>ErrorTable: TaggedOutput("validation_error", error_record)
    end
```

## 🎯 Component Interaction Patterns

### 1. Configuration Pattern
```mermaid
graph TD
    A[parameters.conf] --> B[Config Loader]
    B --> C[Pipeline Options]
    C --> D[Pipeline Execution]
    D --> E[DoFn Components]
    E --> F[Output Processing]
```

### 2. Error Handling Pattern
```mermaid
graph TD
    A[Input Element] --> B{Validation}
    B -->|Pass| C[Process Element]
    B -->|Fail| D[Error Output]
    C --> E{Transform Success}
    E -->|Success| F[Main Output]
    E -->|Error| G[Error Output]
    D --> H[Error Table]
    G --> H
```

### 3. Audit Trail Pattern
```mermaid
graph TD
    A[Processing Element] --> B[AuditLogger]
    B --> C[Audit Record]
    B --> D[Lineage Record]
    B --> E[Metrics Record]
    C --> F[Audit Table]
    D --> G[Lineage Table]
    E --> H[Metrics Table]
```

## 📐 Design Pattern Implementation

### 1. Strategy Pattern
- **Context**: Pipeline execution mode (batch/realtime)
- **Strategies**: Different processing algorithms
- **Benefits**: Runtime mode selection, code reuse

### 2. Template Method Pattern
- **Template**: Base pipeline structure
- **Variable Steps**: Transform logic, I/O operations
- **Benefits**: Consistent pipeline structure, customizable behavior

### 3. Factory Pattern
- **Products**: Window functions, triggers, configurations
- **Factories**: WindowingConfig, OptionsFactory
- **Benefits**: Object creation abstraction, runtime configuration

### 4. Observer Pattern
- **Subject**: Pipeline processing
- **Observers**: AuditLogger, MetricsCollector
- **Benefits**: Loose coupling, extensible monitoring

### 5. Decorator Pattern
- **Component**: Basic DoFn processing
- **Decorators**: Windowing, caching, validation
- **Benefits**: Behavior enhancement, composability

This comprehensive OOP design documentation provides clear understanding of the pipeline architecture, class relationships, and design patterns used throughout the codebase.
