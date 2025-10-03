# Worker Utils Documentation

This module provides common functionality and base classes for all workers in the coffee shop analysis system.

## Overview

The `worker_utils.py` module abstracts common patterns found across all workers to reduce code duplication and provide a consistent interface for:

- RabbitMQ middleware setup and configuration
- Signal handling (SIGTERM)
- Message processing patterns
- EOF handling
- Resource cleanup
- Multi-queue output support

## Base Classes

### 1. `WorkerConfig`

Configuration class that handles environment variable parsing for common worker settings.

```python
config = WorkerConfig(
    input_queue_default='my_input_queue',
    output_queue_default='my_output_queue',
    prefetch_count_default=10
)
```

**Environment Variables:**

- `RABBITMQ_HOST` (default: 'localhost')
- `RABBITMQ_PORT` (default: 5672)
- `INPUT_QUEUE` (default: provided default)
- `OUTPUT_QUEUE` (default: provided default)
- `OUTPUT_QUEUES` (comma-separated for multiple outputs)
- `PREFETCH_COUNT` (default: provided default)

### 2. `BaseWorker`

Abstract base class providing core worker functionality.

```python
class MyWorker(BaseWorker):
    def process_message(self, message):
        # Process individual message
        pass

    def process_batch(self, batch):
        # Process batch of messages
        pass
```

**Features:**

- Automatic middleware setup
- SIGTERM signal handling
- EOF message handling
- Resource cleanup
- Support for multiple output queues

### 3. `FilterWorker`

Specialized base class for workers that filter data based on criteria.

```python
class MyFilterWorker(FilterWorker):
    def apply_filter(self, item) -> bool:
        # Return True if item should pass through
        return some_condition(item)
```

**Automatic Behavior:**

- Applies filter to individual messages and batches
- Only sends messages that pass the filter
- Handles both single messages and batch processing

### 4. `AggregatorWorker`

Base class for workers that collect data and emit results at EOF.

```python
class MyAggregatorWorker(AggregatorWorker):
    def process_item(self, item):
        # Accumulate data from item
        self.data.append(item)

    def compute_results(self):
        # Return final aggregated results
        return self.aggregate_data()
```

**Automatic Behavior:**

- Accumulates data during processing
- Emits results when EOF is received
- Prevents duplicate result emission

### 5. `MultiSourceWorker`

Base class for workers that consume from multiple input sources.

```python
additional_queues = {
    'users': 'users_queue',
    'stores': 'stores_queue'
}

class MyMultiSourceWorker(MultiSourceWorker):
    def __init__(self, config, additional_queues):
        super().__init__(config, additional_queues)

    def start_consuming(self):
        # Consume from additional sources first
        self.consume_additional_source('users', self.process_user)
        self.consume_additional_source('stores', self.process_store)

        # Then consume from main source
        super().start_consuming()
```

## Helper Functions

### Message Utilities

- `is_eof_message(message)` - Check if message is EOF marker
- `parse_output_queues(primary, env_var)` - Parse multi-queue configuration

### Type Conversion Utilities

- `safe_float_conversion(value, default=0.0)` - Safely convert to float
- `safe_int_conversion(value, default=0)` - Safely convert to int

### Date/Time Utilities

- `parse_datetime_safe(datetime_str, format_str)` - Safely parse datetime
- `extract_year_safe(datetime_str)` - Extract year from datetime string
- `extract_time_safe(datetime_str)` - Extract time from datetime string

### Worker Creation Utility

- `create_worker_main(worker_class, *args, **kwargs)` - Create standard main function

## Usage Examples

### Simple Filter Worker

```python
from worker_utils import FilterWorker, WorkerConfig, create_worker_main

class AmountFilterWorker(FilterWorker):
    def _initialize_worker(self):
        self.min_amount = 75.0

    def apply_filter(self, item):
        amount = safe_float_conversion(item.get('final_amount'))
        return amount >= self.min_amount

# Usage
config = WorkerConfig(
    input_queue_default='transactions_in',
    output_queue_default='transactions_out'
)

main = create_worker_main(AmountFilterWorker, config)
if __name__ == "__main__":
    main()
```

### Multi-Source Worker

```python
class TopClientsWorker(MultiSourceWorker):
    def _initialize_worker(self):
        self.store_lookup = {}
        self.user_lookup = {}
        self.counts = defaultdict(int)

    def start_consuming(self):
        # Load metadata first
        self.consume_additional_source('stores', self.process_store)
        self.consume_additional_source('users', self.process_user)

        # Process main data
        super().start_consuming()

# Usage
config = WorkerConfig(input_queue_default='transactions')
additional_queues = {'stores': 'stores_queue', 'users': 'users_queue'}

worker = TopClientsWorker(config, additional_queues)
worker.start_consuming()
```

## Migration Guide

To migrate existing workers to use the utils:

1. **Identify the worker type:**

   - Filter workers → Use `FilterWorker`
   - Aggregation workers → Use `AggregatorWorker`
   - Multi-source workers → Use `MultiSourceWorker`
   - Simple workers → Use `BaseWorker`

2. **Replace boilerplate code:**

   - Remove manual middleware setup
   - Remove signal handling code
   - Remove cleanup code
   - Replace with base class inheritance

3. **Implement required methods:**

   - `process_message()` and `process_batch()` for BaseWorker
   - `apply_filter()` for FilterWorker
   - `process_item()` and `compute_results()` for AggregatorWorker

4. **Use utility functions:**
   - Replace manual type conversions with safe\_\* functions
   - Use datetime utilities for date/time parsing
   - Use `create_worker_main()` for standardized main function

## Benefits

- **Reduced Code Duplication:** Common patterns abstracted into base classes
- **Consistent Interface:** All workers follow the same patterns
- **Better Error Handling:** Safe type conversion and parsing utilities
- **Easier Testing:** Clear separation of business logic from infrastructure
- **Maintainability:** Changes to common functionality only need to be made in one place
- **Configuration:** Standardized environment variable handling
