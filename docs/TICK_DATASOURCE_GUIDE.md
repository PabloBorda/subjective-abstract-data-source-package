# Tick DataSource Implementation Guide

## Overview

`SubjectiveTickDataSource` is a real-time data source that triggers dependent data sources at regular intervals. It emits "tick" events every N seconds, which can be used to schedule and coordinate downstream data sources in a pipeline.

Use this base class when you need to implement:
- Scheduled/periodic data fetching
- Time-based triggers for other data sources
- Polling mechanisms
- Heartbeat/keepalive signals
- Rate-limited batch processing

## Basic Usage

### Simple Tick Source

```python
from subjective_abstract_data_source_package import SubjectiveTickDataSource

# Create a tick source that fires every 5 seconds
tick_source = SubjectiveTickDataSource(
    name="scheduler",
    params={
        "interval": 5.0,  # Tick every 5 seconds
    }
)

# Subscribe a dependent data source
tick_source.subscribe(my_dependent_datasource)

# Start ticking (automatically starts on fetch() if auto_start=True)
tick_source.fetch()
```

### With Custom Parameters

```python
tick_source = SubjectiveTickDataSource(
    name="batch_trigger",
    params={
        "interval": 60.0,           # Tick every minute
        "tick_count": 10,           # Stop after 10 ticks
        "tick_params": {            # Custom data in each tick
            "source": "scheduler",
            "priority": "high"
        },
        "include_tick_number": True,   # Include tick count
        "include_elapsed_time": True,  # Include elapsed time
        "auto_start": False            # Don't start automatically
    }
)

# Manual start
tick_source.start_monitoring()
```

## Configuration Parameters

These parameters can be passed in the `params` dictionary:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `interval` | float | `1.0` | Time between ticks in seconds |
| `tick_count` | int | `-1` | Maximum ticks to emit (-1 = unlimited) |
| `tick_params` | dict | `{}` | Custom parameters included in each tick |
| `include_tick_number` | bool | `True` | Include tick count in notifications |
| `include_elapsed_time` | bool | `True` | Include elapsed time in notifications |
| `auto_start` | bool | `True` | Start ticking on fetch() |

### Reconnection Parameters (inherited from RealTimeDataSource)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `reconnect_initial_delay` | float | `1.0` | Initial reconnect delay |
| `reconnect_max_delay` | float | `30.0` | Maximum reconnect delay |
| `reconnect_jitter` | float | `0.2` | Jitter factor for delays |
| `reconnect_max_attempts` | int | `-1` | Max reconnect attempts (-1 = unlimited) |

## Tick Notification Data

Each tick notification includes:

```json
{
  "tick": true,
  "timestamp": 1737120645.123,
  "interval": 5.0,
  "tick_number": 42,
  "elapsed_time": 210.5,
  "remaining_ticks": 8,
  "custom_key": "custom_value"
}
```

| Field | Description |
|-------|-------------|
| `tick` | Always `true`, identifies this as a tick event |
| `timestamp` | Unix timestamp when tick was generated |
| `interval` | Current tick interval in seconds |
| `tick_number` | Count of ticks since start (if enabled) |
| `elapsed_time` | Seconds since monitoring started (if enabled) |
| `remaining_ticks` | Remaining ticks until limit (if limit set) |
| `...tick_params` | Any custom parameters from configuration |

## Pipeline Integration

### Using in a Pipeline Configuration

```json
{
  "nodes": [
    {
      "node_id": "tick_trigger",
      "class": "SubjectiveTickDataSource",
      "params": {
        "interval": 30,
        "tick_params": {
          "action": "fetch_data"
        }
      }
    },
    {
      "node_id": "api_fetcher",
      "class": "MyAPIDataSource",
      "dependencies": ["tick_trigger"],
      "params": {
        "endpoint": "https://api.example.com/data"
      }
    }
  ]
}
```

### Programmatic Pipeline Setup

```python
from subjective_abstract_data_source_package import SubjectiveTickDataSource

# Create tick source
tick_source = SubjectiveTickDataSource(
    name="scheduler",
    params={"interval": 10.0}
)

# Create dependent data source
api_source = MyAPIDataSource(params={"endpoint": "..."})

# Wire them together
tick_source.subscribe(api_source)

# Start the pipeline
tick_source.fetch()
```

## Runtime Control

### Manual Tick Triggering

```python
# Trigger an immediate tick outside the regular interval
tick_source.trigger_immediate()

# Trigger with extra parameters for this tick only
tick_source.trigger_immediate(extra_params={
    "reason": "manual_request",
    "priority": "urgent"
})
```

### Changing Interval at Runtime

```python
# Update the tick interval (takes effect on next tick cycle)
tick_source.set_interval(30.0)  # Change to 30 seconds
```

### Updating Tick Parameters

```python
# Change the parameters included in tick notifications
tick_source.set_tick_params({
    "mode": "production",
    "batch_size": 100
})
```

### Querying State

```python
# Get current tick count
count = tick_source.get_tick_count()

# Get elapsed time since start
elapsed = tick_source.get_elapsed_time()
```

### Start/Stop Control

```python
# Start monitoring manually
tick_source.start_monitoring()

# Stop monitoring
tick_source.stop_monitoring()

# Or use stop() for graceful shutdown
tick_source.stop()
```

## Implementing a Custom Tick Source

If you need specialized tick behavior, you can extend `SubjectiveTickDataSource`:

```python
from subjective_abstract_data_source_package import SubjectiveTickDataSource


class AdaptiveTickDataSource(SubjectiveTickDataSource):
    """
    A tick source that adjusts interval based on conditions.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._base_interval = self._interval
        self._load_factor = 1.0

    def _build_tick_data(self):
        """Override to add custom data to ticks."""
        tick_data = super()._build_tick_data()
        tick_data["load_factor"] = self._load_factor
        tick_data["adaptive_interval"] = self._interval
        return tick_data

    def adjust_for_load(self, load_factor):
        """Adjust tick interval based on system load."""
        self._load_factor = load_factor
        new_interval = self._base_interval * load_factor
        self.set_interval(new_interval)
```

## Use Cases

### 1. Scheduled Data Polling

```python
# Poll an API every 5 minutes
tick_source = SubjectiveTickDataSource(
    params={
        "interval": 300,  # 5 minutes
        "tick_params": {"action": "poll"}
    }
)
tick_source.subscribe(api_poller)
tick_source.fetch()
```

### 2. Rate-Limited Processing

```python
# Process items at a controlled rate
tick_source = SubjectiveTickDataSource(
    params={
        "interval": 1.0,  # One tick per second
        "tick_count": 100  # Process 100 items
    }
)
tick_source.subscribe(rate_limited_processor)
tick_source.fetch()
```

### 3. Heartbeat/Keepalive

```python
# Send heartbeats every 30 seconds
tick_source = SubjectiveTickDataSource(
    name="heartbeat",
    params={
        "interval": 30,
        "tick_params": {"type": "heartbeat"}
    }
)
tick_source.subscribe(connection_monitor)
tick_source.fetch()
```

### 4. Batch Processing Coordinator

```python
# Trigger batch processing every hour
tick_source = SubjectiveTickDataSource(
    params={
        "interval": 3600,  # 1 hour
        "tick_params": {
            "batch_type": "hourly",
            "include_metrics": True
        }
    }
)
tick_source.subscribe(batch_processor)
tick_source.fetch()
```

## Available Methods

### Core Methods

| Method | Description |
|--------|-------------|
| `fetch()` | Start tick source (respects auto_start) |
| `start_monitoring()` | Start emitting ticks |
| `stop_monitoring()` | Stop emitting ticks |
| `stop()` | Graceful shutdown |
| `trigger_immediate(extra_params)` | Trigger tick immediately |

### Configuration Methods

| Method | Description |
|--------|-------------|
| `set_interval(interval)` | Update tick interval |
| `set_tick_params(params)` | Update tick parameters |

### Query Methods

| Method | Description |
|--------|-------------|
| `get_tick_count()` | Get number of ticks emitted |
| `get_elapsed_time()` | Get time since start |

### Inherited Methods

| Method | Description |
|--------|-------------|
| `subscribe(subscriber)` | Add subscriber for tick events |
| `send_notification(data)` | Send data to subscribers |
| `get_name()` | Get data source name |
| `get_icon()` | Get SVG icon |
| `get_connection_data()` | Get connection metadata |

## Checklist for Usage

- [ ] Set appropriate `interval` for your use case
- [ ] Consider using `tick_count` for finite operations
- [ ] Add relevant `tick_params` for downstream processing
- [ ] Subscribe dependent data sources before starting
- [ ] Call `stop()` or `stop_monitoring()` when done
- [ ] Handle tick data in subscriber's `notify()` method
