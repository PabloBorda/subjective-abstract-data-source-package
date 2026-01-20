# File: SubjectiveTickDataSource.py

import time
import threading
from .SubjectiveRealTimeDataSource import SubjectiveRealTimeDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveTickDataSource(SubjectiveRealTimeDataSource):
    """
    A real-time data source that triggers dependent data sources at regular intervals.

    This data source emits "tick" events every N seconds, which can be used to
    trigger downstream data sources in a pipeline. Each tick notification includes
    configurable parameters that are passed to dependent data sources.

    Parameters (via params dict):
        interval (float): Time between ticks in seconds. Default: 1.0
        tick_params (dict): Additional parameters to include in each tick notification.
        tick_count (int): Maximum number of ticks to emit. -1 for unlimited. Default: -1
        include_tick_number (bool): Whether to include tick count in notifications. Default: True
        include_elapsed_time (bool): Whether to include elapsed time in notifications. Default: True
        auto_start (bool): Whether to start ticking immediately on fetch(). Default: True

    Example usage:
        ```python
        tick_source = SubjectiveTickDataSource(
            name="my_ticker",
            params={
                "interval": 5.0,  # Tick every 5 seconds
                "tick_params": {"source": "scheduler"},
                "tick_count": 10  # Stop after 10 ticks
            }
        )

        # Subscribe a dependent data source
        tick_source.subscribe(my_dependent_datasource)

        # Start ticking
        tick_source.start_monitoring()
        ```
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None,
                 subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources,
            subscribers=subscribers,
            params=params
        )

        # Tick configuration
        self._interval = self._coerce_float_param("interval", 1.0)
        self._tick_params = self.params.get("tick_params", {}) or {}
        self._tick_count_limit = self._coerce_int_param("tick_count", -1)
        self._include_tick_number = self._coerce_bool_param("include_tick_number", True)
        self._include_elapsed_time = self._coerce_bool_param("include_elapsed_time", True)
        self._auto_start = self._coerce_bool_param("auto_start", True)

        # Runtime state
        self._tick_number = 0
        self._start_time = None
        self._stop_event = threading.Event()

        BBLogger.log(f"SubjectiveTickDataSource initialized with interval={self._interval}s")

    def _coerce_bool_param(self, key, default):
        """Convert parameter to boolean."""
        try:
            value = self.params.get(key, default)
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        except (TypeError, ValueError):
            return default

    def _initialize_monitoring(self):
        """Initialize tick monitoring resources."""
        self._tick_number = 0
        self._start_time = time.time()
        self._stop_event.clear()
        BBLogger.log(f"Tick monitoring initialized for {self.__class__.__name__}")

    def _start_monitoring_implementation(self):
        """
        Main tick loop that emits notifications at regular intervals.

        This runs in a background thread and sends tick notifications to all
        subscribers every `interval` seconds.
        """
        BBLogger.log(f"Starting tick loop with interval={self._interval}s")

        while self._monitoring_active and not self._stop_event.is_set():
            # Check tick count limit
            if self._tick_count_limit > 0 and self._tick_number >= self._tick_count_limit:
                BBLogger.log(f"Tick count limit reached ({self._tick_count_limit}), stopping")
                break

            # Build tick notification data
            tick_data = self._build_tick_data()

            # Send notification to subscribers
            self.send_notification(tick_data)

            self._tick_number += 1

            # Wait for next tick interval (using Event.wait for interruptible sleep)
            if self._stop_event.wait(timeout=self._interval):
                # Stop event was set, exit the loop
                break

        BBLogger.log(f"Tick loop ended after {self._tick_number} ticks")

    def _stop_monitoring_implementation(self):
        """Stop the tick loop."""
        self._stop_event.set()
        BBLogger.log(f"Tick monitoring stop requested for {self.__class__.__name__}")

    def _build_tick_data(self):
        """
        Build the tick notification data dictionary.

        Returns:
            dict: Tick data including timestamp, configured params, and optional metrics
        """
        tick_data = {
            "tick": True,
            "timestamp": time.time(),
            "interval": self._interval,
        }

        # Add tick number if configured
        if self._include_tick_number:
            tick_data["tick_number"] = self._tick_number

        # Add elapsed time if configured
        if self._include_elapsed_time and self._start_time is not None:
            tick_data["elapsed_time"] = time.time() - self._start_time

        # Add tick count limit info if set
        if self._tick_count_limit > 0:
            tick_data["remaining_ticks"] = self._tick_count_limit - self._tick_number

        # Merge in user-provided tick parameters
        if self._tick_params:
            tick_data.update(self._tick_params)

        return tick_data

    def get_tick_count(self):
        """
        Get the current tick count.

        Returns:
            int: Number of ticks emitted since monitoring started
        """
        return self._tick_number

    def get_elapsed_time(self):
        """
        Get elapsed time since monitoring started.

        Returns:
            float: Elapsed time in seconds, or 0 if not started
        """
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time

    def set_interval(self, interval):
        """
        Update the tick interval.

        Note: Takes effect on the next tick cycle.

        Args:
            interval (float): New interval in seconds
        """
        self._interval = float(interval)
        BBLogger.log(f"Tick interval updated to {self._interval}s")

    def set_tick_params(self, tick_params):
        """
        Update the parameters included in tick notifications.

        Args:
            tick_params (dict): Parameters to merge into tick notifications
        """
        self._tick_params = tick_params or {}
        BBLogger.log(f"Tick params updated: {self._tick_params}")

    def trigger_immediate(self, extra_params=None):
        """
        Trigger an immediate tick notification outside the regular interval.

        This is useful for manually triggering dependent data sources.

        Args:
            extra_params (dict, optional): Additional parameters to include in this tick only
        """
        tick_data = self._build_tick_data()
        tick_data["immediate"] = True

        if extra_params:
            tick_data.update(extra_params)

        BBLogger.log(f"Triggering immediate tick notification")
        self.send_notification(tick_data)
        self._tick_number += 1

    def fetch(self):
        """
        Start the tick data source.

        If auto_start is True (default), begins emitting ticks immediately.
        Returns status information about the tick source.
        """
        BBLogger.log(f"Fetch called on tick data source {self.__class__.__name__}")

        if self._auto_start:
            self.start_monitoring()

        return {
            "status": "running" if self._monitoring_active else "ready",
            "datasource": self.get_name(),
            "interval": self._interval,
            "tick_count_limit": self._tick_count_limit
        }

    def get_icon(self) -> str:
        """Return SVG icon for the tick data source."""
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
            <polyline points="12,6 12,12 16,14" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
        </svg>'''

    def get_connection_data(self) -> dict:
        """Return connection configuration for the tick data source."""
        return {
            "connection_type": "TICK",
            "fields": [
                "interval",
                "tick_count",
                "tick_params",
                "include_tick_number",
                "include_elapsed_time",
                "auto_start"
            ]
        }
