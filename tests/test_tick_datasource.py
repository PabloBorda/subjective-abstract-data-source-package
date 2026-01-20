import pytest
import time
import threading
from unittest.mock import Mock, patch

from subjective_abstract_data_source_package import SubjectiveTickDataSource


class TestSubjectiveTickDataSourceInit:
    """Test initialization of TickDataSource."""

    def test_init_default_params(self):
        ds = SubjectiveTickDataSource()
        assert ds._interval == 1.0
        assert ds._tick_params == {}
        assert ds._tick_count_limit == -1
        assert ds._include_tick_number is True
        assert ds._include_elapsed_time is True
        assert ds._auto_start is True

    def test_init_custom_interval(self):
        ds = SubjectiveTickDataSource(params={"interval": 5.0})
        assert ds._interval == 5.0

    def test_init_custom_tick_count(self):
        ds = SubjectiveTickDataSource(params={"tick_count": 10})
        assert ds._tick_count_limit == 10

    def test_init_custom_tick_params(self):
        custom_params = {"source": "scheduler", "priority": "high"}
        ds = SubjectiveTickDataSource(params={"tick_params": custom_params})
        assert ds._tick_params == custom_params

    def test_init_disable_tick_number(self):
        ds = SubjectiveTickDataSource(params={"include_tick_number": False})
        assert ds._include_tick_number is False

    def test_init_disable_elapsed_time(self):
        ds = SubjectiveTickDataSource(params={"include_elapsed_time": False})
        assert ds._include_elapsed_time is False

    def test_init_disable_auto_start(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})
        assert ds._auto_start is False

    def test_init_with_name(self):
        ds = SubjectiveTickDataSource(name="my_ticker")
        assert ds.name == "my_ticker"

    def test_init_with_session(self):
        mock_session = Mock()
        ds = SubjectiveTickDataSource(session=mock_session)
        assert ds.session is mock_session


class TestTickDataBuilding:
    """Test tick data construction."""

    def test_build_tick_data_basic(self):
        ds = SubjectiveTickDataSource()
        ds._start_time = time.time()
        ds._tick_number = 5

        tick_data = ds._build_tick_data()

        assert tick_data["tick"] is True
        assert "timestamp" in tick_data
        assert tick_data["interval"] == 1.0
        assert tick_data["tick_number"] == 5
        assert "elapsed_time" in tick_data

    def test_build_tick_data_with_custom_params(self):
        ds = SubjectiveTickDataSource(params={
            "tick_params": {"custom_key": "custom_value"}
        })
        ds._start_time = time.time()

        tick_data = ds._build_tick_data()

        assert tick_data["custom_key"] == "custom_value"

    def test_build_tick_data_with_remaining_ticks(self):
        ds = SubjectiveTickDataSource(params={"tick_count": 10})
        ds._start_time = time.time()
        ds._tick_number = 3

        tick_data = ds._build_tick_data()

        assert tick_data["remaining_ticks"] == 7

    def test_build_tick_data_without_tick_number(self):
        ds = SubjectiveTickDataSource(params={"include_tick_number": False})
        ds._start_time = time.time()

        tick_data = ds._build_tick_data()

        assert "tick_number" not in tick_data

    def test_build_tick_data_without_elapsed_time(self):
        ds = SubjectiveTickDataSource(params={"include_elapsed_time": False})
        ds._start_time = time.time()

        tick_data = ds._build_tick_data()

        assert "elapsed_time" not in tick_data


class TestMonitoring:
    """Test tick monitoring functionality."""

    def test_start_monitoring_sets_active(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})
        assert ds._monitoring_active is False

        ds.start_monitoring()
        assert ds._monitoring_active is True

        ds.stop_monitoring()

    def test_stop_monitoring_clears_active(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})
        ds.start_monitoring()
        ds.stop_monitoring()

        assert ds._monitoring_active is False

    def test_tick_emits_to_subscribers(self):
        ds = SubjectiveTickDataSource(params={
            "interval": 0.1,
            "tick_count": 2,
            "auto_start": False
        })

        notifications = []
        subscriber = Mock()
        subscriber.notify = lambda data: notifications.append(data)

        ds.subscribe(subscriber)

        # Wait for ticks to complete
        time.sleep(0.5)
        ds.stop_monitoring()

        assert len(notifications) >= 2
        assert all(n["tick"] is True for n in notifications)

    def test_tick_count_limit_stops_monitoring(self):
        ds = SubjectiveTickDataSource(params={
            "interval": 0.05,
            "tick_count": 3,
            "auto_start": False
        })

        notifications = []
        subscriber = Mock()
        subscriber.notify = lambda data: notifications.append(data)

        ds.subscribe(subscriber)

        # Wait for ticks to complete
        time.sleep(0.5)
        ds.stop_monitoring()

        assert len(notifications) == 3


class TestImmediateTrigger:
    """Test immediate tick triggering."""

    def test_trigger_immediate_sends_notification(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})

        notifications = []
        subscriber = Mock()
        subscriber.notify = lambda data: notifications.append(data)

        ds.subscribe(subscriber)
        ds._start_time = time.time()
        ds.trigger_immediate()

        assert len(notifications) == 1
        assert notifications[0]["immediate"] is True

    def test_trigger_immediate_with_extra_params(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})

        notifications = []
        subscriber = Mock()
        subscriber.notify = lambda data: notifications.append(data)

        ds.subscribe(subscriber)
        ds._start_time = time.time()
        ds.trigger_immediate(extra_params={"reason": "manual"})

        assert notifications[0]["reason"] == "manual"

    def test_trigger_immediate_increments_tick_count(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})
        ds._start_time = time.time()

        initial_count = ds.get_tick_count()
        ds.trigger_immediate()

        assert ds.get_tick_count() == initial_count + 1


class TestRuntimeConfiguration:
    """Test runtime configuration changes."""

    def test_set_interval(self):
        ds = SubjectiveTickDataSource(params={"interval": 1.0})
        ds.set_interval(5.0)
        assert ds._interval == 5.0

    def test_set_tick_params(self):
        ds = SubjectiveTickDataSource()
        new_params = {"key": "value"}
        ds.set_tick_params(new_params)
        assert ds._tick_params == new_params

    def test_get_tick_count(self):
        ds = SubjectiveTickDataSource()
        ds._tick_number = 42
        assert ds.get_tick_count() == 42

    def test_get_elapsed_time_not_started(self):
        ds = SubjectiveTickDataSource()
        assert ds.get_elapsed_time() == 0.0

    def test_get_elapsed_time_after_start(self):
        ds = SubjectiveTickDataSource()
        ds._start_time = time.time() - 5.0
        elapsed = ds.get_elapsed_time()
        assert 4.9 <= elapsed <= 5.5


class TestFetch:
    """Test fetch method."""

    def test_fetch_returns_status_dict(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})
        result = ds.fetch()

        assert result["status"] == "ready"
        assert "datasource" in result
        assert "interval" in result
        assert "tick_count_limit" in result

    def test_fetch_auto_start_starts_monitoring(self):
        ds = SubjectiveTickDataSource(params={
            "auto_start": True,
            "tick_count": 1,
            "interval": 0.1
        })

        result = ds.fetch()
        assert result["status"] == "running"

        time.sleep(0.3)
        ds.stop_monitoring()

    def test_fetch_no_auto_start_does_not_start(self):
        ds = SubjectiveTickDataSource(params={"auto_start": False})
        result = ds.fetch()

        assert result["status"] == "ready"
        assert ds._monitoring_active is False


class TestAbstractMethods:
    """Test abstract method implementations."""

    def test_get_icon_returns_svg(self):
        ds = SubjectiveTickDataSource()
        icon = ds.get_icon()
        assert "<svg" in icon
        assert "</svg>" in icon

    def test_get_connection_data_returns_dict(self):
        ds = SubjectiveTickDataSource()
        conn_data = ds.get_connection_data()

        assert isinstance(conn_data, dict)
        assert conn_data["connection_type"] == "TICK"
        assert "interval" in conn_data["fields"]
        assert "tick_count" in conn_data["fields"]


class TestIntegration:
    """Integration tests for complete workflows."""

    def test_full_tick_workflow(self):
        """Test complete tick workflow with subscriber."""
        ds = SubjectiveTickDataSource(
            name="integration_ticker",
            params={
                "interval": 0.1,
                "tick_count": 5,
                "tick_params": {"source": "test"},
                "auto_start": False
            }
        )

        notifications = []
        subscriber = Mock()
        subscriber.notify = lambda data: notifications.append(data)

        ds.subscribe(subscriber)

        # Wait for all ticks
        time.sleep(1.0)
        ds.stop_monitoring()

        # Verify notifications
        assert len(notifications) == 5
        for i, notif in enumerate(notifications):
            assert notif["tick"] is True
            assert notif["source"] == "test"
            assert notif["tick_number"] == i

    def test_multiple_subscribers(self):
        """Test that all subscribers receive tick notifications."""
        ds = SubjectiveTickDataSource(params={
            "interval": 0.1,
            "tick_count": 2,
            "auto_start": False
        })

        notifications_1 = []
        notifications_2 = []

        subscriber1 = Mock()
        subscriber1.notify = lambda data: notifications_1.append(data)

        subscriber2 = Mock()
        subscriber2.notify = lambda data: notifications_2.append(data)

        ds.subscribe(subscriber1)
        ds.subscribe(subscriber2)

        time.sleep(0.5)
        ds.stop_monitoring()

        assert len(notifications_1) == 2
        assert len(notifications_2) == 2

    def test_stop_and_restart(self):
        """Test stopping and restarting tick monitoring."""
        ds = SubjectiveTickDataSource(params={
            "interval": 0.1,
            "auto_start": False
        })

        notifications = []
        subscriber = Mock()
        subscriber.notify = lambda data: notifications.append(data)

        ds.subscribe(subscriber)

        # First run
        ds.start_monitoring()
        time.sleep(0.25)
        ds.stop_monitoring()
        count_after_first = len(notifications)

        # Second run
        ds.start_monitoring()
        time.sleep(0.25)
        ds.stop_monitoring()
        count_after_second = len(notifications)

        assert count_after_second > count_after_first


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
