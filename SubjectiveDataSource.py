from abc import ABC, abstractmethod
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveDataSource(ABC):
    def __init__(self, name=None, session=None, dependency_data_sources=[], subscribers=None, params=None):
        self.name = name
        self.session = session
        self.dependency_data_sources = dependency_data_sources
        self.subscribers = subscribers or []
        self.params = params or {}
        self.progress_callback = None  # Initialize the progress callback to None
        self.status_callback = None    # Initialize the status callback to None
        # progress variables normal to all datasources
        self._total_items = 0
        self._processed_items = 0
        self._total_processing_time = 0.0
        self._fetch_completed = False

    def start(self):
        for ds in self.dependency_data_sources:
            ds.subscribe(self)
            ds.start()

    def update(self, data):
        for subscriber in self.subscribers:
            subscriber.notify(data)

    def subscribe(self, subscriber):
        if subscriber not in self.subscribers:
            self.subscribers.append(subscriber)
            BBLogger.log(f"Subscriber {subscriber} added.")

    def get_name(self):
        class_name = self.__class__.__name__
        if self.name:
            return f"{self.name}_{class_name}"
        return class_name

    def get_data_source_type_name(self):
        """
        Returns the name of the data source type by removing
        'Subjective' and 'DataSource' substrings from the class name.
        """
        class_name = self.__class__.__name__
        data_source_type_name = class_name.replace('Subjective', '').replace('RealTimeDataSource', '').replace('DataSource', '')
        return data_source_type_name

    # === Abstract methods that must be implemented by each data source ===
    @abstractmethod
    def fetch(self):
        """Fetch data from the data source."""
        pass

    @abstractmethod
    def get_icon(self):
        """Return the SVG code for the data source icon."""
        pass

    @abstractmethod
    def get_connection_data(self):
        """
        Return the connection type and required fields for this data source.
        This should return a dictionary with connection type and fields.
        Example:
        {
            "connection_type": "AWS",
            "fields": ["region", "access_key", "secret_key", "target_directory"]
        }
        """
        pass

    # === Progress Tracking Methods ===

    def set_progress_callback(self, callback):
        """
        Set a callback function to receive progress updates.
        The callback should accept four arguments:
        data_source_name, total_items, processed_items, estimated_time
        """
        self.progress_callback = callback
        BBLogger.log(f"Progress callback set to: {callback}")

    def set_status_callback(self, callback):
        """
        Set a callback function to receive status updates.
        The callback should accept two arguments:
        data_source_name, status
        """
        self.status_callback = callback
        BBLogger.log(f"Status callback set to: {callback}")

    def estimated_remaining_time(self):
        """
        Returns an estimate of the time required to process the remaining items.
        Calculated as:
          remaining_to_process() * average_time_per_item()
        """
        remaining = self.remaining_to_process() * self.average_time_per_item()
        BBLogger.log(f"Estimated remaining time: {remaining} seconds")
        return remaining

    def get_total_to_process(self):
        BBLogger.log(f"Getting total items to process: {self._total_items}")
        return self._total_items

    def get_total_processed(self):
        BBLogger.log(f"Getting total processed items: {self._processed_items}")
        return self._processed_items

    def get_total_processing_time(self):
        BBLogger.log(f"Getting total processing time: {self._total_processing_time} seconds")
        return self._total_processing_time

    def remaining_to_process(self):
        """
        Returns the remaining number of items to process.
        By default, this is calculated as:
          total_to_process() - total_processed()
        """
        remaining = self.get_total_to_process() - self.get_total_processed()
        BBLogger.log(f"Remaining items to process: {remaining}")
        return remaining

    def increment_processed_items(self):
        new_processed = self.get_total_processed() + 1
        self.set_processed_items(new_processed)
        BBLogger.log(f"Incremented processed items to: {new_processed}")

    def set_total_items(self, total_items):
        self._total_items = total_items
        BBLogger.log(f"Set total items to: {total_items}")

    def set_processed_items(self, processed_items):
        self._processed_items = processed_items
        BBLogger.log(f"Set processed items to: {processed_items}")

    def set_total_processing_time(self, total_processing_time):
        self._total_processing_time = total_processing_time
        BBLogger.log(f"Set total processing time to: {total_processing_time} seconds")

    def set_fetch_completed(self, fetch_completed=False):
        self._fetch_completed = fetch_completed
        BBLogger.log(f"Set fetch completed to: {fetch_completed}")

    def average_time_per_item(self):
        if self.get_total_processed() == 0:
            BBLogger.log("No items processed yet; average time per item is 0.0 seconds")
            return 0.0
        avg = self.get_total_processing_time() / self.get_total_processed()
        BBLogger.log(f"Calculated average time per item: {avg} seconds")
        return avg
