from abc import abstractmethod
from brainboost_data_source_logger_package.BBLogger import BBLogger
from .SubjectiveDataSource import SubjectiveDataSource
from collections.abc import Callable
from typing import Any, Optional, List, Dict
import threading
import queue
import time


class SubjectiveOnDemandDataSource(SubjectiveDataSource):
    """
    Abstract base class for on-demand request/response data sources.

    This data source type works like a conversation or chat interface,
    where messages are sent and responses are received asynchronously.

    Subclasses must implement:
        - _process_message(message): Handle the incoming message and return a response
        - get_icon(): Return SVG icon for the data source
        - get_connection_data(): Return connection metadata

    Usage:
        datasource = MyOnDemandDataSource(params={...})
        datasource.set_response_callback(my_callback)
        datasource.send_message("Hello how are you today")
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None,
                 subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params
        )

        # Response callback for async responses
        self._response_callback: Optional[Callable[[Any], None]] = None

        # Message queue for async processing
        self._message_queue: queue.Queue = queue.Queue()

        # Processing thread
        self._processing_thread: Optional[threading.Thread] = None
        self._processing_active: bool = False

        # Conversation history for chat-like interactions
        self._conversation_history: List[Dict[str, Any]] = []

        # Configuration
        self._async_mode: bool = self.params.get("async_mode", True)
        self._max_history: int = self.params.get("max_history", 100)

        BBLogger.log(f"SubjectiveOnDemandDataSource initialized: {self.get_name()}")

    def fetch(self):
        """
        For on-demand data sources, fetch starts the message processing loop.
        This allows the data source to be used in pipelines.
        """
        BBLogger.log(f"OnDemand fetch called for {self.get_name()}")
        if self._async_mode:
            self._start_processing_loop()
        return {"status": "ready", "datasource": self.get_name()}

    def send_message(self, message: Any) -> Optional[Any]:
        """
        Send a message to the data source and get a response.

        In async mode, the response will be delivered via the response callback.
        In sync mode, this method blocks until the response is ready.

        Args:
            message: The message to send (string, dict, or any serializable object)

        Returns:
            In sync mode: The response from the data source
            In async mode: None (response delivered via callback)
        """
        BBLogger.log(f"Sending message to {self.get_name()}: {str(message)[:100]}...")

        # Add to conversation history
        self._add_to_history("user", message)

        if self._async_mode:
            # Queue the message for async processing
            self._message_queue.put(message)
            self._ensure_processing_loop()
            return None
        else:
            # Process synchronously
            return self._handle_message(message)

    def set_response_callback(self, callback: Callable[[Any], None]):
        """
        Set a callback function to receive responses.

        The callback will be called with the response data whenever
        a message is processed.

        Args:
            callback: Function that accepts response data
        """
        self._response_callback = callback
        BBLogger.log(f"Response callback set for {self.get_name()}")

    def get_conversation_history(self) -> List[Dict[str, Any]]:
        """
        Get the conversation history.

        Returns:
            List of conversation entries with role, content, and timestamp
        """
        return list(self._conversation_history)

    def clear_conversation_history(self):
        """Clear the conversation history."""
        self._conversation_history.clear()
        BBLogger.log(f"Conversation history cleared for {self.get_name()}")

    def _add_to_history(self, role: str, content: Any):
        """Add an entry to the conversation history."""
        entry = {
            "role": role,
            "content": content,
            "timestamp": time.time()
        }
        self._conversation_history.append(entry)

        # Trim history if needed
        if len(self._conversation_history) > self._max_history:
            self._conversation_history = self._conversation_history[-self._max_history:]

    def _handle_message(self, message: Any) -> Any:
        """
        Handle a message by processing it and delivering the response.

        Args:
            message: The message to process

        Returns:
            The response from _process_message
        """
        start_time = time.time()

        try:
            # Process the message (implemented by subclass)
            response = self._process_message(message)

            # Track processing time
            processing_time = time.time() - start_time
            self._total_processing_time += processing_time

            # Add response to history
            self._add_to_history("assistant", response)

            # Notify subscribers via update mechanism
            response_data = {
                "request": message,
                "response": response,
                "processing_time": processing_time,
                "timestamp": time.time()
            }
            self.update(response_data)

            # Call response callback if set
            if self._response_callback:
                try:
                    self._response_callback(response)
                except Exception as e:
                    BBLogger.log(f"Response callback failed: {e}")

            BBLogger.log(f"Message processed in {processing_time:.2f}s")
            return response

        except Exception as e:
            BBLogger.log(f"Error processing message: {e}")
            error_response = {"error": str(e), "message": message}
            self._add_to_history("error", error_response)

            if self._response_callback:
                try:
                    self._response_callback(error_response)
                except Exception as cb_error:
                    BBLogger.log(f"Error callback failed: {cb_error}")

            return error_response

    def _start_processing_loop(self):
        """Start the async message processing loop."""
        if self._processing_active:
            return

        self._processing_active = True
        self._processing_thread = threading.Thread(
            target=self._processing_loop,
            daemon=True,
            name=f"{self.get_name()}_processor"
        )
        self._processing_thread.start()
        BBLogger.log(f"Processing loop started for {self.get_name()}")

    def _ensure_processing_loop(self):
        """Ensure the processing loop is running."""
        if not self._processing_active or (
            self._processing_thread and not self._processing_thread.is_alive()
        ):
            self._start_processing_loop()

    def _processing_loop(self):
        """Main loop for async message processing."""
        BBLogger.log(f"Processing loop running for {self.get_name()}")

        while self._processing_active:
            try:
                # Wait for a message with timeout
                message = self._message_queue.get(timeout=1.0)
                self._handle_message(message)
                self._message_queue.task_done()
            except queue.Empty:
                # No message, continue waiting
                continue
            except Exception as e:
                BBLogger.log(f"Processing loop error: {e}")

    def stop(self):
        """Stop the message processing loop."""
        self._processing_active = False
        if self._processing_thread:
            self._processing_thread.join(timeout=5.0)
            self._processing_thread = None
        BBLogger.log(f"Processing loop stopped for {self.get_name()}")

    def wait_for_pending(self, timeout: Optional[float] = None):
        """
        Wait for all pending messages to be processed.

        Args:
            timeout: Maximum time to wait in seconds (None for no limit)
        """
        self._message_queue.join()

    # === Abstract methods that must be implemented by subclasses ===

    @abstractmethod
    def _process_message(self, message: Any) -> Any:
        """
        Process an incoming message and return a response.

        This method must be implemented by subclasses to define
        the actual message processing logic.

        Args:
            message: The incoming message to process

        Returns:
            The response to send back
        """
        pass

    @abstractmethod
    def get_icon(self) -> str:
        """Return the SVG code for the data source icon."""
        pass

    @abstractmethod
    def get_connection_data(self) -> dict:
        """
        Return the connection type and required fields for this data source.

        Example:
        {
            "connection_type": "ON_DEMAND",
            "fields": ["api_endpoint", "api_key"]
        }
        """
        pass
