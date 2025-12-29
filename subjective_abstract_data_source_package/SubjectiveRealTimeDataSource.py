# File: SubjectiveRealTimeDataSource.py

import asyncio
import json
import logging
import threading
import time
from abc import abstractmethod
from .SubjectiveDataSource import SubjectiveDataSource
from brainboost_data_tools_logger_package.BBLogger import BBLogger  # Ensure BBLogger is correctly implemented


class SubjectiveRealTimeDataSource(SubjectiveDataSource):
    # Internal host and port (not exposed to users)
    _HOST = 'localhost'
    _PORT = 65432  # Fixed port for simplicity; adjust as needed

    def __init__(self, name=None, session=None, dependency_data_sources=None, subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources,
            subscribers=subscribers,
            params=params
        )
        self._server = None
        self._connected_clients = set()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._monitoring_active = False
        self._monitoring_thread = None
        BBLogger.log("Asyncio event loop started in a separate thread.")

    def _run_loop(self):
        """Run the asyncio event loop."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _handle_client(self, reader, writer):
        """Handle incoming data connections."""
        addr = writer.get_extra_info('peername')
        BBLogger.log(f"Connection established from {addr}")
        self._connected_clients.add(writer)
        try:
            while True:
                data = await reader.readline()
                if not data:
                    BBLogger.log(f"Connection closed by {addr}")
                    break
                message = data.decode('utf-8').strip()
                if message:
                    try:
                        data_dict = json.loads(message)
                        BBLogger.log(f"Received data: {data_dict} from {addr}")
                        self.update(data_dict)  # Notify subscribers
                    except json.JSONDecodeError:
                        BBLogger.log(f"Invalid JSON received from {addr}: {message}", level='error')
        except asyncio.IncompleteReadError:
            BBLogger.log(f"Connection lost with {addr}", level='warning')
        finally:
            self._connected_clients.remove(writer)
            writer.close()
            await writer.wait_closed()
            BBLogger.log(f"Connection with {addr} closed.")

    async def _start_server_async(self):
        """Start the asyncio server."""
        self._server = await asyncio.start_server(
            self._handle_client, self._HOST, self._PORT, loop=self._loop
        )
        addr = self._server.sockets[0].getsockname()
        BBLogger.log(f"Real-time Data Source Server listening on {addr}")
        async with self._server:
            await self._server.serve_forever()

    def _start_server(self):
        """Start the server coroutine."""
        if not self._server:
            asyncio.run_coroutine_threadsafe(self._start_server_async(), self._loop)
            BBLogger.log("Real-time Data Source Server started.")

    def _send_mock_data(self):
        """Send mock data to all subscribers."""
        mock_data = {
            "timestamp": "2025-01-08T12:00:00.000000",
            "value": "Mock data update after 10 seconds"
        }
        BBLogger.log("Sending mock data update to subscribers.")
        self.update(mock_data)  # Notify subscribers

    def _schedule_mock_update(self):
        """Schedule sending mock data after a 10-second delay."""
        timer = threading.Timer(10, self._send_mock_data)
        timer.start()
        BBLogger.log("Scheduled mock data update to be sent after 10 seconds.")

    # Enhanced Real-Time Data Source Methods
    
    def start_monitoring(self):
        """
        Start real-time monitoring. This method should be implemented by subclasses
        to define their specific monitoring behavior (file system, network, etc.).
        """
        if self._monitoring_active:
            BBLogger.log("Monitoring is already active")
            return
            
        BBLogger.log(f"Starting monitoring for {self.__class__.__name__}")
        self._monitoring_active = True
        
        # Start the monitoring implementation
        try:
            self._initialize_monitoring()
            self._start_monitoring_implementation()
            BBLogger.log(f"Monitoring started successfully for {self.__class__.__name__}")
        except Exception as e:
            self._monitoring_active = False
            BBLogger.log(f"Failed to start monitoring: {e}")
            raise

    def stop_monitoring(self):
        """
        Stop real-time monitoring.
        """
        if not self._monitoring_active:
            BBLogger.log("Monitoring is not active")
            return
            
        BBLogger.log(f"Stopping monitoring for {self.__class__.__name__}")
        self._monitoring_active = False
        
        try:
            self._stop_monitoring_implementation()
            BBLogger.log(f"Monitoring stopped successfully for {self.__class__.__name__}")
        except Exception as e:
            BBLogger.log(f"Error while stopping monitoring: {e}")

    def _initialize_monitoring(self):
        """
        Initialize monitoring resources. Override in subclasses if needed.
        """
        pass

    def _start_monitoring_implementation(self):
        """
        Implementation-specific monitoring start. Override in subclasses.
        Default implementation starts the TCP server and mock data.
        """
        self._start_server()
        self._schedule_mock_update()

    def _stop_monitoring_implementation(self):
        """
        Implementation-specific monitoring stop. Override in subclasses.
        Default implementation stops the TCP server.
        """
        self._stop_server()

    def send_notification(self, notification_data):
        """
        Send a notification to all subscribers with the provided data.
        This is the main method subclasses should use to send real-time updates.
        
        :param notification_data: Dictionary containing the notification data
        """
        try:
            # Add timestamp if not present
            if 'timestamp' not in notification_data:
                notification_data['timestamp'] = time.time()
                
            BBLogger.log(f"Sending real-time notification: {notification_data}")
            self.update(notification_data)  # Notify subscribers
            
        except Exception as e:
            BBLogger.log(f"Error sending notification: {e}")

    def send_redis_notification(self, channel, notification_data):
        """
        Send a notification via Redis channel (if Redis is available).
        
        :param channel: Redis channel name
        :param notification_data: Dictionary containing the notification data
        """
        try:
            import redis
            from com_subjective_utils.config import config
            
            # Create Redis connection
            redis_host = config.get('REDIS_SERVER_IP', 'localhost')
            redis_port = config.get('REDIS_SERVER_PORT', 6379)
            
            redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
            
            # Add timestamp if not present
            if 'timestamp' not in notification_data:
                notification_data['timestamp'] = time.time()
                
            # Publish to Redis channel
            message = json.dumps(notification_data)
            redis_client.publish(channel, message)
            
            BBLogger.log(f"Sent Redis notification to channel '{channel}': {notification_data}")
            
        except ImportError:
            BBLogger.log("Redis not available for notifications")
        except Exception as e:
            BBLogger.log(f"Error sending Redis notification: {e}")

    def subscribe(self, subscriber):
        """
        Subscribe a subscriber and initiate listening.

        :param subscriber: An instance of BBSubscriber to be notified.
        """
        super().subscribe(subscriber)
        # Start monitoring when first subscriber is added
        if len(self.subscribers) == 1:
            self.start_monitoring()

    def _stop_server(self):
        """Stop the server and close all connections."""
        if self._server:
            self._server.close()
            asyncio.run_coroutine_threadsafe(self._server.wait_closed(), self._loop)
            BBLogger.log("Real-time Data Source Server closing.")
        for writer in self._connected_clients:
            writer.close()
            asyncio.run_coroutine_threadsafe(writer.wait_closed(), self._loop)
            BBLogger.log("Closed connection with a client.")
        # Stop the event loop
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        BBLogger.log("Real-time Data Source Server stopped and event loop terminated.")

    def stop(self):
        """Public method to gracefully shut down the data source."""
        self.stop_monitoring()
        self._stop_server()

    def fetch(self):
        """
        Implement the abstract fetch method. For real-time data sources,
        this typically starts monitoring rather than fetching static data.
        """
        BBLogger.log(f"Fetch called on real-time data source {self.__class__.__name__}")
        self.start_monitoring()

