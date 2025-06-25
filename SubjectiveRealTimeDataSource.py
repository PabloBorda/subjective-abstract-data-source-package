# File: data_source_abstract/SubjectiveDataSource.py

import asyncio
import json
import logging
import threading
from data_source_abstract.SubjectiveDataSource import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger  # Ensure BBLogger is correctly implemented


class SubjectiveDataSource(SubjectiveDataSource):
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

    def subscribe(self, subscriber):
        """
        Subscribe a subscriber and initiate listening and mock data sending.

        :param subscriber: An instance of BBSubscriber to be notified.
        """
        super().subscribe(subscriber)
        self._start_server()
        self._schedule_mock_update()

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
        """Public method to gracefully shut down the server."""
        self._stop_server()

    def fetch(self):
        """Implement the abstract fetch method. Left empty as per requirements."""
        pass

