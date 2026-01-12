"""
SubjectiveDataSourcePipeline

A pipeline orchestration system that acts as a SubjectiveDataSource itself.
This allows pipelines to be used like any other data source with dependencies,
subscribers, and the standard data source interface.

Can be instantiated from .pipe (JSON) files or configured programmatically.
"""

from typing import Dict, List, Optional, Callable, Any
from brainboost_data_source_logger_package.BBLogger import BBLogger
from .SubjectiveDataSource import SubjectiveDataSource
import importlib
import json
import os
import threading


class PipelineNode:
    """
    Represents a node in the data pipeline with its data source instance,
    dependencies, and data transformation logic.
    """

    def __init__(
        self,
        node_id: str,
        data_source_class,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ):
        """
        Initialize a pipeline node.

        Args:
            node_id: Unique identifier for this node
            data_source_class: The data source class (not instance)
            params: Parameters to pass to the data source constructor
            dependencies: List of node_ids this node depends on
            transform_fn: Optional function to transform data before passing to this node
            filter_fn: Optional function to filter which updates trigger this node
        """
        self.node_id = node_id
        self.data_source_class = data_source_class
        self.params = params or {}
        self.dependencies = dependencies or []
        self.transform_fn = transform_fn
        self.filter_fn = filter_fn
        self.instance = None

    def should_process(self, data: Any) -> bool:
        """Check if this node should process the given data."""
        if self.filter_fn is None:
            return True
        try:
            return self.filter_fn(data)
        except Exception as e:
            BBLogger.log(f"Filter function error in node {self.node_id}: {e}")
            return False

    def transform_data(self, data: Any) -> Any:
        """Transform the data before processing."""
        if self.transform_fn is None:
            return data
        try:
            return self.transform_fn(data)
        except Exception as e:
            BBLogger.log(f"Transform function error in node {self.node_id}: {e}")
            return data


class PipelineAdapter:
    """
    Adapter that connects a dependent data source to a dependency data source.
    Receives updates from the dependency and triggers the dependent data source.
    """

    def __init__(self, pipeline_node: PipelineNode, pipeline: 'SubjectiveDataSourcePipeline'):
        self.pipeline_node = pipeline_node
        self.pipeline = pipeline

    def update(self, data: Any):
        """
        Called when the dependency data source sends an update.
        Filters, transforms, and processes the data.
        """
        try:
            BBLogger.log(f"PipelineAdapter for {self.pipeline_node.node_id} received update: {data}")

            # Check if this node should process this data
            if not self.pipeline_node.should_process(data):
                BBLogger.log(f"Node {self.pipeline_node.node_id} filtered out update")
                return

            # Transform the data
            transformed_data = self.pipeline_node.transform_data(data)

            # Trigger the data source with the transformed data
            self.pipeline._trigger_node(self.pipeline_node.node_id, transformed_data)

        except Exception as e:
            BBLogger.log(f"Error in PipelineAdapter for {self.pipeline_node.node_id}: {e}")


class SubjectiveDataSourcePipeline(SubjectiveDataSource):
    """
    Pipeline orchestrator that inherits from SubjectiveDataSource.

    This allows the pipeline to be used like any other data source:
    - Can have dependencies
    - Can have subscribers
    - Implements fetch() method
    - Can be loaded from .pipe files

    Usage:
        # From .pipe file
        pipeline = SubjectiveDataSourcePipeline(
            name="VideoTranscription",
            params={"pipeline_file": "path/to/pipeline.pipe"}
        )
        pipeline.fetch()

        # Programmatic configuration
        pipeline = SubjectiveDataSourcePipeline(name="MyPipeline")
        pipeline.add_node(...)
        pipeline.fetch()
    """

    def __init__(
        self,
        name: str = None,
        session=None,
        dependency_data_sources: List[SubjectiveDataSource] = None,
        subscribers=None,
        params: Dict[str, Any] = None
    ):
        """
        Initialize the pipeline data source.

        Args:
            name: Name for this pipeline instance
            session: Session object (inherited from SubjectiveDataSource)
            dependency_data_sources: List of data sources this pipeline depends on
            subscribers: List of subscribers to notify
            params: Parameters including optional 'pipeline_file' path to .pipe file
        """
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params or {}
        )

        self.nodes: Dict[str, PipelineNode] = {}
        self.adapters: Dict[str, List[PipelineAdapter]] = {}
        self._started = False
        self._pipeline_config = None
        self._pipeline_file_path = None

        # Check if we should load from a .pipe file
        pipeline_file = self.params.get('pipeline_file')
        if pipeline_file:
            self._pipeline_file_path = pipeline_file
            self._load_from_pipe_file(pipeline_file)

        BBLogger.log(f"Pipeline '{self.get_name()}' initialized")

    def _load_from_pipe_file(self, file_path: str):
        """
        Load pipeline configuration from a .pipe (JSON) file.

        Args:
            file_path: Path to the .pipe file
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self._pipeline_config = json.load(f)

            BBLogger.log(f"Loaded pipeline configuration from {file_path}")

            # Build the pipeline from config
            self._build_from_config()

        except FileNotFoundError:
            raise ValueError(f"Pipeline file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in pipeline file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading pipeline file: {e}")

    def _build_from_config(self):
        """
        Build the pipeline from loaded configuration.
        """
        if not self._pipeline_config:
            raise ValueError("No pipeline configuration loaded")

        # Validate config structure
        if 'nodes' not in self._pipeline_config:
            raise ValueError("Pipeline config must have 'nodes' key")

        # Update name if specified in config
        if 'name' in self._pipeline_config and not self.name:
            self.name = self._pipeline_config['name']

        BBLogger.log(f"Building pipeline from config: {self.get_name()}")

        # Add each node
        nodes = self._pipeline_config['nodes']
        for node_config in nodes:
            self._add_node_from_config(node_config)

        BBLogger.log(f"Pipeline built with {len(nodes)} nodes")

    def _add_node_from_config(self, node_config: Dict[str, Any]):
        """
        Add a node to the pipeline from configuration.

        Expected node_config format:
        {
            "node_id": "unique_id",
            "module": "package.module",
            "class": "DataSourceClassName",
            "params": {...},
            "dependencies": ["other_node_id"],
            "filter": "python_expression",
            "transform": "python_expression"
        }
        """
        node_id = node_config.get('node_id')
        if not node_id:
            raise ValueError("Each node must have a 'node_id'")

        module_path = node_config.get('module')
        class_name = node_config.get('class')

        if not module_path or not class_name:
            raise ValueError(f"Node '{node_id}' must specify 'module' and 'class'")

        # Load the data source class
        try:
            module = importlib.import_module(module_path)
            data_source_class = getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            BBLogger.log(f"Failed to load class {class_name} from {module_path}: {e}")
            raise ValueError(f"Could not load data source class: {e}")

        # Get parameters
        params = node_config.get('params', {})

        # Get dependencies
        dependencies = node_config.get('dependencies', [])

        # Create filter function if specified
        filter_fn = None
        filter_expr = node_config.get('filter')
        if filter_expr:
            filter_fn = self._create_filter_function(filter_expr)

        # Create transform function if specified
        transform_fn = None
        transform_expr = node_config.get('transform')
        if transform_expr:
            transform_fn = self._create_transform_function(transform_expr)

        # Add the node
        self.add_node(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params,
            dependencies=dependencies,
            filter_fn=filter_fn,
            transform_fn=transform_fn
        )

        BBLogger.log(f"Added node '{node_id}' to pipeline")

    def _create_filter_function(self, filter_expr: str):
        """
        Create a filter function from a string expression.

        The expression should be a Python expression that evaluates to bool.
        The variable 'data' is available in the expression.

        Examples:
            "data.get('event_type') == 'created'"
            "str(data.get('path', '')).endswith('.mp4')"
        """
        def filter_fn(data):
            try:
                # Make 'data' available to the expression
                return eval(filter_expr, {"__builtins__": {}}, {"data": data, "str": str, "int": int, "float": float})
            except Exception as e:
                BBLogger.log(f"Error evaluating filter expression '{filter_expr}': {e}")
                return False

        return filter_fn

    def _create_transform_function(self, transform_expr: str):
        """
        Create a transform function from a string expression.

        The expression should be a Python expression that returns the transformed data.
        The variable 'data' is available in the expression.

        Examples:
            "data.get('path')"
            "{'video_path': data.get('path'), 'timestamp': data.get('timestamp')}"
        """
        def transform_fn(data):
            try:
                # Make 'data' available to the expression
                return eval(transform_expr, {"__builtins__": {}}, {"data": data, "str": str, "int": int, "float": float})
            except Exception as e:
                BBLogger.log(f"Error evaluating transform expression '{transform_expr}': {e}")
                return data

        return transform_fn

    def add_node(
        self,
        node_id: str,
        data_source_class,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ) -> 'SubjectiveDataSourcePipeline':
        """
        Add a node to the pipeline.

        Args:
            node_id: Unique identifier for this node
            data_source_class: The data source class (not instance)
            params: Parameters to pass to the data source constructor
            dependencies: List of node_ids this node depends on
            transform_fn: Optional function to transform data: (data) -> transformed_data
            filter_fn: Optional function to filter updates: (data) -> bool

        Returns:
            Self for method chaining
        """
        if node_id in self.nodes:
            raise ValueError(f"Node with id '{node_id}' already exists in pipeline")

        node = PipelineNode(
            node_id=node_id,
            data_source_class=data_source_class,
            params=params,
            dependencies=dependencies,
            transform_fn=transform_fn,
            filter_fn=filter_fn
        )

        self.nodes[node_id] = node
        BBLogger.log(f"Added node '{node_id}' to pipeline '{self.get_name()}'")

        return self

    def add_node_from_module(
        self,
        node_id: str,
        module_path: str,
        class_name: str,
        params: Optional[Dict[str, Any]] = None,
        dependencies: Optional[List[str]] = None,
        transform_fn: Optional[Callable] = None,
        filter_fn: Optional[Callable] = None
    ) -> 'SubjectiveDataSourcePipeline':
        """
        Add a node by dynamically loading a data source class from a module.

        Args:
            node_id: Unique identifier for this node
            module_path: Path to the Python module (e.g., 'package.module')
            class_name: Name of the data source class
            params: Parameters to pass to the data source constructor
            dependencies: List of node_ids this node depends on
            transform_fn: Optional function to transform data
            filter_fn: Optional function to filter updates

        Returns:
            Self for method chaining
        """
        try:
            module = importlib.import_module(module_path)
            data_source_class = getattr(module, class_name)

            return self.add_node(
                node_id=node_id,
                data_source_class=data_source_class,
                params=params,
                dependencies=dependencies,
                transform_fn=transform_fn,
                filter_fn=filter_fn
            )
        except (ImportError, AttributeError) as e:
            BBLogger.log(f"Failed to load data source class {class_name} from {module_path}: {e}")
            raise

    def _validate_dependencies(self):
        """Validate that all dependencies exist and there are no circular dependencies."""
        for node_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    raise ValueError(f"Node '{node_id}' depends on non-existent node '{dep_id}'")

        # Check for circular dependencies using DFS
        visited = set()
        rec_stack = set()

        def has_cycle(node_id):
            visited.add(node_id)
            rec_stack.add(node_id)

            for dep_id in self.nodes[node_id].dependencies:
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True

            rec_stack.remove(node_id)
            return False

        for node_id in self.nodes:
            if node_id not in visited:
                if has_cycle(node_id):
                    raise ValueError(f"Circular dependency detected involving node '{node_id}'")

    def build(self):
        """
        Build the pipeline by instantiating data sources and setting up dependencies.
        """
        if self._started:
            BBLogger.log(f"Pipeline '{self.get_name()}' already built")
            return

        BBLogger.log(f"Building pipeline '{self.get_name()}'")

        # Validate dependencies
        self._validate_dependencies()

        # Instantiate all data sources
        for node_id, node in self.nodes.items():
            try:
                BBLogger.log(f"Instantiating data source for node '{node_id}'")

                # Get dependency data source instances
                dependency_instances = [
                    self.nodes[dep_id].instance
                    for dep_id in node.dependencies
                ]

                # Create the data source instance
                node.instance = node.data_source_class(
                    name=node_id,
                    dependency_data_sources=dependency_instances,
                    params=node.params
                )

                BBLogger.log(f"Instantiated {node.data_source_class.__name__} for node '{node_id}'")

            except Exception as e:
                BBLogger.log(f"Failed to instantiate data source for node '{node_id}': {e}")
                raise

        # Set up adapters for nodes with dependencies
        for node_id, node in self.nodes.items():
            if node.dependencies:
                for dep_id in node.dependencies:
                    # Create an adapter for this dependency relationship
                    adapter = PipelineAdapter(node, self)

                    # Subscribe the adapter to the dependency data source
                    dependency_node = self.nodes[dep_id]
                    dependency_node.instance.subscribe(adapter)

                    # Track the adapter
                    if node_id not in self.adapters:
                        self.adapters[node_id] = []
                    self.adapters[node_id].append(adapter)

                    BBLogger.log(f"Connected node '{node_id}' to dependency '{dep_id}'")

        self._started = True
        BBLogger.log(f"Pipeline '{self.get_name()}' built successfully")

    def start(self):
        """
        Start the pipeline by starting all data sources.
        """
        if not self._started:
            self.build()

        BBLogger.log(f"Starting pipeline '{self.get_name()}'")

        # Start nodes in dependency order (dependencies first)
        started = set()

        def start_node(node_id):
            if node_id in started:
                return

            node = self.nodes[node_id]

            # Start dependencies first
            for dep_id in node.dependencies:
                start_node(dep_id)

            # Start this node
            try:
                BBLogger.log(f"Starting node '{node_id}'")

                # For real-time data sources, call fetch() which starts monitoring
                # For regular data sources, fetch() will process data
                if hasattr(node.instance, 'fetch'):
                    # Start in a separate thread for real-time sources to avoid blocking
                    thread = threading.Thread(
                        target=node.instance.fetch,
                        name=f"Pipeline-{self.get_name()}-{node_id}",
                        daemon=True
                    )
                    thread.start()

                    # Notify UI that node has started
                    self._notify_node_started(node_id, pid=thread.ident)

                started.add(node_id)
                BBLogger.log(f"Started node '{node_id}'")

            except Exception as e:
                BBLogger.log(f"Failed to start node '{node_id}': {e}")
                raise

        # Start all nodes
        for node_id in self.nodes:
            start_node(node_id)

        BBLogger.log(f"Pipeline '{self.get_name()}' started successfully")

    def stop(self):
        """
        Stop the pipeline by stopping all data sources.
        """
        BBLogger.log(f"Stopping pipeline '{self.get_name()}'")

        for node_id, node in self.nodes.items():
            try:
                if node.instance and hasattr(node.instance, 'stop'):
                    node.instance.stop()
                    BBLogger.log(f"Stopped node '{node_id}'")

                    # Notify UI that node has stopped
                    self._notify_node_stopped(node_id)

            except Exception as e:
                BBLogger.log(f"Error stopping node '{node_id}': {e}")

        self._started = False
        BBLogger.log(f"Pipeline '{self.get_name()}' stopped")

    def _trigger_node(self, node_id: str, data: Any):
        """
        Manually trigger a node with specific data.
        This is called by PipelineAdapters when dependency data arrives.

        Args:
            node_id: ID of the node to trigger
            data: Data to pass to the node
        """
        node = self.nodes.get(node_id)
        if not node or not node.instance:
            BBLogger.log(f"Cannot trigger node '{node_id}': not found or not instantiated")
            return

        try:
            BBLogger.log(f"Triggering node '{node_id}' with data: {data}")

            # If the data source has a process_input method, use it
            if hasattr(node.instance, 'process_input'):
                node.instance.process_input(data)
            # Otherwise, call update to notify its subscribers
            elif hasattr(node.instance, 'update'):
                node.instance.update(data)
            else:
                BBLogger.log(f"Node '{node_id}' has no process_input or update method")

        except Exception as e:
            BBLogger.log(f"Error triggering node '{node_id}': {e}")

    def get_node_instance(self, node_id: str):
        """
        Get the data source instance for a specific node.

        Args:
            node_id: ID of the node

        Returns:
            The data source instance or None if not found
        """
        node = self.nodes.get(node_id)
        return node.instance if node else None

    # === Pipeline UI Integration Methods ===

    def get_pipeline_nodes_for_ui(self) -> List[Dict[str, Any]]:
        """
        Extract pipeline node information for creating ConnectionListItems in UI.

        This method returns configuration for each node in the pipeline so that
        the UI can create corresponding connection list items.

        Returns:
            List of dicts with format:
            {
                "node_id": "monitor",
                "connection_name": "local_videos_monitor",
                "ds_class_name": "SubjectiveLocalFolderMonitorDataSource",
                "ds_type": "LocalFolderMonitor",
                "module": "SubjectiveLocalFolderMonitorDataSource",
                "params": {...},
                "dependencies": ["other_node_id"],
                "parent_pipeline": "video_transcript_pipeline"
            }
        """
        nodes_info = []

        for node_id, node in self.nodes.items():
            # Extract data source type name from class name
            ds_class_name = node.data_source_class.__name__
            ds_type = ds_class_name.replace('Subjective', '').replace('DataSource', '')

            # Get connection name (prefer from params, fallback to node_id)
            connection_name = node.params.get('connection_name', node_id)

            node_info = {
                'node_id': node_id,
                'connection_name': connection_name,
                'ds_class_name': ds_class_name,
                'ds_type': ds_type,
                'params': node.params.copy(),
                'dependencies': node.dependencies.copy(),
                'parent_pipeline': self.get_name() or self._pipeline_file_path or 'unknown'
            }

            # Try to get module name
            if hasattr(node.data_source_class, '__module__'):
                node_info['module'] = node.data_source_class.__module__
            else:
                node_info['module'] = ds_class_name

            nodes_info.append(node_info)

        return nodes_info

    @staticmethod
    def load_pipeline_nodes_from_file(pipeline_file_path: str) -> List[Dict[str, Any]]:
        """
        Load pipeline nodes from a .pipe file without instantiating the pipeline.

        This is a utility method for the UI to load node information before
        creating the pipeline data source.

        Args:
            pipeline_file_path: Path to .pipe JSON file

        Returns:
            List of node configurations (same format as get_pipeline_nodes_for_ui)
        """
        try:
            with open(pipeline_file_path, 'r', encoding='utf-8') as f:
                pipeline_config = json.load(f)

            nodes_info = []
            pipeline_name = pipeline_config.get('name', os.path.basename(pipeline_file_path))

            for node_config in pipeline_config.get('nodes', []):
                node_id = node_config.get('node_id')
                ds_class_name = node_config.get('class')
                params = node_config.get('params', {})

                # Extract ds_type from class name
                ds_type = ds_class_name.replace('Subjective', '').replace('DataSource', '')

                # Get connection name
                connection_name = params.get('connection_name', node_id)

                node_info = {
                    'node_id': node_id,
                    'connection_name': connection_name,
                    'ds_class_name': ds_class_name,
                    'ds_type': ds_type,
                    'module': node_config.get('module', ds_class_name),
                    'params': params,
                    'dependencies': node_config.get('dependencies', []),
                    'parent_pipeline': pipeline_name
                }

                nodes_info.append(node_info)

            return nodes_info

        except Exception as e:
            BBLogger.log(f"Error loading pipeline nodes from file: {e}")
            raise

    def _notify_node_started(self, node_id: str, pid: Any = None):
        """
        Publish event to Redis when a pipeline node starts.
        This allows the UI to update corresponding connection list items.

        Args:
            node_id: Node identifier
            pid: Process ID or thread ID of started node
        """
        try:
            import redis
            from brainboost_configuration_package.BBConfig import BBConfig

            redis_host = BBConfig.get('REDIS_SERVER_IP', 'localhost')
            redis_port = BBConfig.get('REDIS_SERVER_PORT', 6379)

            redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)

            event_data = {
                'pipeline_name': self.get_name(),
                'node_id': node_id,
                'connection_name': self.nodes[node_id].params.get('connection_name', node_id),
                'action': 'start',
                'pid': pid,
                'pipeline_file': self._pipeline_file_path
            }

            redis_client.publish('pipeline_node_events', json.dumps(event_data))
            BBLogger.log(f"Published node start event for '{node_id}': {event_data}")

        except Exception as e:
            BBLogger.log(f"Error publishing node start event: {e}")

    def _notify_node_stopped(self, node_id: str):
        """
        Publish event to Redis when a pipeline node stops.

        Args:
            node_id: Node identifier
        """
        try:
            import redis
            from brainboost_configuration_package.BBConfig import BBConfig

            redis_host = BBConfig.get('REDIS_SERVER_IP', 'localhost')
            redis_port = BBConfig.get('REDIS_SERVER_PORT', 6379)

            redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)

            event_data = {
                'pipeline_name': self.get_name(),
                'node_id': node_id,
                'connection_name': self.nodes[node_id].params.get('connection_name', node_id),
                'action': 'stop',
                'pipeline_file': self._pipeline_file_path
            }

            redis_client.publish('pipeline_node_events', json.dumps(event_data))
            BBLogger.log(f"Published node stop event for '{node_id}': {event_data}")

        except Exception as e:
            BBLogger.log(f"Error publishing node stop event: {e}")

    # === SubjectiveDataSource Abstract Method Implementations ===

    def fetch(self):
        """
        Fetch data by starting the pipeline.
        This implements the abstract fetch() method from SubjectiveDataSource.

        The pipeline will run until explicitly stopped or interrupted.
        """
        BBLogger.log(f"Fetch called on pipeline '{self.get_name()}'")

        # Build and start the pipeline
        self.start()

        BBLogger.log(f"Pipeline '{self.get_name()}' is running")

        # Keep running (for real-time pipelines)
        import time
        try:
            while self._started:
                time.sleep(1)
        except KeyboardInterrupt:
            BBLogger.log("Pipeline interrupted by user")
            self.stop()

    def get_icon(self) -> str:
        """
        Return SVG icon for pipeline data source.
        This implements the abstract get_icon() method from SubjectiveDataSource.
        """
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="3" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="15" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="9" y="14" width="6" height="6" rx="1" fill="#4F46E5"/>
            <path d="M6 10 L12 14 M18 10 L12 14" stroke="#4F46E5" stroke-width="2"/>
        </svg>'''

    def get_connection_data(self) -> dict:
        """
        Return connection configuration for pipeline data source.
        This implements the abstract get_connection_data() method from SubjectiveDataSource.
        """
        return {
            "connection_type": "PIPELINE",
            "fields": [
                {
                    "name": "pipeline_file",
                    "type": "text",
                    "label": "Pipeline File Path",
                    "description": "Path to .pipe (JSON) file defining the pipeline",
                    "required": False
                },
                {
                    "name": "pipeline_name",
                    "type": "text",
                    "label": "Pipeline Name",
                    "description": "Name for this pipeline instance",
                    "required": False
                }
            ]
        }
