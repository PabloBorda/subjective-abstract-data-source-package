"""
SubjectivePipelineDataSource

A data source that loads and executes pipelines defined in JSON format.
This allows pipelines to be managed from the UI like any other data source.
"""

import json
import importlib
from typing import Dict, Any, Optional
from subjective_abstract_data_source_package import SubjectiveDataSource
from subjective_abstract_data_source_package.SubjectiveDataSourcePipeline import SubjectiveDataSourcePipeline
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectivePipelineDataSource(SubjectiveDataSource):
    """
    A data source that manages and executes pipelines defined in JSON.

    This allows pipelines to be:
    - Managed from the UI
    - Started/stopped like any data source
    - Configured using connection metadata
    - Defined declaratively without Python code
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None, subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params
        )

        self.pipeline = None
        self.pipeline_config = None
        self.pipeline_name = self.params.get('pipeline_name', 'Pipeline')

        BBLogger.log(f"SubjectivePipelineDataSource initialized: {self.pipeline_name}")

    def fetch(self):
        """
        Load and start the pipeline from configuration.
        """
        try:
            BBLogger.log("Starting pipeline data source")

            # Load pipeline configuration
            self._load_pipeline_config()

            # Build the pipeline from config
            self._build_pipeline_from_config()

            # Start the pipeline
            BBLogger.log(f"Starting pipeline: {self.pipeline_name}")
            self.pipeline.start()

            BBLogger.log(f"Pipeline {self.pipeline_name} is running")

            # Keep running (for real-time pipelines)
            import time
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                BBLogger.log("Pipeline interrupted by user")
                self.stop()

        except Exception as e:
            BBLogger.log(f"Error in pipeline data source: {e}")
            raise

    def stop(self):
        """
        Stop the pipeline.
        """
        if self.pipeline:
            BBLogger.log(f"Stopping pipeline: {self.pipeline_name}")
            self.pipeline.stop()
            BBLogger.log(f"Pipeline {self.pipeline_name} stopped")

    def get_icon(self):
        """
        Return SVG icon for pipeline data source.
        """
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <rect x="3" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="15" y="4" width="6" height="6" rx="1" fill="#4F46E5"/>
            <rect x="9" y="14" width="6" height="6" rx="1" fill="#4F46E5"/>
            <path d="M6 10 L12 14 M18 10 L12 14" stroke="#4F46E5" stroke-width="2"/>
        </svg>'''

    def get_connection_data(self):
        """
        Return connection configuration for pipeline data source.
        """
        return {
            "connection_type": "PIPELINE",
            "fields": [
                {
                    "name": "pipeline_name",
                    "type": "text",
                    "label": "Pipeline Name",
                    "description": "Name for this pipeline",
                    "required": True
                },
                {
                    "name": "pipeline_json",
                    "type": "textarea",
                    "label": "Pipeline Definition (JSON)",
                    "description": "JSON configuration defining the pipeline",
                    "required": True
                },
                {
                    "name": "pipeline_json_path",
                    "type": "text",
                    "label": "Pipeline JSON File Path (Alternative)",
                    "description": "Path to JSON file with pipeline definition",
                    "required": False
                }
            ]
        }

    def _load_pipeline_config(self):
        """
        Load pipeline configuration from params.
        """
        # Try to load from inline JSON first (support pipeline_json and pipeline_data)
        pipeline_json = self.params.get('pipeline_json') or self.params.get('pipeline_data')
        if pipeline_json:
            try:
                if isinstance(pipeline_json, str):
                    self.pipeline_config = json.loads(pipeline_json)
                else:
                    self.pipeline_config = pipeline_json
                BBLogger.log("Loaded pipeline config from inline JSON/data")
                return
            except json.JSONDecodeError as e:
                BBLogger.log(f"Error parsing inline pipeline JSON: {e}")
                raise ValueError(f"Invalid pipeline JSON: {e}")

        # Try to load from file path (support both pipeline_json_path and pipeline_file)
        pipeline_json_path = self.params.get('pipeline_json_path') or self.params.get('pipeline_file')
        if pipeline_json_path:
            try:
                with open(pipeline_json_path, 'r', encoding='utf-8') as f:
                    self.pipeline_config = json.load(f)
                BBLogger.log(f"Loaded pipeline config from file: {pipeline_json_path}")
                return
            except (FileNotFoundError, json.JSONDecodeError) as e:
                BBLogger.log(f"Error loading pipeline from file: {e}")
                raise ValueError(f"Could not load pipeline from file: {e}")

        raise ValueError("No pipeline configuration provided (pipeline_json, pipeline_json_path, or pipeline_file)")

    def _build_pipeline_from_config(self):
        """
        Build the pipeline from the loaded configuration.
        """
        if not self.pipeline_config:
            raise ValueError("No pipeline configuration loaded")

        # Validate config structure
        if 'nodes' not in self.pipeline_config:
            raise ValueError("Pipeline config must have 'nodes' key")

        # Create the pipeline
        pipeline_name = self.pipeline_config.get('name', self.pipeline_name)
        self.pipeline = SubjectiveDataSourcePipeline(name=pipeline_name)

        BBLogger.log(f"Building pipeline from config: {pipeline_name}")

        # Add each node
        nodes = self.pipeline_config['nodes']
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
        self.pipeline.add_node(
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

    def get_data_source_type_name(self):
        """
        Returns the name of the data source type.
        """
        return "Pipeline"
