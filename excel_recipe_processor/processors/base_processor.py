"""
Base step processor for Excel automation recipes.

Defines the interface and common functionality that all step processors must implement.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)

step_desc = 'step_description'
proc_type = 'processor_type'

class StepProcessorError(Exception):
    """Raised when a step processor encounters an error during execution."""
    pass


class BaseStepProcessor(ABC):
    """
    Abstract base class for all step processors.
    
    All step types (filter_data, pivot_table, etc.) inherit from this class
    and implement the execute method.
    """
    
    def __init__(self, step_config: dict):
        """
        Initialize the step processor.
        
        Args:
            step_config: Dictionary containing step configuration from recipe
        """
        # Guard clause: step_config must be a dictionary
        if not isinstance(step_config, dict):
            raise StepProcessorError("Step configuration must be a dictionary")
        
        # Guard clause: step must have a type
        if proc_type not in step_config:
            raise StepProcessorError(f"Step configuration missing required '{proc_type}' field")
        
        step_type = step_config[proc_type]
        if not isinstance(step_type, str) or not step_type.strip():
            raise StepProcessorError(f"Step {proc_type} must be a non-empty string")
        
        self.step_config = step_config
        self.step_type = step_type
        self.step_name = step_config.get('step_description', f'Unnamed {step_type} step')
        
        # Guard clause: step_name must be a string if provided
        if 'step_description' in step_config and not isinstance(self.step_name, str):
            raise StepProcessorError("Step 'step_description' must be a string")
        
        logger.debug(f"Initialized {self.__class__.__name__} for step: {self.step_name}")
    
    @abstractmethod
    def execute(self, data: Any) -> Any:
        """
        Execute the step on the provided data.
        
        Args:
            data: Input data to process (typically pandas DataFrame)
            
        Returns:
            Processed data
            
        Raises:
            StepProcessorError: If step execution fails
        """
        pass
    
    def validate_required_fields(self, required_fields: list) -> None:
        """
        Validate that required configuration fields are present.
        
        Args:
            required_fields: List of required field names
            
        Raises:
            StepProcessorError: If any required fields are missing
        """
        # Guard clause: required_fields must be a list
        if not isinstance(required_fields, list):
            raise StepProcessorError("Required fields must be provided as a list")
        
        missing_fields = []
        for field in required_fields:
            # Guard clause: each field name must be a string
            if not isinstance(field, str):
                raise StepProcessorError(f"Field name must be a string, got: {type(field)}")
            
            if field not in self.step_config:
                missing_fields.append(field)
        
        if missing_fields:
            raise StepProcessorError(
                f"Step '{self.step_name}' missing required fields: {', '.join(missing_fields)}"
            )
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value with optional default.
        
        Args:
            key: Configuration key to retrieve
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        # Guard clause: key must be a string
        if not isinstance(key, str):
            raise StepProcessorError("Configuration key must be a string")
        
        return self.step_config.get(key, default)
    
    def validate_data_not_empty(self, data: Any) -> None:
        """
        Validate that input data is not None or empty.
        
        Args:
            data: Data to validate
            
        Raises:
            StepProcessorError: If data is None or empty
        """
        if data is None:
            raise StepProcessorError(f"Step '{self.step_name}' received None data")
        
        # Check for pandas DataFrame
        if hasattr(data, 'empty') and data.empty:
            raise StepProcessorError(f"Step '{self.step_name}' received empty DataFrame")
        
        # Check for lists
        if isinstance(data, list) and len(data) == 0:
            raise StepProcessorError(f"Step '{self.step_name}' received empty list")
        
        # Check for dictionaries
        if isinstance(data, dict) and len(data) == 0:
            raise StepProcessorError(f"Step '{self.step_name}' received empty dictionary")
    
    def log_step_start(self) -> None:
        """Log the start of step execution."""
        logger.info(f"Starting step: {self.step_name} ({self.step_type})")
    
    def log_step_complete(self, result_info: str = "") -> None:
        """
        Log the completion of step execution.
        
        Args:
            result_info: Optional information about the result
        """
        if result_info:
            logger.info(f"Completed step: {self.step_name} - {result_info}")
        else:
            logger.info(f"Completed step: {self.step_name}")
    
    def log_step_error(self, error: Exception) -> None:
        """
        Log step execution error.
        
        Args:
            error: Exception that occurred
        """
        logger.error(f"Error in step '{self.step_name}': {error}")
    
    def __str__(self) -> str:
        """String representation of the step processor."""
        return f"{self.__class__.__name__}(name='{self.step_name}', {proc_type}='{self.step_type}')"
    
    def __repr__(self) -> str:
        """Developer representation of the step processor."""
        return f"{self.__class__.__name__}(step_config={self.step_config})"


class StepProcessorRegistry:
    """
    Registry for mapping step types to their processor classes.
    
    This allows the system to dynamically create the correct processor
    for each step type defined in a recipe.
    """
    
    def __init__(self):
        """Initialize the registry."""
        self._processors = {}
    
    def register(self, step_type: str, processor_class: type) -> None:
        """
        Register a processor class for a step type.
        
        Args:
            step_type: String identifier for the step type
            processor_class: Class that inherits from BaseStepProcessor
            
        Raises:
            StepProcessorError: If registration is invalid
        """
        # Guard clauses
        if not isinstance(step_type, str) or not step_type.strip():
            raise StepProcessorError("Step type must be a non-empty string")
        
        if not isinstance(processor_class, type):
            raise StepProcessorError("Processor must be a class")
        
        if not issubclass(processor_class, BaseStepProcessor):
            raise StepProcessorError(
                f"Processor class {processor_class.__name__} must inherit from BaseStepProcessor"
            )
        
        self._processors[step_type] = processor_class
        logger.debug(f"Registered processor for step type: {step_type}")
    
    def get_processor_class(self, step_type: str) -> type:
        """
        Get the processor class for a step type.
        
        Args:
            step_type: Step type to look up
            
        Returns:
            Processor class
            
        Raises:
            StepProcessorError: If step type is not registered
        """
        # Guard clause
        if not isinstance(step_type, str):
            raise StepProcessorError("Step type must be a string")
        
        if step_type not in self._processors:
            available_types = list(self._processors.keys())
            raise StepProcessorError(
                f"Unknown step type: {step_type}. "
                f"Available types: {', '.join(available_types) if available_types else 'none'}"
            )
        
        return self._processors[step_type]
    
    def create_processor(self, step_config: dict) -> BaseStepProcessor:
        """
        Create a processor instance for a step configuration.
        
        Args:
            step_config: Step configuration dictionary
            
        Returns:
            Initialized processor instance
            
        Raises:
            StepProcessorError: If processor creation fails
        """
        # Guard clause
        if not isinstance(step_config, dict):
            raise StepProcessorError("Step configuration must be a dictionary")
        
        if proc_type not in step_config:
            raise StepProcessorError(f"Step configuration missing {proc_type} field")
        
        step_type = step_config[proc_type]
        processor_class = self.get_processor_class(step_type)
        
        try:
            return processor_class(step_config)
        except Exception as e:
            raise StepProcessorError(f"Failed to create processor for step type '{step_type}': {e}")
    
    def get_registered_types(self) -> list:
        """
        Get list of all registered step types.
        
        Returns:
            List of registered step type strings
        """
        return list(self._processors.keys())


# Global registry instance
registry = StepProcessorRegistry()
