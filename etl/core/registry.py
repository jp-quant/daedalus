"""
Transform Registry
==================

Registry for discovering and managing transforms.
"""

import logging
from typing import Callable, Optional, Type

from etl.core.base import BaseTransform
from etl.core.config import TransformConfig

logger = logging.getLogger(__name__)

# Type alias for transform factory
TransformFactory = Callable[[TransformConfig], BaseTransform]


class TransformRegistry:
    """
    Registry for transforms.
    
    Provides a central location to register and discover transforms.
    Supports both class-based and factory-based registration.
    
    Example:
        registry = TransformRegistry()
        
        # Register by class
        registry.register("orderbook_features", OrderbookFeatureTransform)
        
        # Register by factory
        @registry.register_factory("custom_transform")
        def create_custom(config: TransformConfig) -> BaseTransform:
            return CustomTransform(config)
        
        # Create transform instance
        transform = registry.create("orderbook_features", config)
    """
    
    def __init__(self):
        """Initialize empty registry."""
        self._transforms: dict[str, Type[BaseTransform]] = {}
        self._factories: dict[str, TransformFactory] = {}
    
    # =========================================================================
    # Registration
    # =========================================================================
    
    def register(
        self,
        name: str,
        transform_class: Type[BaseTransform],
    ) -> None:
        """
        Register a transform class.
        
        Args:
            name: Unique name for the transform.
            transform_class: Transform class to register.
        
        Raises:
            ValueError: If name is already registered.
        """
        if name in self._transforms or name in self._factories:
            raise ValueError(f"Transform '{name}' is already registered")
        
        self._transforms[name] = transform_class
        logger.debug(f"Registered transform: {name}")
    
    def register_factory(
        self,
        name: str,
    ) -> Callable[[TransformFactory], TransformFactory]:
        """
        Decorator to register a transform factory.
        
        Args:
            name: Unique name for the transform.
        
        Returns:
            Decorator function.
        
        Example:
            @registry.register_factory("my_transform")
            def create_my_transform(config):
                return MyTransform(config)
        """
        def decorator(factory: TransformFactory) -> TransformFactory:
            if name in self._transforms or name in self._factories:
                raise ValueError(f"Transform '{name}' is already registered")
            self._factories[name] = factory
            logger.debug(f"Registered transform factory: {name}")
            return factory
        return decorator
    
    def unregister(self, name: str) -> None:
        """
        Unregister a transform.
        
        Args:
            name: Name of transform to unregister.
        """
        self._transforms.pop(name, None)
        self._factories.pop(name, None)
    
    # =========================================================================
    # Discovery
    # =========================================================================
    
    def list_transforms(self) -> list[str]:
        """
        List all registered transform names.
        
        Returns:
            List of transform names.
        """
        return list(set(self._transforms.keys()) | set(self._factories.keys()))
    
    def has_transform(self, name: str) -> bool:
        """
        Check if a transform is registered.
        
        Args:
            name: Transform name to check.
        
        Returns:
            True if registered.
        """
        return name in self._transforms or name in self._factories
    
    def get_transform_class(self, name: str) -> Optional[Type[BaseTransform]]:
        """
        Get the class for a registered transform.
        
        Args:
            name: Transform name.
        
        Returns:
            Transform class or None if factory-registered.
        """
        return self._transforms.get(name)
    
    # =========================================================================
    # Creation
    # =========================================================================
    
    def create(
        self,
        name: str,
        config: TransformConfig,
    ) -> BaseTransform:
        """
        Create a transform instance.
        
        Args:
            name: Registered transform name.
            config: Transform configuration.
        
        Returns:
            Transform instance.
        
        Raises:
            ValueError: If transform is not registered.
        """
        if name in self._transforms:
            return self._transforms[name](config)
        elif name in self._factories:
            return self._factories[name](config)
        else:
            available = self.list_transforms()
            raise ValueError(
                f"Transform '{name}' is not registered. "
                f"Available transforms: {available}"
            )


# Global registry instance
_global_registry: Optional[TransformRegistry] = None


def get_registry() -> TransformRegistry:
    """
    Get the global transform registry.
    
    Creates the registry on first access.
    
    Returns:
        Global TransformRegistry instance.
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = TransformRegistry()
    return _global_registry


def register_transform(name: str):
    """
    Decorator to register a transform class with the global registry.
    
    Example:
        @register_transform("orderbook_features")
        class OrderbookFeatureTransform(BaseTransform):
            ...
    
    Args:
        name: Unique name for the transform.
    
    Returns:
        Class decorator.
    """
    def decorator(cls: Type[BaseTransform]) -> Type[BaseTransform]:
        get_registry().register(name, cls)
        return cls
    return decorator
