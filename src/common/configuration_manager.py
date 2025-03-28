#!/usr/bin/env python3
"""
Configuration Manager for ClusterSentry

This module provides a centralized configuration management system for ClusterSentry
components with version tracking, validation, and distributed updates.
"""

import os
import sys
import json
import yaml
import logging
import threading
import time
import redis
import jsonschema
from pathlib import Path
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Callable, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("config_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ConfigManager")

class ConfigChangeType(Enum):
    """Types of configuration changes"""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"

class ConfigVersion:
    """Semantic versioning for configurations"""
    
    def __init__(self, major: int = 0, minor: int = 1, patch: int = 0):
        """Initialize with version number"""
        self.major = major
        self.minor = minor
        self.patch = patch
    
    def __str__(self) -> str:
        """String representation (semver)"""
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def __eq__(self, other) -> bool:
        """Check if versions are equal"""
        if not isinstance(other, ConfigVersion):
            return False
        return (self.major == other.major and 
                self.minor == other.minor and 
                self.patch == other.patch)
    
    def __lt__(self, other) -> bool:
        """Check if this version is lower than the other"""
        if not isinstance(other, ConfigVersion):
            return NotImplemented
        
        if self.major != other.major:
            return self.major < other.major
        
        if self.minor != other.minor:
            return self.minor < other.minor
        
        return self.patch < other.patch
    
    @classmethod
    def from_string(cls, version_str: str) -> 'ConfigVersion':
        """Create from string representation"""
        try:
            parts = version_str.split('.')
            if len(parts) >= 3:
                return cls(int(parts[0]), int(parts[1]), int(parts[2]))
            elif len(parts) == 2:
                return cls(int(parts[0]), int(parts[1]))
            elif len(parts) == 1:
                return cls(int(parts[0]))
            else:
                return cls()
        except (ValueError, AttributeError):
            logger.warning(f"Invalid version string: {version_str}, using default")
            return cls()

class ConfigurationManager:
    """
    Centralized configuration management system for ClusterSentry.
    
    Features:
    - Component-specific configurations
    - Version tracking with semantic versioning
    - Schema-based validation
    - Distributed notifications for configuration changes
    - Local and Redis-based storage
    - Configuration history tracking
    """
    
    def __init__(self, base_directory: Union[str, Path] = "config", 
                 redis_config: Optional[Dict[str, Any]] = None,
                 namespace: str = "clustersentry"):
        """
        Initialize the configuration manager
        
        Args:
            base_directory: Base directory for configuration files
            redis_config: Redis connection configuration (host, port, password, etc.)
            namespace: Namespace for Redis keys
        """
        # Set up base directory
        self.base_directory = Path(base_directory)
        self.base_directory.mkdir(parents=True, exist_ok=True)
        
        # Redis configuration
        self.redis_config = redis_config or {}
        self.namespace = namespace
        self.redis_client = None
        
        if self.redis_config:
            try:
                self.redis_client = redis.Redis(
                    host=self.redis_config.get("host", "localhost"),
                    port=self.redis_config.get("port", 6379),
                    password=self.redis_config.get("password"),
                    db=self.redis_config.get("db", 0),
                    decode_responses=True
                )
                # Test connection
                self.redis_client.ping()
                logger.info("Connected to Redis")
            except redis.RedisError as e:
                logger.warning(f"Redis connection failed: {e}. Operating in local-only mode.")
                self.redis_client = None
        
        # In-memory cache
        self.config_cache = {}  # {component_name: config_dict}
        self.version_cache = {}  # {component_name: ConfigVersion}
        self.schema_cache = {}  # {component_name: schema_dict}
        
        # Change listeners
        self.listeners = {}  # {component_name: [callback_fn]}
        
        # Locks for thread safety
        self.cache_lock = threading.RLock()
        self.listener_lock = threading.RLock()
        
        # Set up Redis subscription for distributed updates if Redis is available
        self.pubsub = None
        self.pubsub_thread = None
        
        if self.redis_client:
            self._setup_redis_subscription()
        
        logger.info(f"Configuration Manager initialized with base directory: {self.base_directory}")
    
    def _setup_redis_subscription(self):
        """Set up Redis pub/sub for configuration updates"""
        try:
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(f"{self.namespace}:config_updates")
            
            # Start pubsub thread
            self.pubsub_thread = threading.Thread(
                target=self._pubsub_listener,
                daemon=True
            )
            self.pubsub_thread.start()
            logger.info("Redis pub/sub subscription established")
        except Exception as e:
            logger.error(f"Failed to set up Redis subscription: {e}")
    
    def _pubsub_listener(self):
        """Listen for configuration updates from Redis pub/sub"""
        try:
            while True:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        data = json.loads(message["data"])
                        component_name = data.get("component")
                        change_type = data.get("type")
                        source = data.get("source")
                        
                        # Ignore our own messages
                        if source == id(self):
                            continue
                        
                        logger.debug(f"Received config update for {component_name} ({change_type})")
                        
                        # Invalidate cache
                        with self.cache_lock:
                            if component_name in self.config_cache:
                                del self.config_cache[component_name]
                                if component_name in self.version_cache:
                                    del self.version_cache[component_name]
                                
                                # Reload from Redis or file
                                self.get_configuration(component_name)
                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON in configuration update")
                    except Exception as e:
                        logger.error(f"Error processing configuration update: {e}")
                
                time.sleep(0.1)  # Prevent CPU spinning
        except Exception as e:
            logger.error(f"Error in Redis pub/sub listener: {e}")
    
    def close(self):
        """Clean up resources"""
        if self.pubsub:
            self.pubsub.unsubscribe()
        
        if self.redis_client:
            self.redis_client.close()
    
    def get_configuration(self, component_name: str, default_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get configuration for a component
        
        Args:
            component_name: Name of the component
            default_config: Default configuration to use if none exists
            
        Returns:
            Configuration dictionary
        """
        with self.cache_lock:
            # Check if we have a cached version
            if component_name in self.config_cache:
                return self.config_cache[component_name]
            
            # Try to load from Redis
            config = None
            version = None
            
            if self.redis_client:
                config = self._load_from_redis(component_name)
                if config:
                    logger.debug(f"Loaded configuration for {component_name} from Redis")
            
            # If not in Redis, try to load from file
            if not config:
                config, version = self._load_from_file(component_name)
                if config:
                    logger.debug(f"Loaded configuration for {component_name} from file")
            
            # If still not found, use default
            if not config:
                if default_config:
                    logger.info(f"Using default configuration for {component_name}")
                    config = default_config.copy()
                    version = ConfigVersion()
                    
                    # Save default configuration
                    self._save_to_file(component_name, config, version)
                    self._save_to_redis(component_name, config, version)
                else:
                    logger.warning(f"No configuration found for {component_name} and no default provided")
                    config = {}
                    version = ConfigVersion()
            
            # Extract version if not already done
            if not version and config:
                version_str = config.get("_version")
                version = ConfigVersion.from_string(version_str) if version_str else ConfigVersion()
            
            # Cache the configuration
            self.config_cache[component_name] = config
            self.version_cache[component_name] = version
            
            return config
    
    def set_configuration(self, component_name: str, config: Dict[str, Any], 
                          increment_version: str = "patch",
                          validate: bool = True,
                          notify: bool = True) -> bool:
        """
        Set configuration for a component
        
        Args:
            component_name: Name of the component
            config: Configuration to set
            increment_version: Version increment type (major, minor, patch, none)
            validate: Whether to validate against schema
            notify: Whether to notify listeners and other instances
            
        Returns:
            True if successful, False otherwise
        """
        with self.cache_lock:
            # Check if we need to validate
            if validate and component_name in self.schema_cache:
                schema = self.schema_cache[component_name]
                is_valid, errors = self.validate_configuration(config, schema)
                if not is_valid:
                    logger.error(f"Configuration for {component_name} failed validation: {errors}")
                    return False
            
            # Get current version
            current_version = self.version_cache.get(component_name, ConfigVersion())
            
            # Increment version
            if increment_version.lower() != "none":
                new_version = ConfigVersion(current_version.major, current_version.minor, current_version.patch)
                
                if increment_version.lower() == "major":
                    new_version.major += 1
                    new_version.minor = 0
                    new_version.patch = 0
                elif increment_version.lower() == "minor":
                    new_version.minor += 1
                    new_version.patch = 0
                elif increment_version.lower() == "patch":
                    new_version.patch += 1
            else:
                new_version = current_version
            
            # Add version to config
            config["_version"] = str(new_version)
            
            # Save to file
            file_success = self._save_to_file(component_name, config, new_version)
            
            # Save to Redis
            redis_success = self._save_to_redis(component_name, config, new_version)
            
            success = file_success or redis_success
            
            if success:
                # Update in-memory cache
                self.config_cache[component_name] = config
                self.version_cache[component_name] = new_version
                
                # Notify listeners
                if notify:
                    self._notify_listeners(component_name, config, ConfigChangeType.UPDATED)
                
                logger.info(f"Updated configuration for {component_name} to version {new_version}")
            else:
                logger.error(f"Failed to save configuration for {component_name}")
            
            return success
    
    def set_schema(self, component_name: str, schema: Dict[str, Any]) -> bool:
        """
        Set JSON schema for a component configuration
        
        Args:
            component_name: Name of the component
            schema: JSON Schema for validation
            
        Returns:
            True if successful, False otherwise
        """
        with self.cache_lock:
            try:
                # Validate that the schema itself is valid
                jsonschema.Draft7Validator.check_schema(schema)
                
                # Store the schema
                self.schema_cache[component_name] = schema
                
                # Save schema to file
                schema_path = self.base_directory / f"{component_name}.schema.json"
                with open(schema_path, 'w') as f:
                    json.dump(schema, f, indent=2)
                
                logger.info(f"Set schema for {component_name}")
                return True
            except Exception as e:
                logger.error(f"Error setting schema for {component_name}: {e}")
                return False
    
    def get_schema(self, component_name: str) -> Optional[Dict[str, Any]]:
        """
        Get JSON schema for a component configuration
        
        Args:
            component_name: Name of the component
            
        Returns:
            Schema dictionary if found, None otherwise
        """
        with self.cache_lock:
            # Check in-memory cache
            if component_name in self.schema_cache:
                return self.schema_cache[component_name]
            
            # Try to load from file
            schema_path = self.base_directory / f"{component_name}.schema.json"
            if schema_path.exists():
                try:
                    with open(schema_path, 'r') as f:
                        schema = json.load(f)
                    
                    # Cache the schema
                    self.schema_cache[component_name] = schema
                    return schema
                except Exception as e:
                    logger.error(f"Error loading schema for {component_name}: {e}")
            
            return None
    
    def validate_configuration(self, config: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate configuration against a JSON schema
        
        Args:
            config: Configuration to validate
            schema: JSON Schema for validation
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        try:
            validator = jsonschema.Draft7Validator(schema)
            errors = list(validator.iter_errors(config))
            
            if not errors:
                return True, []
            
            # Format error messages
            error_messages = []
            for error in errors:
                path = ".".join(str(p) for p in error.path) if error.path else "root"
                error_messages.append(f"At {path}: {error.message}")
            
            return False, error_messages
        except Exception as e:
            logger.error(f"Error validating configuration: {e}")
            return False, [str(e)]
    
    def add_change_listener(self, component_name: str, callback: Callable[[Dict[str, Any], ConfigChangeType], None]) -> bool:
        """
        Add a listener for configuration changes
        
        Args:
            component_name: Name of the component to listen for
            callback: Function to call when configuration changes
            
        Returns:
            True if successful, False otherwise
        """
        with self.listener_lock:
            if component_name not in self.listeners:
                self.listeners[component_name] = []
            
            if callback not in self.listeners[component_name]:
                self.listeners[component_name].append(callback)
                logger.debug(f"Added change listener for {component_name}")
                return True
            
            return False
    
    def remove_change_listener(self, component_name: str, callback: Callable[[Dict[str, Any], ConfigChangeType], None]) -> bool:
        """
        Remove a listener for configuration changes
        
        Args:
            component_name: Name of the component
            callback: Function to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.listener_lock:
            if component_name in self.listeners and callback in self.listeners[component_name]:
                self.listeners[component_name].remove(callback)
                logger.debug(f"Removed change listener for {component_name}")
                return True
            
            return False
    
    def reset_to_default(self, component_name: str, default_config: Dict[str, Any]) -> bool:
        """
        Reset configuration to default
        
        Args:
            component_name: Name of the component
            default_config: Default configuration
            
        Returns:
            True if successful, False otherwise
        """
        with self.cache_lock:
            # Archive current config first
            current_config = self.get_configuration(component_name)
            if current_config:
                self._archive_configuration(component_name, current_config)
            
            # Create new major version with default config
            return self.set_configuration(
                component_name,
                default_config.copy(),
                increment_version="major",
                notify=True
            )
    
    def list_configurations(self) -> List[str]:
        """
        List all available configurations
        
        Returns:
            List of component names with configurations
        """
        # Get configurations from files
        config_files = list(self.base_directory.glob("*.json"))
        components = [f.stem for f in config_files if not f.name.endswith(".schema.json")]
        
        # Get configurations from Redis
        if self.redis_client:
            try:
                # Find all keys in the configuration namespace
                pattern = f"{self.namespace}:config:*"
                redis_keys = self.redis_client.keys(pattern)
                
                # Extract component names from keys
                prefix_len = len(f"{self.namespace}:config:")
                redis_components = [key[prefix_len:] for key in redis_keys]
                
                # Combine with file-based components
                components = list(set(components + redis_components))
            except Exception as e:
                logger.error(f"Error listing configurations from Redis: {e}")
        
        return components
    
    def get_version(self, component_name: str) -> ConfigVersion:
        """
        Get the current version of a component's configuration
        
        Args:
            component_name: Name of the component
            
        Returns:
            Current version
        """
        with self.cache_lock:
            # Try cached version first
            if component_name in self.version_cache:
                return self.version_cache[component_name]
            
            # Load configuration to get version
            config = self.get_configuration(component_name)
            version_str = config.get("_version")
            
            if version_str:
                version = ConfigVersion.from_string(version_str)
            else:
                version = ConfigVersion()
            
            # Cache the version
            self.version_cache[component_name] = version
            
            return version
    
    def get_history(self, component_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get configuration history for a component
        
        Args:
            component_name: Name of the component
            limit: Maximum number of history entries to retrieve
            
        Returns:
            List of historical configurations with metadata
        """
        history = []
        
        # Try to get history from Redis
        if self.redis_client:
            try:
                history_key = f"{self.namespace}:config_history:{component_name}"
                history_data = self.redis_client.lrange(history_key, 0, limit - 1)
                
                for item_data in history_data:
                    try:
                        item = json.loads(item_data)
                        history.append(item)
                    except json.JSONDecodeError:
                        continue
            except Exception as e:
                logger.error(f"Error getting configuration history from Redis: {e}")
        
        # Also look for archived files
        history_dir = self.base_directory / "history" / component_name
        if history_dir.exists():
            try:
                # Get archived files sorted by timestamp (newest first)
                archive_files = sorted(
                    history_dir.glob("*.json"),
                    key=lambda f: int(f.stem.split("_")[0]),
                    reverse=True
                )
                
                # Load each file
                for i, file_path in enumerate(archive_files):
                    if i >= limit:
                        break
                    
                    try:
                        with open(file_path, 'r') as f:
                            config = json.load(f)
                        
                        # Create history entry
                        timestamp = int(file_path.stem.split("_")[0])
                        version = config.get("_version", "0.1.0")
                        
                        entry = {
                            "timestamp": timestamp,
                            "version": version,
                            "config": config
                        }
                        
                        # Add to history if not already there
                        if not any(h.get("timestamp") == timestamp for h in history):
                            history.append(entry)
                    except Exception as e:
                        logger.error(f"Error loading archived configuration: {e}")
            except Exception as e:
                logger.error(f"Error getting configuration history from files: {e}")
        
        # Sort by timestamp (newest first)
        history.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        
        # Limit the number of entries
        return history[:limit]
    
    def export_configuration(self, component_name: str, file_path: Union[str, Path]) -> bool:
        """
        Export configuration to a file
        
        Args:
            component_name: Name of the component
            file_path: Path to export to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            config = self.get_configuration(component_name)
            if not config:
                logger.error(f"No configuration found for {component_name}")
                return False
            
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w') as f:
                if file_path.suffix.lower() == '.yaml' or file_path.suffix.lower() == '.yml':
                    yaml.dump(config, f, default_flow_style=False)
                else:
                    json.dump(config, f, indent=2)
            
            logger.info(f"Exported configuration for {component_name} to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error exporting configuration for {component_name}: {e}")
            return False
    
    def import_configuration(self, component_name: str, file_path: Union[str, Path], 
                            validate: bool = True) -> bool:
        """
        Import configuration from a file
        
        Args:
            component_name: Name of the component
            file_path: Path to import from
            validate: Whether to validate against schema
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.error(f"Import file does not exist: {file_path}")
                return False
            
            # Load configuration from file
            with open(file_path, 'r') as f:
                if file_path.suffix.lower() == '.yaml' or file_path.suffix.lower() == '.yml':
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            
            # Set configuration
            return self.set_configuration(
                component_name,
                config,
                increment_version="minor",
                validate=validate,
                notify=True
            )
        except Exception as e:
            logger.error(f"Error importing configuration for {component_name}: {e}")
            return False
    
    def _load_from_redis(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Load configuration from Redis"""
        if not self.redis_client:
            return None
        
        try:
            config_key = f"{self.namespace}:config:{component_name}"
            config_data = self.redis_client.get(config_key)
            
            if config_data:
                return json.loads(config_data)
            
            return None
        except Exception as e:
            logger.error(f"Error loading configuration from Redis for {component_name}: {e}")
            return None
    
    def _load_from_file(self, component_name: str) -> Tuple[Optional[Dict[str, Any]], Optional[ConfigVersion]]:
        """Load configuration from file"""
        config_path = self.base_directory / f"{component_name}.json"
        
        if not config_path.exists():
            return None, None
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Extract version
            version_str = config.get("_version")
            version = ConfigVersion.from_string(version_str) if version_str else ConfigVersion()
            
            return config, version
        except Exception as e:
            logger.error(f"Error loading configuration from file for {component_name}: {e}")
            return None, None
    
    def _save_to_redis(self, component_name: str, config: Dict[str, Any], version: ConfigVersion) -> bool:
        """Save configuration to Redis"""
        if not self.redis_client:
            return False
        
        try:
            # Save current configuration
            config_key = f"{self.namespace}:config:{component_name}"
            self.redis_client.set(config_key, json.dumps(config))
            
            # Add to history
            history_key = f"{self.namespace}:config_history:{component_name}"
            history_entry = {
                "timestamp": int(time.time()),
                "version": str(version),
                "config": config
            }
            
            self.redis_client.lpush(history_key, json.dumps(history_entry))
            self.redis_client.ltrim(history_key, 0, 99)  # Keep last 100 versions
            
            # Publish update notification
            if self.redis_client:
                update_notification = {
                    "component": component_name,
                    "type": "updated",
                    "version": str(version),
                    "timestamp": time.time(),
                    "source": id(self)
                }
                
                self.redis_client.publish(
                    f"{self.namespace}:config_updates",
                    json.dumps(update_notification)
                )
            
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to Redis for {component_name}: {e}")
            return False
    
    def _save_to_file(self, component_name: str, config: Dict[str, Any], version: ConfigVersion) -> bool:
        """Save configuration to file"""
        try:
            # Save current configuration
            config_path = self.base_directory / f"{component_name}.json"
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            # Archive previous version if it exists
            self._archive_configuration(component_name, config)
            
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to file for {component_name}: {e}")
            return False
    
    def _archive_configuration(self, component_name: str, config: Dict[str, Any]):
        """Archive a configuration version"""
        try:
            # Create history directory
            history_dir = self.base_directory / "history" / component_name
            history_dir.mkdir(parents=True, exist_ok=True)
            
            # Create archive filename with timestamp
            timestamp = int(time.time())
            version_str = config.get("_version", "0.1.0")
            archive_path = history_dir / f"{timestamp}_{version_str}.json"
            
            # Save archive
            with open(archive_path, 'w') as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Error archiving configuration for {component_name}: {e}")
    
    def _notify_listeners(self, component_name: str, config: Dict[str, Any], change_type: ConfigChangeType):
        """Notify listeners of a configuration change"""
        with self.listener_lock:
            if component_name in self.listeners:
                for callback in self.listeners[component_name]:
                    try:
                        callback(config, change_type)
                    except Exception as e:
                        logger.error(f"Error in configuration change listener: {e}")


class ComponentConfig:
    """Helper class for component-specific configuration"""
    
    def __init__(self, component_name: str, config_manager: ConfigurationManager, 
                default_config: Dict[str, Any], schema: Optional[Dict[str, Any]] = None):
        """
        Initialize with component name and configuration manager
        
        Args:
            component_name: Name of the component
            config_manager: Configuration manager instance
            default_config: Default configuration
            schema: JSON Schema for validation
        """
        self.component_name = component_name
        self.config_manager = config_manager
        self.default_config = default_config.copy()
        
        # Set schema if provided
        if schema:
            self.config_manager.set_schema(component_name, schema)
        
        # Load initial configuration
        self.config = self.config_manager.get_configuration(component_name, default_config)
        
        # Register for change notifications
        self.config_manager.add_change_listener(component_name, self._on_config_changed)
        
        logger.info(f"ComponentConfig initialized for {component_name}")
    
    def _on_config_changed(self, new_config: Dict[str, Any], change_type: ConfigChangeType):
        """Handle configuration changes"""
        if change_type == ConfigChangeType.UPDATED or change_type == ConfigChangeType.CREATED:
            self.config = new_config
            logger.info(f"Configuration for {self.component_name} updated")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get entire configuration
        
        Returns:
            Configuration dictionary
        """
        return self.config.copy()
    
    def set(self, key: str, value: Any, save: bool = True) -> bool:
        """
        Set a configuration value
        
        Args:
            key: Configuration key
            value: Value to set
            save: Whether to save the configuration
            
        Returns:
            True if successful, False otherwise
        """
        self.config[key] = value
        
        if save:
            return self.save()
        
        return True
    
    def update(self, updates: Dict[str, Any], save: bool = True) -> bool:
        """
        Update multiple configuration values
        
        Args:
            updates: Dictionary of updates
            save: Whether to save the configuration
            
        Returns:
            True if successful, False otherwise
        """
        self.config.update(updates)
        
        if save:
            return self.save()
        
        return True
    
    def save(self) -> bool:
        """
        Save current configuration
        
        Returns:
            True if successful, False otherwise
        """
        return self.config_manager.set_configuration(
            self.component_name,
            self.config,
            increment_version="patch"
        )
    
    def reset(self) -> bool:
        """
        Reset to default configuration
        
        Returns:
            True if successful, False otherwise
        """
        return self.config_manager.reset_to_default(
            self.component_name,
            self.default_config
        )
    
    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validate current configuration against schema
        
        Returns:
            Tuple of (is_valid, error_messages)
        """
        schema = self.config_manager.get_schema(self.component_name)
        if not schema:
            return True, ["No schema available for validation"]
        
        return self.config_manager.validate_configuration(self.config, schema)


def create_default_config(output_path: Union[str, Path]):
    """
    Create a default configuration manager configuration
    
    Args:
        output_path: Path to write the configuration file
    """
    default_config = {
        "_version": "1.0.0",
        "redis": {
            "host": "localhost",
            "port": 6379,
            "password": None,
            "db": 0
        },
        "namespace": "clustersentry",
        "config_dirs": {
            "base": "config",
            "schemas": "config/schemas",
            "history": "config/history"
        },
        "cache": {
            "enabled": True,
            "ttl_seconds": 30
        },
        "notification": {
            "enabled": True
        },
        "archiving": {
            "enabled": True,
            "max_versions": 100
        }
    }
    
    # Ensure parent directory exists
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write configuration file
    with open(output_path, 'w') as f:
        json.dump(default_config, f, indent=2)
    
    print(f"Created default configuration manager configuration at {output_path}")


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ClusterSentry Configuration Manager")
    parser.add_argument("--config-dir", default="config", help="Base directory for configuration files")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--component", help="Component to operate on")
    parser.add_argument("--get", action="store_true", help="Get configuration")
    parser.add_argument("--set", help="Set configuration value (key=value)")
    parser.add_argument("--import", dest="import_file", help="Import configuration from file")
    parser.add_argument("--export", help="Export configuration to file")
    parser.add_argument("--validate", action="store_true", help="Validate configuration")
    parser.add_argument("--create-default", help="Create default configuration file")
    parser.add_argument("--list", action="store_true", help="List available configurations")
    parser.add_argument("--history", action="store_true", help="Show configuration history")
    parser.add_argument("--reset", action="store_true", help="Reset to default configuration")
    args = parser.parse_args()
    
    # Create default configuration file if requested
    if args.create_default:
        create_default_config(args.create_default)
        sys.exit(0)
    
    # Create configuration manager
    redis_config = {
        "host": args.redis_host,
        "port": args.redis_port
    }
    
    config_manager = ConfigurationManager(
        base_directory=args.config_dir,
        redis_config=redis_config
    )
    
    try:
        # List configurations
        if args.list:
            components = config_manager.list_configurations()
            print(f"Available configurations ({len(components)}):")
            for component in components:
                version = config_manager.get_version(component)
                print(f"  {component} (v{version})")
            sys.exit(0)
        
        # Component-specific operations
        if args.component:
            component = args.component
            
            # Get configuration
            if args.get:
                config = config_manager.get_configuration(component)
                if config:
                    version = config_manager.get_version(component)
                    print(f"Configuration for {component} (version {version}):")
                    print(json.dumps(config, indent=2))
                else:
                    print(f"No configuration found for {component}")
            
            # Set configuration value
            elif args.set:
                if "=" not in args.set:
                    print("Error: Set format should be key=value")
                    sys.exit(1)
                
                key, value = args.set.split("=", 1)
                
                # Try to parse the value as JSON
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    # Keep as string if not valid JSON
                    pass
                
                config = config_manager.get_configuration(component, {})
                config[key] = value
                
                if config_manager.set_configuration(component, config):
                    print(f"Updated configuration for {component}")
                else:
                    print(f"Failed to update configuration for {component}")
            
            # Import configuration
            elif args.import_file:
                if config_manager.import_configuration(component, args.import_file):
                    print(f"Imported configuration for {component} from {args.import_file}")
                else:
                    print(f"Failed to import configuration for {component}")
            
            # Export configuration
            elif args.export:
                if config_manager.export_configuration(component, args.export):
                    print(f"Exported configuration for {component} to {args.export}")
                else:
                    print(f"Failed to export configuration for {component}")
            
            # Validate configuration
            elif args.validate:
                config = config_manager.get_configuration(component)
                schema = config_manager.get_schema(component)
                
                if not schema:
                    print(f"No schema found for {component}")
                    sys.exit(1)
                
                is_valid, errors = config_manager.validate_configuration(config, schema)
                
                if is_valid:
                    print(f"Configuration for {component} is valid")
                else:
                    print(f"Configuration for {component} is invalid:")
                    for error in errors:
                        print(f"  - {error}")
            
            # Show configuration history
            elif args.history:
                history = config_manager.get_history(component)
                
                if history:
                    print(f"Configuration history for {component} ({len(history)} entries):")
                    for entry in history:
                        timestamp = entry.get("timestamp")
                        version = entry.get("version")
                        if timestamp and version:
                            time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
                            print(f"  {time_str} - v{version}")
                else:
                    print(f"No configuration history found for {component}")
            
            # Reset to default
            elif args.reset:
                default_path = Path(args.config_dir) / f"{component}.default.json"
                
                if not default_path.exists():
                    print(f"No default configuration found at {default_path}")
                    sys.exit(1)
                
                try:
                    with open(default_path, 'r') as f:
                        default_config = json.load(f)
                    
                    if config_manager.reset_to_default(component, default_config):
                        print(f"Reset configuration for {component} to default")
                    else:
                        print(f"Failed to reset configuration for {component}")
                except Exception as e:
                    print(f"Error loading default configuration: {e}")
            
            else:
                print("No operation specified. Use --get, --set, --import, --export, --validate, or --reset")
        
        elif not args.list and not args.create_default:
            print("No component specified. Use --component or --list")
    
    finally:
        # Clean up
        config_manager.close()