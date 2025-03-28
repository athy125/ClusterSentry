#!/usr/bin/env python3
"""
State Manager for ClusterSentry

This module provides a distributed state management layer for the recovery
orchestrator, ensuring consistency across multiple recovery orchestrator
instances.
"""

import os
import time
import json
import logging
import threading
import redis
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("state_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("StateManager")

class StateKey(Enum):
    """Enum defining keys for state storage"""
    NODE_STATUS = "node_status"
    RECOVERY_PLANS = "recovery_plans"
    ACTIVE_RECOVERIES = "active_recoveries"
    RECOVERY_HISTORY = "recovery_history"
    CLUSTER_TOPOLOGY = "cluster_topology"
    HEALTH_METRICS = "health_metrics"
    ANOMALY_THRESHOLDS = "anomaly_thresholds"

class StateManager:
    """
    Distributed state manager for the ClusterSentry recovery orchestrator.
    Uses Redis for state storage with pub/sub for notifications.
    """
    
    def __init__(self, redis_host='localhost', redis_port=6379, redis_password=None, 
                 namespace='clustersentry', instance_id=None):
        """Initialize the state manager"""
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.namespace = namespace
        
        # Generate instance ID if not provided
        if instance_id is None:
            import socket
            import uuid
            hostname = socket.gethostname()
            self.instance_id = f"{hostname}-{uuid.uuid4().hex[:8]}"
        else:
            self.instance_id = instance_id
        
        # Initialize Redis client
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True  # Automatically decode responses to strings
        )
        
        # Initialize Redis pub/sub
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(f"{namespace}:state_changes")
        
        # Start listener thread
        self.running = True
        self.listener_thread = threading.Thread(target=self._state_change_listener, daemon=True)
        self.listener_thread.start()
        
        # Locks for thread safety
        self.state_locks = {key: threading.RLock() for key in StateKey}
        
        # Local cache
        self.state_cache = {}
        self.cache_timestamps = {}
        self.cache_ttl = 5.0  # seconds
        
        logger.info(f"State Manager initialized with instance ID: {self.instance_id}")
        
        # Register this instance
        self._register_instance()

    def _register_instance(self):
        """Register this orchestrator instance in the distributed registry"""
        instance_key = f"{self.namespace}:instances:{self.instance_id}"
        
        # Store instance info with expiration
        instance_info = {
            "instance_id": self.instance_id,
            "host": os.environ.get("HOSTNAME", "unknown"),
            "start_time": time.time(),
            "last_heartbeat": time.time()
        }
        
        self.redis.hset(instance_key, mapping=instance_info)
        self.redis.expire(instance_key, 60)  # 60-second TTL, will be refreshed by heartbeat
        
        # Add to active instances set
        self.redis.sadd(f"{self.namespace}:active_instances", self.instance_id)
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
        logger.info(f"Registered instance {self.instance_id} in distributed registry")
    
    def _heartbeat_loop(self):
        """Send regular heartbeats to keep the instance registration alive"""
        instance_key = f"{self.namespace}:instances:{self.instance_id}"
        
        while self.running:
            try:
                # Update last heartbeat
                self.redis.hset(instance_key, "last_heartbeat", time.time())
                # Reset expiration
                self.redis.expire(instance_key, 60)
            except Exception as e:
                logger.error(f"Error in heartbeat: {str(e)}")
            
            # Sleep for 20 seconds
            for _ in range(20):
                if not self.running:
                    break
                time.sleep(1)
    
    def _state_change_listener(self):
        """Listen for state change notifications from other instances"""
        logger.info("State change listener started")
        
        while self.running:
            try:
                message = self.pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    # Process state change notification
                    try:
                        data = json.loads(message['data'])
                        
                        # Skip our own messages
                        if data.get('source_instance') == self.instance_id:
                            continue
                        
                        # Invalidate cache for the changed state key
                        state_key = data.get('state_key')
                        if state_key:
                            # Convert string back to enum if needed
                            if isinstance(state_key, str):
                                try:
                                    state_key = StateKey[state_key]
                                except KeyError:
                                    logger.warning(f"Unknown state key: {state_key}")
                                    continue
                            
                            with self.state_locks[state_key]:
                                if state_key in self.state_cache:
                                    del self.state_cache[state_key]
                                    logger.debug(f"Invalidated cache for {state_key.name}")
                    except json.JSONDecodeError:
                        logger.error("Invalid JSON in state change notification")
                    
            except Exception as e:
                logger.error(f"Error in state change listener: {str(e)}")
                time.sleep(1)  # Prevent tight loop in case of errors
        
        # Clean up
        self.pubsub.unsubscribe()
        logger.info("State change listener stopped")
    
    def get_state(self, key: StateKey, field: Optional[str] = None) -> Any:
        """
        Get state from distributed storage, with local caching
        
        Args:
            key: StateKey enum value for the state category
            field: Optional specific field to retrieve, if None gets all fields
            
        Returns:
            State value (dict, list, or scalar depending on the key and field)
        """
        with self.state_locks[key]:
            # Check if we have a fresh cached value
            cache_key = (key, field)
            if cache_key in self.state_cache:
                timestamp = self.cache_timestamps.get(cache_key, 0)
                if time.time() - timestamp < self.cache_ttl:
                    return self.state_cache[cache_key]
            
            # No fresh cache, retrieve from Redis
            redis_key = f"{self.namespace}:{key.name.lower()}"
            
            value = None
            if field is not None:
                # Get specific field
                if isinstance(field, (list, tuple)):
                    # Multiple fields
                    value = self.redis.hmget(redis_key, field)
                    # Convert to dictionary
                    value = {f: v for f, v in zip(field, value)}
                    # Convert JSON strings to Python objects
                    for f, v in value.items():
                        if v is not None:
                            try:
                                value[f] = json.loads(v)
                            except json.JSONDecodeError:
                                pass  # Keep as string if not JSON
                else:
                    # Single field
                    value = self.redis.hget(redis_key, field)
                    if value is not None:
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError:
                            pass  # Keep as string if not JSON
            else:
                # Get all fields
                value = self.redis.hgetall(redis_key)
                # Convert JSON strings to Python objects
                for f, v in value.items():
                    try:
                        value[f] = json.loads(v)
                    except json.JSONDecodeError:
                        pass  # Keep as string if not JSON
            
            # Store in cache
            self.state_cache[cache_key] = value
            self.cache_timestamps[cache_key] = time.time()
            
            return value
    
    def set_state(self, key: StateKey, field: str, value: Any, notify: bool = True) -> bool:
        """
        Set state in distributed storage
        
        Args:
            key: StateKey enum value for the state category
            field: Field to set
            value: Value to set (will be JSON encoded)
            notify: Whether to notify other instances of the change
            
        Returns:
            True if successful, False otherwise
        """
        with self.state_locks[key]:
            try:
                # Convert value to JSON string
                json_value = json.dumps(value)
                
                # Set in Redis
                redis_key = f"{self.namespace}:{key.name.lower()}"
                self.redis.hset(redis_key, field, json_value)
                
                # Update local cache
                cache_key = (key, field)
                self.state_cache[cache_key] = value
                self.cache_timestamps[cache_key] = time.time()
                
                # Also update the full state cache if it exists
                full_cache_key = (key, None)
                if full_cache_key in self.state_cache:
                    if isinstance(self.state_cache[full_cache_key], dict):
                        self.state_cache[full_cache_key][field] = value
                        self.cache_timestamps[full_cache_key] = time.time()
                
                # Notify other instances
                if notify:
                    notification = {
                        "source_instance": self.instance_id,
                        "state_key": key.name,
                        "field": field,
                        "timestamp": time.time()
                    }
                    self.redis.publish(f"{self.namespace}:state_changes", json.dumps(notification))
                
                return True
            
            except Exception as e:
                logger.error(f"Error setting state {key.name}.{field}: {str(e)}")
                return False
    
    def get_node_status(self, node_id: str) -> Dict:
        """
        Get the current status of a specific node
        
        Args:
            node_id: ID of the node to get status for
            
        Returns:
            Dictionary with node status information
        """
        return self.get_state(StateKey.NODE_STATUS, node_id) or {}
    
    def set_node_status(self, node_id: str, status: Dict) -> bool:
        """
        Set the status of a specific node
        
        Args:
            node_id: ID of the node to update
            status: Node status information
            
        Returns:
            True if successful, False otherwise
        """
        return self.set_state(StateKey.NODE_STATUS, node_id, status)
    
    def get_recovery_plan(self, plan_id: str) -> Dict:
        """
        Get a specific recovery plan
        
        Args:
            plan_id: ID of the recovery plan
            
        Returns:
            Dictionary with recovery plan information
        """
        return self.get_state(StateKey.RECOVERY_PLANS, plan_id) or {}
    
    def set_recovery_plan(self, plan_id: str, plan: Dict) -> bool:
        """
        Store a recovery plan
        
        Args:
            plan_id: ID of the recovery plan
            plan: Recovery plan information
            
        Returns:
            True if successful, False otherwise
        """
        return self.set_state(StateKey.RECOVERY_PLANS, plan_id, plan)
    
    def get_active_recoveries(self) -> Dict[str, Dict]:
        """
        Get all active recovery operations
        
        Returns:
            Dictionary mapping recovery IDs to recovery information
        """
        return self.get_state(StateKey.ACTIVE_RECOVERIES) or {}
    
    def add_active_recovery(self, recovery_id: str, recovery_info: Dict) -> bool:
        """
        Add an active recovery operation
        
        Args:
            recovery_id: ID of the recovery operation
            recovery_info: Recovery information
            
        Returns:
            True if successful, False otherwise
        """
        return self.set_state(StateKey.ACTIVE_RECOVERIES, recovery_id, recovery_info)
    
    def remove_active_recovery(self, recovery_id: str) -> bool:
        """
        Remove an active recovery operation (typically when completed)
        
        Args:
            recovery_id: ID of the recovery operation
            
        Returns:
            True if successful, False otherwise
        """
        with self.state_locks[StateKey.ACTIVE_RECOVERIES]:
            try:
                # Remove from Redis
                redis_key = f"{self.namespace}:{StateKey.ACTIVE_RECOVERIES.name.lower()}"
                self.redis.hdel(redis_key, recovery_id)
                
                # Update local cache
                cache_key = (StateKey.ACTIVE_RECOVERIES, None)
                if cache_key in self.state_cache:
                    if isinstance(self.state_cache[cache_key], dict):
                        if recovery_id in self.state_cache[cache_key]:
                            del self.state_cache[cache_key][recovery_id]
                            self.cache_timestamps[cache_key] = time.time()
                
                # Notify other instances
                notification = {
                    "source_instance": self.instance_id,
                    "state_key": StateKey.ACTIVE_RECOVERIES.name,
                    "field": recovery_id,
                    "action": "delete",
                    "timestamp": time.time()
                }
                self.redis.publish(f"{self.namespace}:state_changes", json.dumps(notification))
                
                return True
            
            except Exception as e:
                logger.error(f"Error removing active recovery {recovery_id}: {str(e)}")
                return False
    
    def add_to_recovery_history(self, recovery_id: str, recovery_info: Dict) -> bool:
        """
        Add a completed recovery to history
        
        Args:
            recovery_id: ID of the recovery operation
            recovery_info: Recovery information including results
            
        Returns:
            True if successful, False otherwise
        """
        # First, ensure the recovery history list exists
        redis_key = f"{self.namespace}:recovery_history_list"
        
        # Add record to history hash
        history_success = self.set_state(StateKey.RECOVERY_HISTORY, recovery_id, recovery_info)
        
        # Add to time-ordered list
        try:
            self.redis.lpush(redis_key, recovery_id)
            # Trim the list to a reasonable size
            self.redis.ltrim(redis_key, 0, 999)  # Keep the last 1000 entries
            return history_success
        except Exception as e:
            logger.error(f"Error adding to recovery history list: {str(e)}")
            return False
    
    def get_recovery_history(self, limit: int = 100) -> List[Dict]:
        """
        Get recovery history in chronological order
        
        Args:
            limit: Maximum number of history items to retrieve
            
        Returns:
            List of recovery history items, newest first
        """
        try:
            # Get list of recovery IDs
            redis_key = f"{self.namespace}:recovery_history_list"
            recovery_ids = self.redis.lrange(redis_key, 0, limit - 1)
            
            if not recovery_ids:
                return []
            
            # Get recovery info for each ID
            history_items = []
            for recovery_id in recovery_ids:
                info = self.get_state(StateKey.RECOVERY_HISTORY, recovery_id)
                if info:
                    info['recovery_id'] = recovery_id
                    history_items.append(info)
            
            return history_items
        
        except Exception as e:
            logger.error(f"Error getting recovery history: {str(e)}")
            return []
    
    def update_cluster_topology(self, topology: Dict) -> bool:
        """
        Update the cluster topology information
        
        Args:
            topology: Cluster topology information
            
        Returns:
            True if successful, False otherwise
        """
        # Store with a timestamp
        topology['last_updated'] = time.time()
        return self.set_state(StateKey.CLUSTER_TOPOLOGY, 'current', topology)
    
    def get_cluster_topology(self) -> Dict:
        """
        Get the current cluster topology
        
        Returns:
            Dictionary with cluster topology information
        """
        return self.get_state(StateKey.CLUSTER_TOPOLOGY, 'current') or {}
    
    def update_health_metrics(self, node_id: str, metrics: Dict) -> bool:
        """
        Update health metrics for a node
        
        Args:
            node_id: ID of the node
            metrics: Health metrics
            
        Returns:
            True if successful, False otherwise
        """
        # Store with a timestamp
        metrics['timestamp'] = time.time()
        return self.set_state(StateKey.HEALTH_METRICS, node_id, metrics)
    
    def get_health_metrics(self, node_id: str) -> Dict:
        """
        Get health metrics for a node
        
        Args:
            node_id: ID of the node
            
        Returns:
            Dictionary with health metrics
        """
        return self.get_state(StateKey.HEALTH_METRICS, node_id) or {}
    
    def get_active_orchestrator_instances(self) -> List[Dict]:
        """
        Get information about all active orchestrator instances
        
        Returns:
            List of dictionaries with instance information
        """
        try:
            # Get list of active instance IDs
            instance_ids = self.redis.smembers(f"{self.namespace}:active_instances")
            
            if not instance_ids:
                return []
            
            # Get instance info for each ID
            instances = []
            for instance_id in instance_ids:
                instance_key = f"{self.namespace}:instances:{instance_id}"
                info = self.redis.hgetall(instance_key)
                if info:
                    instances.append(info)
            
            return instances
        
        except Exception as e:
            logger.error(f"Error getting active orchestrator instances: {str(e)}")
            return []
    
    def acquire_lock(self, lock_name: str, timeout: float = 10.0, 
                    expire: float = 30.0) -> bool:
        """
        Acquire a distributed lock
        
        Args:
            lock_name: Name of the lock to acquire
            timeout: Time to wait for lock acquisition (seconds)
            expire: Time until lock auto-expires (seconds)
            
        Returns:
            True if the lock was acquired, False otherwise
        """
        lock_key = f"{self.namespace}:locks:{lock_name}"
        lock_value = f"{self.instance_id}:{time.time()}"
        
        end_time = time.time() + timeout
        retry_interval = 0.1
        
        while time.time() < end_time:
            # Try to acquire the lock using setnx
            if self.redis.set(lock_key, lock_value, ex=int(expire), nx=True):
                logger.debug(f"Acquired lock: {lock_name}")
                return True
            
            # Wait before retrying
            time.sleep(retry_interval)
            # Exponential backoff with maximum interval of 1 second
            retry_interval = min(1.0, retry_interval * 1.5)
        
        logger.warning(f"Failed to acquire lock: {lock_name} (timeout)")
        return False
    
    def release_lock(self, lock_name: str) -> bool:
        """
        Release a distributed lock
        
        Args:
            lock_name: Name of the lock to release
            
        Returns:
            True if the lock was released, False otherwise
        """
        lock_key = f"{self.namespace}:locks:{lock_name}"
        lock_value = f"{self.instance_id}:"
        
        # Check if we own the lock
        current_value = self.redis.get(lock_key)
        if current_value and current_value.startswith(lock_value):
            # We own the lock, delete it
            self.redis.delete(lock_key)
            logger.debug(f"Released lock: {lock_name}")
            return True
        else:
            logger.warning(f"Cannot release lock: {lock_name} (not owner)")
            return False
    
    def cleanup(self):
        """Clean up resources and connections"""
        logger.info("Cleaning up state manager")
        
        # Stop threads
        self.running = False
        
        if self.listener_thread.is_alive():
            self.listener_thread.join(timeout=2.0)
        
        if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2.0)
        
        # Remove from active instances
        try:
            self.redis.srem(f"{self.namespace}:active_instances", self.instance_id)
            instance_key = f"{self.namespace}:instances:{self.instance_id}"
            self.redis.delete(instance_key)
        except Exception as e:
            logger.error(f"Error removing instance from registry: {str(e)}")
        
        # Close Redis connections
        try:
            self.pubsub.close()
            self.redis.close()
        except Exception as e:
            logger.error(f"Error closing Redis connections: {str(e)}")
        
        logger.info("State manager cleanup complete")

# Example usage
if __name__ == "__main__":
    # Initialize state manager
    state_manager = StateManager(redis_host='localhost', redis_port=6379)
    
    try:
        # Example operations
        print("Setting node status...")
        state_manager.set_node_status("node-1", {
            "status": "healthy",
            "last_heartbeat": time.time(),
            "resources": {
                "cpu": 0.25,
                "memory": 0.40,
                "disk": 0.60
            }
        })
        
        print("Getting node status...")
        status = state_manager.get_node_status("node-1")
        print(f"Node status: {status}")
        
        # Wait for a while to see state change notifications
        print("Running for 60 seconds...")
        time.sleep(60)
    
    finally:
        # Clean up
        state_manager.cleanup()