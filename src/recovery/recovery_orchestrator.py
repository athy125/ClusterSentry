#!/usr/bin/env python3
"""
Self-Healing Recovery Orchestrator for Fault-Tolerant HPC System

This module implements the recovery orchestration logic that coordinates
recovery actions across the distributed system based on detected failures.
"""

import os
import time
import json
import logging
import threading
import argparse
import queue
import socket
import subprocess
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass
import grpc
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("recovery_orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RecoveryOrchestrator")

# Define failure types
class FailureType(Enum):
    PROCESS_CRASH = "process_crash"
    HIGH_CPU_USAGE = "high_cpu_usage"
    MEMORY_LEAK = "memory_leak"
    DISK_SPACE = "disk_space"
    NETWORK_ISSUE = "network_issue"
    SERVICE_UNRESPONSIVE = "service_unresponsive"
    DATABASE_CONNECTION = "database_connection"
    HARDWARE_FAILURE = "hardware_failure"
    UNKNOWN = "unknown"

# Define recovery actions
class RecoveryAction(Enum):
    RESTART_PROCESS = "restart_process"
    RESTART_SERVICE = "restart_service"
    REBOOT_NODE = "reboot_node"
    MIGRATE_WORKLOAD = "migrate_workload"
    CLEAR_CACHE = "clear_cache"
    CLEANUP_DISK = "cleanup_disk"
    RESET_NETWORK = "reset_network"
    FAILOVER_DATABASE = "failover_database"
    MANUAL_INTERVENTION = "manual_intervention"

@dataclass
class FailureEvent:
    """Represents a detected failure in the system"""
    node_id: str
    timestamp: float
    failure_type: FailureType
    severity: int  # 1-5, with 5 being most severe
    affected_components: List[str]
    metrics: Dict[str, float]
    error_message: str

@dataclass
class RecoveryPlan:
    """Defines a plan to recover from a failure"""
    failure_event: FailureEvent
    actions: List[Tuple[RecoveryAction, Dict]]
    estimated_recovery_time: float  # seconds
    priority: int  # 1-5, with 5 being highest priority
    dependencies: List[str]  # IDs of other recovery plans that must complete first

class RecoveryOrchestrator:
    """
    Coordinates recovery actions across a distributed system based on detected failures.
    Implements a priority-based recovery strategy with dependency management.
    """
    
    def __init__(self, config_path: str):
        """Initialize the recovery orchestrator with configuration"""
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.node_statuses = {}  # Track status of all nodes
        self.active_recoveries = {}  # Track ongoing recovery operations
        self.recovery_history = []  # Historical record of recovery operations
        self.recovery_queue = queue.PriorityQueue()  # Priority queue for recovery actions
        
        # Connect to Redis for distributed state
        self.redis_client = redis.Redis(
            host=self.config.get('redis_host', 'localhost'),
            port=self.config.get('redis_port', 6379),
            password=self.config.get('redis_password', None)
        )
        
        # Initialize locks
        self.node_lock = threading.RLock()
        self.recovery_lock = threading.RLock()
        
        # Start worker threads
        self.running = True
        self.worker_threads = []
        for i in range(self.config.get('num_worker_threads', 4)):
            thread = threading.Thread(target=self._recovery_worker, daemon=True)
            thread.start()
            self.worker_threads.append(thread)
        
        # Start failure event listener
        self.event_thread = threading.Thread(target=self._listen_for_failures, daemon=True)
        self.event_thread.start()
        
        logger.info(f"Recovery Orchestrator initialized with {len(self.worker_threads)} workers")
    
    def shutdown(self):
        """Gracefully shut down the recovery orchestrator"""
        logger.info("Shutting down Recovery Orchestrator...")
        self.running = False
        
        # Wait for all worker threads to complete
        for thread in self.worker_threads:
            thread.join(timeout=5.0)
        
        # Wait for event listener thread
        self.event_thread.join(timeout=5.0)
        
        # Close Redis connection
        self.redis_client.close()
        logger.info("Recovery Orchestrator shutdown complete")
    
    def handle_failure(self, failure_event: FailureEvent) -> str:
        """
        Process a new failure event and create a recovery plan
        Returns the ID of the created recovery plan
        """
        logger.info(f"Handling failure on node {failure_event.node_id}: {failure_event.failure_type.value}")
        
        # Generate recovery plan based on failure type
        recovery_plan = self._create_recovery_plan(failure_event)
        plan_id = f"recovery-{failure_event.node_id}-{int(failure_event.timestamp)}"
        
        # Update node status
        with self.node_lock:
            self.node_statuses[failure_event.node_id] = {
                "status": "degraded" if failure_event.severity < 4 else "failed",
                "ongoing_recovery": plan_id,
                "last_failure": failure_event.timestamp,
                "failure_type": failure_event.failure_type.value
            }
        
        # Add to recovery queue with priority
        self.recovery_queue.put((
            -recovery_plan.priority,  # Negative for highest priority first
            failure_event.timestamp,
            plan_id,
            recovery_plan
        ))
        
        # Track active recovery
        with self.recovery_lock:
            self.active_recoveries[plan_id] = {
                "plan": recovery_plan,
                "status": "queued",
                "start_time": time.time(),
                "completed_actions": []
            }
        
        logger.info(f"Created recovery plan {plan_id} with {len(recovery_plan.actions)} actions")
        return plan_id
    
    def get_recovery_status(self, plan_id: str) -> Dict:
        """Get the status of a specific recovery plan"""
        with self.recovery_lock:
            if plan_id in self.active_recoveries:
                return self.active_recoveries[plan_id]
            
            # Check history for completed recoveries
            for hist in self.recovery_history:
                if hist.get("plan_id") == plan_id:
                    return hist
        
        return {"status": "not_found", "error": "Recovery plan not found"}
    
    def get_node_status(self, node_id: str) -> Dict:
        """Get the current status of a specific node"""
        with self.node_lock:
            if node_id in self.node_statuses:
                return self.node_statuses[node_id]
        
        return {"status": "unknown", "error": "Node not found"}
    
    def _create_recovery_plan(self, failure_event: FailureEvent) -> RecoveryPlan:
        """Create a recovery plan based on the failure type and severity"""
        actions = []
        dependencies = []
        
        # Generate actions based on failure type
        if failure_event.failure_type == FailureType.PROCESS_CRASH:
            for component in failure_event.affected_components:
                actions.append((
                    RecoveryAction.RESTART_PROCESS,
                    {"process_name": component, "max_retries": 3}
                ))
        
        elif failure_event.failure_type == FailureType.HIGH_CPU_USAGE:
            # First identify resource-intensive processes
            actions.append((
                RecoveryAction.RESTART_PROCESS,
                {"process_name": failure_event.affected_components[0], "graceful": True}
            ))
        
        elif failure_event.failure_type == FailureType.MEMORY_LEAK:
            actions.append((RecoveryAction.CLEAR_CACHE, {}))
            actions.append((
                RecoveryAction.RESTART_SERVICE,
                {"service_name": failure_event.affected_components[0]}
            ))
        
        elif failure_event.failure_type == FailureType.DISK_SPACE:
            actions.append((RecoveryAction.CLEANUP_DISK, {"min_free_percent": 15}))
        
        elif failure_event.failure_type == FailureType.NETWORK_ISSUE:
            actions.append((RecoveryAction.RESET_NETWORK, {}))
        
        elif failure_event.failure_type == FailureType.SERVICE_UNRESPONSIVE:
            for service in failure_event.affected_components:
                actions.append((
                    RecoveryAction.RESTART_SERVICE,
                    {"service_name": service, "timeout": 30}
                ))
        
        elif failure_event.failure_type == FailureType.DATABASE_CONNECTION:
            actions.append((RecoveryAction.FAILOVER_DATABASE, {}))
        
        elif failure_event.failure_type == FailureType.HARDWARE_FAILURE:
            # For hardware failures, migrate workloads and notify operators
            actions.append((
                RecoveryAction.MIGRATE_WORKLOAD,
                {"source_node": failure_event.node_id, "workload_type": "all"}
            ))
            actions.append((RecoveryAction.MANUAL_INTERVENTION, {
                "message": f"Hardware failure detected on {failure_event.node_id}: {failure_event.error_message}"
            }))
        
        else:  # UNKNOWN or any other type
            # For unknown failures, restart the affected services and notify operators
            for component in failure_event.affected_components:
                actions.append((
                    RecoveryAction.RESTART_SERVICE,
                    {"service_name": component, "timeout": 60}
                ))
            
            # If severity is high, also consider node reboot as last resort
            if failure_event.severity >= 4:
                actions.append((RecoveryAction.REBOOT_NODE, {"timeout": 300}))
        
        # Calculate estimated recovery time based on actions
        estimated_time = sum(30 for _ in actions)  # Simple estimate of 30 seconds per action
        
        # Set priority based on severity and affected components
        priority = min(5, failure_event.severity + 
                       (2 if "database" in failure_event.affected_components else 0) +
                       (1 if "compute" in failure_event.affected_components else 0))
        
        return RecoveryPlan(
            failure_event=failure_event,
            actions=actions,
            estimated_recovery_time=estimated_time,
            priority=priority,
            dependencies=dependencies
        )
    
    def _listen_for_failures(self):
        """Listen for failure events from the message queue or Redis pub/sub"""
        pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe("failure_events")
        
        logger.info("Failure event listener started")
        
        while self.running:
            try:
                message = pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    # Process failure event
                    event_data = json.loads(message['data'])
                    
                    # Convert to FailureEvent
                    failure_event = FailureEvent(
                        node_id=event_data['node_id'],
                        timestamp=event_data['timestamp'],
                        failure_type=FailureType(event_data['failure_type']),
                        severity=event_data['severity'],
                        affected_components=event_data['affected_components'],
                        metrics=event_data['metrics'],
                        error_message=event_data['error_message']
                    )
                    
                    # Handle the failure
                    self.handle_failure(failure_event)
            
            except Exception as e:
                logger.error(f"Error processing failure event: {str(e)}")
                time.sleep(1)  # Prevent tight loop in case of persistent errors
        
        # Clean up
        pubsub.unsubscribe()
        logger.info("Failure event listener stopped")
    
    def _recovery_worker(self):
        """Worker thread that executes recovery actions"""
        logger.info("Recovery worker thread started")
        
        while self.running:
            try:
                # Get the next recovery plan from the queue
                try:
                    _, _, plan_id, recovery_plan = self.recovery_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                # Update recovery status
                with self.recovery_lock:
                    if plan_id in self.active_recoveries:
                        self.active_recoveries[plan_id]["status"] = "in_progress"
                        self.active_recoveries[plan_id]["start_time"] = time.time()
                
                logger.info(f"Executing recovery plan {plan_id} for node {recovery_plan.failure_event.node_id}")
                
                # Execute each action in the plan
                all_successful = True
                for i, (action, params) in enumerate(recovery_plan.actions):
                    try:
                        logger.info(f"Executing action {i+1}/{len(recovery_plan.actions)}: {action.value}")
                        success = self._execute_recovery_action(
                            recovery_plan.failure_event.node_id, 
                            action, 
                            params
                        )
                        
                        # Update recovery status
                        with self.recovery_lock:
                            if plan_id in self.active_recoveries:
                                self.active_recoveries[plan_id]["completed_actions"].append({
                                    "action": action.value,
                                    "params": params,
                                    "success": success,
                                    "timestamp": time.time()
                                })
                        
                        if not success:
                            all_successful = False
                            logger.error(f"Action {action.value} failed for node {recovery_plan.failure_event.node_id}")
                            
                            # If it's a critical action, break the recovery plan
                            if action in [RecoveryAction.RESTART_SERVICE, RecoveryAction.REBOOT_NODE]:
                                break
                    
                    except Exception as e:
                        all_successful = False
                        logger.error(f"Error executing action {action.value}: {str(e)}")
                
                # Update recovery status and node status
                completion_status = "completed" if all_successful else "failed"
                with self.recovery_lock:
                    if plan_id in self.active_recoveries:
                        self.active_recoveries[plan_id]["status"] = completion_status
                        self.active_recoveries[plan_id]["end_time"] = time.time()
                        
                        # Move to history
                        history_entry = self.active_recoveries.pop(plan_id)
                        history_entry["plan_id"] = plan_id
                        self.recovery_history.append(history_entry)
                
                with self.node_lock:
                    node_id = recovery_plan.failure_event.node_id
                    if node_id in self.node_statuses:
                        if all_successful:
                            self.node_statuses[node_id]["status"] = "healthy"
                            self.node_statuses[node_id]["ongoing_recovery"] = None
                        else:
                            # If recovery failed, keep node in degraded state
                            self.node_statuses[node_id]["status"] = "degraded"
                            self.node_statuses[node_id]["ongoing_recovery"] = None
                
                logger.info(f"Recovery plan {plan_id} {completion_status}")
                
                # Indicate that we're done with this queue item
                self.recovery_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in recovery worker: {str(e)}")
                time.sleep(1)  # Prevent tight loop in case of persistent errors
        
        logger.info("Recovery worker thread stopped")
    
    def _execute_recovery_action(self, node_id: str, action: RecoveryAction, params: Dict) -> bool:
        """
        Execute a specific recovery action on a node
        Returns True if the action was successful, False otherwise
        """
        # In a real implementation, this would use gRPC to communicate with the node's agent
        # For demo purposes, we'll simulate the actions
        
        # Simulate execution time
        time.sleep(1.0)
        
        if action == RecoveryAction.RESTART_PROCESS:
            process_name = params.get("process_name", "unknown")
            logger.info(f"Restarting process {process_name} on node {node_id}")
            # In a real system, would use SSH or agent RPC to restart the process
            return True
        
        elif action == RecoveryAction.RESTART_SERVICE:
            service_name = params.get("service_name", "unknown")
            logger.info(f"Restar