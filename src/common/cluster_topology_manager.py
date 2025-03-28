#!/usr/bin/env python3
"""
Cluster Topology Manager for ClusterSentry

This module discovers and maintains the topology of the cluster, providing
information about network structure, node relationships, and service dependencies
for intelligent failure impact assessment and recovery planning.
"""

import os
import sys
import time
import json
import logging
import threading
import ipaddress
import socket
import subprocess
import redis
import networkx as nx
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Union, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("topology_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TopologyManager")

@dataclass
class NodeInfo:
    """Information about a node in the cluster topology"""
    node_id: str
    hostname: str
    ip_address: str
    status: str = "unknown"  # unknown, healthy, degraded, critical, offline
    labels: Dict[str, str] = field(default_factory=dict)
    rack: Optional[str] = None
    datacenter: Optional[str] = None
    region: Optional[str] = None
    node_type: str = "worker"  # master, worker, storage, etc.
    services: List[str] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    neighbors: List[str] = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)

@dataclass
class ServiceInfo:
    """Information about a service in the cluster topology"""
    service_id: str
    service_name: str
    service_type: str
    nodes: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    endpoints: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "unknown"  # unknown, available, degraded, unavailable
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

@dataclass
class NetworkLink:
    """Information about a network link between nodes"""
    source_node: str
    target_node: str
    link_type: str = "ethernet"  # ethernet, infiniband, fiber, etc.
    bandwidth: Optional[float] = None  # Mbps
    latency: Optional[float] = None  # ms
    packet_loss: Optional[float] = None  # percentage
    status: str = "unknown"  # unknown, up, down, degraded
    last_updated: float = field(default_factory=time.time)

class TopologyDiscoveryMethod(Enum):
    """Methods for topology discovery"""
    NETWORK_SCAN = "network_scan"
    KUBERNETES = "kubernetes"
    CONSUL = "consul"
    ZOOKEEPER = "zookeeper"
    MANUAL = "manual"
    IMPORT = "import"

class ClusterTopologyManager:
    """
    Manages the topology of the cluster, providing information about
    network structure, node relationships, and service dependencies.
    
    Features:
    - Automated topology discovery
    - Topology visualization
    - Critical path analysis
    - Failure impact prediction
    - Historical topology tracking
    """
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the topology manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        
        # Set default configuration values
        self.discovery_interval = self.config.get("discovery_interval", 3600)
        self.update_interval = self.config.get("update_interval", 300)
        self.max_history = self.config.get("max_history", 100)
        self.discovery_method = TopologyDiscoveryMethod(self.config.get("discovery_method", "network_scan"))
        
        # Initialize Redis client if configured
        self.redis_client = None
        if self.config.get("redis_enabled", True):
            self.redis_client = self._init_redis_client()
        
        # Directories for persistent storage
        self.data_dir = Path(self.config.get("data_directory", "data/topology"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.history_dir = self.data_dir / "history"
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize topology data structures
        self.nodes = {}  # Dict[node_id, NodeInfo]
        self.services = {}  # Dict[service_id, ServiceInfo]
        self.links = []  # List[NetworkLink]
        
        # Network graph for topology operations
        self.graph = nx.Graph()
        
        # Locks for thread safety
        self.nodes_lock = threading.RLock()
        self.services_lock = threading.RLock()
        self.links_lock = threading.RLock()
        self.graph_lock = threading.RLock()
        
        # Topology history
        self.topology_history = []  # List of timestamped topology snapshots
        
        # Event listeners
        self.listeners = {}  # Dict[event_type, List[callback]]
        self.listeners_lock = threading.RLock()
        
        # Threads
        self.running = False
        self.discovery_thread = None
        self.update_thread = None
        self.pubsub_thread = None
        self.pubsub = None
        
        logger.info("Cluster Topology Manager initialized")
    
    def _load_config(self, config_path: Optional[Union[str, Path]]) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            "discovery_interval": 3600,  # 1 hour
            "update_interval": 300,      # 5 minutes
            "max_history": 100,
            "redis_enabled": True,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_password": None,
            "redis_namespace": "clustersentry",
            "discovery_method": "network_scan",
            "discovery_subnets": ["192.168.1.0/24"],
            "discovery_exclude_ips": ["192.168.1.1"],
            "node_stale_threshold": 86400,  # 24 hours
            "data_directory": "data/topology",
            "log_level": "INFO"
        }
        
        if not config_path:
            logger.warning("No configuration path provided, using defaults")
            return default_config
        
        try:
            path = Path(config_path)
            if not path.exists():
                logger.warning(f"Configuration file not found: {path}, using defaults")
                return default_config
            
            with open(path, 'r') as f:
                if path.suffix.lower() == '.yaml' or path.suffix.lower() == '.yml':
                    import yaml
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            
            # Merge with defaults for backward compatibility
            merged_config = default_config.copy()
            merged_config.update(config)
            
            logger.info(f"Loaded configuration from {path}")
            return merged_config
        
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return default_config
    
    def _init_redis_client(self) -> Optional[redis.Redis]:
        """Initialize Redis client"""
        try:
            client = redis.Redis(
                host=self.config.get("redis_host", "localhost"),
                port=self.config.get("redis_port", 6379),
                password=self.config.get("redis_password"),
                db=self.config.get("redis_db", 0),
                decode_responses=True
            )
            
            # Test connection
            client.ping()
            logger.info("Connected to Redis")
            return client
        
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return None
    
    def start(self):
        """Start topology management threads"""
        if self.running:
            logger.warning("Topology manager is already running")
            return
        
        self.running = True
        
        # Start discovery thread
        self.discovery_thread = threading.Thread(
            target=self._discovery_loop,
            daemon=True
        )
        self.discovery_thread.start()
        
        # Start update thread
        self.update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )
        self.update_thread.start()
        
        # Start Redis pubsub thread if Redis is available
        if self.redis_client:
            self._setup_redis_subscription()
        
        # Load existing topology data if available
        self._load_topology_data()
        
        logger.info("Topology manager started")
    
    def stop(self):
        """Stop topology management threads"""
        if not self.running:
            logger.warning("Topology manager is not running")
            return
        
        self.running = False
        
        # Save current topology data
        self._save_topology_data()
        
        # Wait for threads to stop
        for thread in [self.discovery_thread, self.update_thread, self.pubsub_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5.0)
        
        # Clean up Redis subscription
        if self.pubsub:
            self.pubsub.unsubscribe()
        
        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
        
        logger.info("Topology manager stopped")
    
    def _setup_redis_subscription(self):
        """Set up Redis subscription for topology updates"""
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(f"{namespace}:topology_updates")
            
            # Start pubsub listener thread
            self.pubsub_thread = threading.Thread(
                target=self._pubsub_listener,
                daemon=True
            )
            self.pubsub_thread.start()
            
            logger.info("Redis subscription set up")
        except Exception as e:
            logger.error(f"Failed to set up Redis subscription: {e}")
    
    def _pubsub_listener(self):
        """Listen for topology updates from Redis"""
        logger.info("Redis pubsub listener started")
        
        try:
            while self.running:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        data = json.loads(message["data"])
                        update_type = data.get("type")
                        
                        if update_type == "node_update":
                            node_id = data.get("node_id")
                            node_data = data.get("node_info")
                            
                            if node_id and node_data:
                                # Update node info
                                node_info = NodeInfo(**node_data)
                                self.update_node(node_info)
                        
                        elif update_type == "service_update":
                            service_id = data.get("service_id")
                            service_data = data.get("service_info")
                            
                            if service_id and service_data:
                                # Update service info
                                service_info = ServiceInfo(**service_data)
                                self.update_service(service_info)
                        
                        elif update_type == "link_update":
                            link_data = data.get("link_info")
                            
                            if link_data:
                                # Update link info
                                link_info = NetworkLink(**link_data)
                                self.update_link(link_info)
                        
                        elif update_type == "topology_updated":
                            # Reload entire topology
                            if self.redis_client:
                                self._load_topology_from_redis()
                    
                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON in topology update")
                    except Exception as e:
                        logger.error(f"Error processing topology update: {e}")
                
                time.sleep(0.1)  # Prevent CPU spinning
        
        except Exception as e:
            logger.error(f"Error in Redis pubsub listener: {e}")
        
        logger.info("Redis pubsub listener stopped")
    
    def _load_topology_data(self):
        """Load topology data from persistent storage"""
        # Try to load from Redis first if available
        if self.redis_client and self._load_topology_from_redis():
            logger.info("Loaded topology data from Redis")
            return
        
        # Otherwise, load from files
        try:
            # Load nodes
            nodes_path = self.data_dir / "nodes.json"
            if nodes_path.exists():
                with open(nodes_path, 'r') as f:
                    nodes_data = json.load(f)
                
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_data in nodes_data.items():
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_path = self.data_dir / "services.json"
            if services_path.exists():
                with open(services_path, 'r') as f:
                    services_data = json.load(f)
                
                with self.services_lock:
                    self.services = {}
                    for service_id, service_data in services_data.items():
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_path = self.data_dir / "links.json"
            if links_path.exists():
                with open(links_path, 'r') as f:
                    links_data = json.load(f)
                
                with self.links_lock:
                    self.links = []
                    for link_data in links_data:
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            logger.info("Loaded topology data from files")
        
        except Exception as e:
            logger.error(f"Error loading topology data: {e}")
    
    def _load_topology_from_redis(self) -> bool:
        """Load topology data from Redis"""
        if not self.redis_client:
            return False
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Load nodes
            nodes_key = f"{namespace}:topology:nodes"
            nodes_data = self.redis_client.hgetall(nodes_key)
            
            if nodes_data:
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_json in nodes_data.items():
                        node_data = json.loads(node_json)
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_key = f"{namespace}:topology:services"
            services_data = self.redis_client.hgetall(services_key)
            
            if services_data:
                with self.services_lock:
                    self.services = {}
                    for service_id, service_json in services_data.items():
                        service_data = json.loads(service_json)
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_key = f"{namespace}:topology:links"
            links_data = self.redis_client.lrange(links_key, 0, -1)
            
            if links_data:
                with self.links_lock:
                    self.links = []
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            return True
        
        except Exception as e:
            logger.error(f"Error loading topology from Redis: {e}")
            return False
    
    def _save_topology_data(self):
        """Save topology data to persistent storage"""
        try:
            # Save to Redis if available
            if self.redis_client:
                self._save_topology_to_redis()
            
            # Save nodes
            with self.nodes_lock:
                nodes_data = {node_id: asdict(node) for node_id, node in self.nodes.items()}
                
                with open(self.data_dir / "nodes.json", 'w') as f:
                    json.dump(nodes_data, f, indent=2)
            
            # Save services
            with self.services_lock:
                services_data = {service_id: asdict(service) for service_id, service in self.services.items()}
                
                with open(self.data_dir / "services.json", 'w') as f:
                    json.dump(services_data, f, indent=2)
            
            # Save links
            with self.links_lock:
                links_data = [asdict(link) for link in self.links]
                
                with open(self.data_dir / "links.json", 'w') as f:
                    json.dump(links_data, f, indent=2)
            
            logger.info("Saved topology data to files")
        
        except Exception as e:
            logger.error(f"Error saving topology data: {e}")
    
    def _save_topology_to_redis(self):
        """Save topology data to Redis"""
        if not self.redis_client:
            return
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Save nodes
            with self.nodes_lock:
                nodes_key = f"{namespace}:topology:nodes"
                nodes_data = {}
                
                for node_id, node in self.nodes.items():
                    nodes_data[node_id] = json.dumps(asdict(node))
                
                if nodes_data:
                    self.redis_client.delete(nodes_key)
                    self.redis_client.hset(nodes_key, mapping=nodes_data)
            
            # Save services
            with self.services_lock:
                services_key = f"{namespace}:topology:services"
                services_data = {}
                
                for service_id, service in self.services.items():
                    services_data[service_id] = json.dumps(asdict(service))
                
                if services_data:
                    self.redis_client.delete(services_key)
                    self.redis_client.hset(services_key, mapping=services_data)
            
            # Save links
            with self.links_lock:
                links_key = f"{namespace}:topology:links"
                
                self.redis_client.delete(links_key)
                
                for link in self.links:
                    self.redis_client.rpush(links_key, json.dumps(asdict(link)))
            
            logger.info("Saved topology data to Redis")
        
        except Exception as e:
            logger.error(f"Error saving topology to Redis: {e}")
    
    def _rebuild_graph(self):
        """Rebuild the network graph from nodes and links"""
        with self.graph_lock:
            # Create new graph
            self.graph = nx.Graph()
            
            # Add nodes
            with self.nodes_lock:
                for node_id, node in self.nodes.items():
                    self.graph.add_node(node_id, **asdict(node))
            
            # Add links
            with self.links_lock:
                for link in self.links:
                    self.graph.add_edge(
                        link.source_node,
                        link.target_node,
                        **asdict(link)
                    )
    
    def _discovery_loop(self):
        """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
                
                """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
        
        logger.info("Discovery loop stopped")
    
    def _update_loop(self):
        """Loop for updating existing topology information"""
        logger.info("Update loop started")
        
        while self.running:
            try:
                logger.info("Starting topology update cycle")
                
                # Update node statuses
                self._update_node_statuses()
                
                # Update service statuses
                self._update_service_statuses()
                
                # Update link statuses
                self._update_link_statuses()
                
                # Remove stale nodes
                self._cleanup_stale_nodes()
                
                # Notify listeners
                self._notify_listeners("topology_status_updated", {})
            
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
            
            # Sleep until next update cycle
            sleep_interval = min(30, self.update_interval)  # Check at most every 30 seconds
            for _ in range(int(self.update_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
        
        logger.info("Update loop stopped")
    
    def _discover_by_network_scan(self):
        """Discover topology by network scanning"""
        logger.info("Starting network scan discovery")
        
        # Get subnets to scan from configuration
        subnets = self.config.get("discovery_subnets", ["127.0.0.1/24"])
        exclude_ips = set(self.config.get("discovery_exclude_ips", []))
        
        discovered_nodes = []
        
        for subnet_str in subnets:
            try:
                # Parse subnet
                subnet = ipaddress.ip_network(subnet_str)
                logger.info(f"Scanning subnet: {subnet}")
                
                # Limit scan to prevent excessive scanning
                host_count = 0
                host_limit = min(1024, subnet.num_addresses)  # Reasonable limit for scanning
                
                # Scan subnet
                for ip in subnet.hosts():
                    ip_str = str(ip)
                    
                    # Skip excluded IPs
                    if ip_str in exclude_ips:
                        continue
                    
                    # Check if host is reachable
                    if self._ping_host(ip_str):
                        # Try to resolve hostname
                        hostname = self._resolve_hostname(ip_str) or ip_str
                        
                        # Create node ID from hostname
                        node_id = self._generate_node_id(hostname, ip_str)
                        
                        # Create node information
                        node_info = NodeInfo(
                            node_id=node_id,
                            hostname=hostname,
                            ip_address=ip_str,
                            status="unknown",
                            last_seen=time.time(),
                            last_updated=time.time()
                        )
                        
                        # Update or add node
                        self.update_node(node_info)
                        discovered_nodes.append(node_id)
                        
                        logger.info(f"Discovered node: {node_id} ({ip_str})")
                    
                    # Increment host count and check limit
                    host_count += 1
                    if host_count >= host_limit:
                        logger.warning(f"Host limit reached for subnet {subnet}, stopping scan")
                        break
            
            except Exception as e:
                logger.error(f"Error scanning subnet {subnet_str}: {e}")
        
        # Discover network links between nodes
        self._discover_network_links(discovered_nodes)
        
        # Discover services on nodes
        self._discover_services(discovered_nodes)
        
        logger.info(f"Network scan discovery completed: {len(discovered_nodes)} nodes discovered")
    
    def _discover_by_kubernetes(self):
        """Discover topology from Kubernetes API"""
        logger.info("Starting Kubernetes discovery")
        
        try:
            # Check if the kubernetes module is available
            try:
                from kubernetes import client, config
            except ImportError:
                logger.error("Kubernetes module not installed. Use 'pip install kubernetes'")
                return
            
            # Load configuration from default location
            try:
                config.load_kube_config()
            except Exception:
                # Try in-cluster config
                try:
                    config.load_incluster_config()
                except Exception as e:
                    logger.error(f"Failed to load Kubernetes configuration: {e}")
                    return
            
            # Create API clients
            core_api = client.CoreV1Api()
            apps_api = client.AppsV1Api()
            
            # Get nodes
            nodes = core_api.list_node().items
            for k8s_node in nodes:
                node_name = k8s_node.metadata.name
                
                # Get IP address
                ip_address = None
                for address in k8s_node.status.addresses:
                    if address.type == "InternalIP":
                        ip_address = address.address
                        break
                
                if not ip_address:
                    logger.warning(f"No IP address found for node {node_name}")
                    continue
                
                # Extract labels
                labels = k8s_node.metadata.labels or {}
                
                # Create node ID
                node_id = self._generate_node_id(node_name, ip_address)
                
                # Get resources
                resources = {}
                if k8s_node.status.capacity:
                    resources = {
                        "cpu": k8s_node.status.capacity.get("cpu"),
                        "memory": k8s_node.status.capacity.get("memory"),
                        "pods": k8s_node.status.capacity.get("pods")
                    }
                
                # Determine node status
                status = "unknown"
                if k8s_node.status.conditions:
                    for condition in k8s_node.status.conditions:
                        if condition.type == "Ready":
                            status = "healthy" if condition.status == "True" else "degraded"
                            break
                
                # Get location information
                region = labels.get("topology.kubernetes.io/region")
                zone = labels.get("topology.kubernetes.io/zone")
                
                # Create node information
                node_info = NodeInfo(
                    node_id=node_id,
                    hostname=node_name,
                    ip_address=ip_address,
                    status=status,
                    labels=labels,
                    region=region,
                    datacenter=zone,
                    node_type="worker",
                    resources=resources,
                    last_seen=time.time(),
                    last_updated=time.time()
                )
                
                # Update or add node
                self.update_node(node_info)
                logger.info(f"Discovered Kubernetes node: {node_id}")
            
            # Get services
            services = core_api.list_service_for_all_namespaces().items
            for k8s_service in services:
                service_name = k8s_service.metadata.name
                namespace = k8s_service.metadata.namespace
                
                # Create service ID
                service_id = f"{namespace}/{service_name}"
                
                # Get endpoints
                endpoints = []
                if k8s_service.spec.ports:
                    for port in k8s_service.spec.ports:
                        endpoint = {
                            "port": port.port,
                            "protocol": port.protocol,
                            "target_port": port.target_port
                        }
                        endpoints.append(endpoint)
                
                # Get selector
                selector = k8s_service.spec.selector or {}
                
                # Create service information
                service_info = ServiceInfo(
                    service_id=service_id,
                    service_name=service_name,
                    service_type="kubernetes",
                    endpoints=endpoints,
                    metadata={
                        "namespace": namespace,
                        "selector": selector,
                        "type": k8s_service.spec.type,
                        "cluster_ip": k8s_service.spec.cluster_ip
                    },
                    last_updated=time.time()
                )
                
                # Find nodes running this service
                if selector:
                    # Get pods with this selector
                    selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
                    pods = core_api.list_pod_for_all_namespaces(
                        label_selector=selector_str
                    ).items
                    
                    # Extract node names
                    node_names = set()
                    for pod in pods:
                        if pod.spec.node_name:
                            node_names.add(pod.spec.node_name)
                    
                    # Find corresponding node IDs
                    node_ids = []
                    with self.nodes_lock:
                        for node_id, node in self.nodes.items():
                            if node.hostname in node_names:
                                node_ids.append(node_id)
                    
                    service_info.nodes = node_ids
                
                # Find service dependencies
                # This would require more complex analysis of Kubernetes resources
                
                # Update or add service
                self.update_service(service_info)
                logger.info(f"Discovered Kubernetes service: {service_id}")
            
            # Discover network links
            # In Kubernetes, nodes are typically all connected to each other
            node_ids = []
            with self.nodes_lock:
                node_ids = list(self.nodes.keys())
            
            for i, source_id in enumerate(node_ids):
                for target_id in node_ids[i+1:]:
                    # Create link
                    link = NetworkLink(
                        source_node=source_id,
                        target_node=target_id,
                        link_type="kubernetes",
                        status="up",
                        last_updated=time.time()
                    )
                    
                    # Update or add link
                    self.update_link(link)
            
            logger.info(f"Kubernetes discovery completed: {len(node_ids)} nodes, {len(services)} services")
        
        except Exception as e:
            logger.error(f"Error in Kubernetes discovery: {e}")
    
    def _discover_by_consul(self):
        """Discover topology from Consul API"""
        logger.info("Starting Consul discovery")
        
        try:
            # Check if the consul module is available
            try:
                import consul
            except ImportError:
                logger.error("Consul module not installed. Use 'pip install python-consul'")
                return
            
            # Get Consul connection parameters from configuration
            consul_host = self.config.get("consul_host", "localhost")
            consul_port = self.config.get("consul_port", 8500)
            consul_token = self.config.get("consul_token")
            consul_scheme = self.config.get("consul_scheme", "http")
            
            # Create Consul client
            consul_client = consul.Consul(
                host=consul_host,
                port=consul_port,
                token=consul_token,
                scheme=consul_scheme
            )
            
            # Get nodes
            index, nodes = consul_client.catalog.nodes()
            
            for node in nodes:
                node_name = node["Node"]
                ip_address = node["Address"]
                
                # Create node ID
                node_id = self._generate_node_id(node_name, ip_address)
                
                # Get node health
                index, health = consul_client.health.node(node_name)
                
                # Determine node status
                status = "unknown"
                if health:
                    status_counts = {"passing": 0, "warning": 0, "critical": 0}
                    for check in health:
                        status_counts[check["Status"]] = status_counts.get(check["Status"], 0) + 1
                    
                    if status_counts.get("critical", 0) > 0:
                        status = "critical"
                    elif status_counts.get("warning", 0) > 0:
                        status = "degraded"
                    elif status_counts.get("passing", 0) > 0:
                        status = "healthy"
                
                # Get node metadata
                index, node_info = consul_client.catalog.node(node_name)
                datacenter = node_info.get("Datacenter")
                
                # Create node information
                node_info = NodeInfo(
                    node_id=node_id,
                    hostname=node_name,
                    ip_address=ip_address,
                    status=status,
                    datacenter=datacenter,
                    last_seen=time.time(),
                    last_updated=time.time()
                )
                
                # Update or add node
                self.update_node(node_info)
                logger.info(f"Discovered Consul node: {node_id}")
            
            # Get services
            index, services = consul_client.catalog.services()
            
            for service_name, tags in services.items():
                # Get instances of this service
                index, service_instances = consul_client.catalog.service(service_name)
                
                if not service_instances:
                    continue
                
                # Create service ID
                service_id = f"consul/{service_name}"
                
                # Collect nodes running this service
                node_ids = []
                endpoints = []
                
                for instance in service_instances:
                    node_name = instance["Node"]
                    
                    # Find corresponding node ID
                    with self.nodes_lock:
                        for node_id, node in self.nodes.items():
                            if node.hostname == node_name:
                                node_ids.append(node_id)
                                break
                    
                    # Create endpoint
                    endpoint = {
                        "host": instance["ServiceAddress"] or instance["Address"],
                        "port": instance["ServicePort"],
                        "tags": instance["ServiceTags"]
                    }
                    endpoints.append(endpoint)
                
                # Create service information
                service_info = ServiceInfo(
                    service_id=service_id,
                    service_name=service_name,
                    service_type="consul",
                    nodes=node_ids,
                    endpoints=endpoints,
                    metadata={"tags": tags},
                    last_updated=time.time()
                )
                
                # Update or add service
                self.update_service(service_info)
                logger.info(f"Discovered Consul service: {service_id}")
            
            # Discover dependencies through service health checks
            # This is more complex and would require analyzing service health checks
            
            logger.info(f"Consul discovery completed: {len(nodes)} nodes, {len(services)} services")
        
        except Exception as e:
            logger.error(f"Error in Consul discovery: {e}")
    
    def _discover_by_zookeeper(self):
        """Discover topology from ZooKeeper"""
        logger.info("Starting ZooKeeper discovery")
        
        try:
            # Check if the kazoo module is available
            try:
                from kazoo.client import KazooClient
            except ImportError:
                logger.error("Kazoo module not installed. Use 'pip install kazoo'")
                return
            
            # Get ZooKeeper connection parameters from configuration
            zk_hosts = self.config.get("zookeeper_hosts", "localhost:2181")
            
            # Create ZooKeeper client
            zk = KazooClient(hosts=zk_hosts)
            zk.start()
            
            try:
                # ZooKeeper doesn't have a built-in service registry like Consul
                # This is just a basic implementation that assumes services register under /services
                
                # Get services
                if zk.exists("/services"):
                    services = zk.get_children("/services")
                    
                    for service_name in services:
                        # Get instances of this service
                        service_path = f"/services/{service_name}"
                        if zk.exists(service_path):
                            instances = zk.get_children(service_path)
                            
                            if not instances:
                                continue
                            
                            # Create service ID
                            service_id = f"zookeeper/{service_name}"
                            
                            # Collect endpoints
                            endpoints = []
                            metadata = {}
                            
                            for instance_id in instances:
                                instance_path = f"{service_path}/{instance_id}"
                                if zk.exists(instance_path):
                                    data, _ = zk.get(instance_path)
                                    
                                    try:
                                        instance_data = json.loads(data.decode('utf-8'))
                                        
                                        # Extract endpoint information
                                        if "host" in instance_data and "port" in instance_data:
                                            endpoint = {
                                                "host": instance_data["host"],
                                                "port": instance_data["port"]
                                            }
                                            endpoints.append(endpoint)
                                        
                                        # Extract metadata
                                        if "metadata" in instance_data:
                                            metadata.update(instance_data["metadata"])
                                    
                                    except (json.JSONDecodeError, UnicodeDecodeError):
                                        logger.warning(f"Invalid data format for instance {instance_id}")
                            
                            # Create service information
                            service_info = ServiceInfo(
                                service_id=service_id,
                                service_name=service_name,
                                service_type="zookeeper",
                                endpoints=endpoints,
                                metadata=metadata,
                                last_updated=time.time()
                            )
                            
                            # Update or add service
                            self.update_service(service_info)
                            logger.info(f"Discovered ZooKeeper service: {service_id}")
                
                logger.info(f"ZooKeeper discovery completed")
            
            finally:
                zk.stop()
                zk.close()
        
        except Exception as e:
            logger.error(f"Error in ZooKeeper discovery: {e}")
    
    def _discover_network_links(self, node_ids: List[str]):
        """Discover network links between nodes"""
        logger.info(f"Discovering network links for {len(node_ids)} nodes")
        
        # Get node information
        nodes = {}
        with self.nodes_lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    nodes[node_id] = self.nodes[node_id]
        
        # Check connectivity between nodes
        for i, (source_id, source_node) in enumerate(nodes.items()):
            for target_id, target_node in list(nodes.items())[i+1:]:
                try:
                    # Check connectivity
                    if self._check_connectivity(source_node.ip_address, target_node.ip_address):
                        # Measure network metrics
                        metrics = self._measure_network_metrics(source_node.ip_address, target_node.ip_address)
                        
                        # Create link
                        link = NetworkLink(
                            source_node=source_id,
                            target_node=target_id,
                            link_type="ethernet",
                            status="up",
                            last_updated=time.time()
                        )
                        
                        if metrics:
                            link.bandwidth = metrics.get("bandwidth")
                            link.latency = metrics.get("latency")
                            link.packet_loss = metrics.get("packet_loss")
                        
                        # Update or add link
                        self.update_link(link)
                        logger.info(f"Discovered network link: {source_id} - {target_id}")
                
                except Exception as e:
                    logger.error(f"Error discovering link {source_id} - {target_id}: {e}")
    
    def _discover_services(self, node_ids: List[str]):
        """Discover services running on nodes"""
        logger.info(f"Discovering services for {len(node_ids)} nodes")
        
        # Get node information
        nodes = {}
        with self.nodes_lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    nodes[node_id] = self.nodes[node_id]
        
        # Common service ports to check
        service_ports = {
            "http": 80,
            "https": 443,
            "ssh": 22,
            "mysql": 3306,
            "postgresql": 5432,
            "redis": 6379,
            "mongodb": 27017,
            "elasticsearch": 9200,
            "kafka": 9092,
            "zookeeper": 2181
        }
        
        # Check for services on each node
        for node_id, node in nodes.items():
            services_found = []
            
            for service_name, port in service_ports.items():
                try:
                    if self._check_port(node.ip_address, port):
                        # Create service ID
                        service_id = f"{service_name}-{node_id}"
                        
                        # Check if service exists
                        with self.services_lock:
                            if service_id in self.services:
                                # Update existing service
                                service = self.services[service_id]
                                if node_id not in service.nodes:
                                    service.nodes.append(node_id)
                                service.last_updated = time.time()
                            else:
                                # Create new service
                                service_info = ServiceInfo(
                                    service_id=service_id,
                                    service_name=service_name,
                                    service_type="detected",
                                    nodes=[node_id],
                                    endpoints=[{
                                        "host": node.ip_address,
                                        "port": port,
                                        "protocol": "tcp"
                                    }],
                                    status="unknown",
                                    last_updated=time.time()
                                )
                                
                                self.update_service(service_info)
                        
                        services_found.append(service_name)
                        logger.info(f"Discovered service: {service_name} on {node_id}")
                
                except Exception as e:
                    logger.error(f"Error checking service {service_name} on {node_id}: {e}")
            
            # Update node's service list
            if services_found:
                with self.nodes_lock:
                    if node_id in self.nodes:
                        self.nodes[node_id].services = services_found
            
            # Try to detect services using zeroconf/mDNS if available
            self._discover_mdns_services(node_id, node)
    
    def _discover_mdns_services(self, node_id: str, node: NodeInfo):
        """Discover services using mDNS/Zeroconf"""
        try:
            # Check if the zeroconf module is available
            try:
                from zeroconf import ServiceBrowser, Zeroconf
            except ImportError:
                return  # Silently skip if not available
            
            class ServiceListener:
                def __init__(self, outer, node_id):
                    self.outer = outer
                    self.node_id = node_id
                    self.services_found = []
                
                def add_service(self, zc, type, name):
                    info = zc.get_service_info(type, name)
                    if info:
                        service_name = name.split('.')[0]
                        service_type = type.split('.')[0]
                        
                        # Extract IP and port
                        ip = ".".join(str(x) for x in info.addresses[0]) if info.addresses else None
                        port = info.port
                        
                        if ip and port:
                            # Create service ID
                            service_id = f"{service_type}-{self.node_id}"
                            
                            # Create service info
                            service_info = ServiceInfo(
                                service_id=service_id,
                                service_name=service_name,
                                service_type="mdns",
                                nodes=[self.node_id],
                                endpoints=[{
                                    "host": ip,
                                    "port": port,
                                    "protocol": "tcp"
                                }],
                                status="unknown",
                                metadata={"mdns_type": type},
                                last_updated=time.time()
                            )
                            
                            self.outer.update_service(service_info)
                            self.services_found.append(service_type)
                            logger.info(f"Discovered mDNS service: {service_name} on {self.node_id}")
            
            # Create zeroconf instance
            zc = Zeroconf()
            listener = ServiceListener(self, node_id)
            
            # Browse for common service types
            browser = ServiceBrowser(zc, "_http._tcp.local.", listener)
            browser = ServiceBrowser(zc, "_ssh._tcp.local.", listener)
            
            # Wait a short time for discovery
            time.sleep(3)
            
            # Clean up
            zc.close()
            
            # Update node's service list with mDNS services
            if listener.services_found:
                with self.nodes_lock:
                    if node_id in self.nodes:
                        self.nodes[node_id].services.extend(listener.services_found)
        
        except Exception as e:
            logger.debug(f"Error in mDNS discovery for {node_id}: {e}")
    
    def _update_node_statuses(self):
        """Update status of all nodes"""
        logger.info("Updating node statuses")
        
        with self.nodes_lock:
            for node_id, node in list(self.nodes.items()):
                try:
                    # Skip recently updated nodes
                    if time.time() - node.last_updated < 60:  # Don't update nodes updated in the last minute
                        continue
                    
                    # Check if node is reachable
                    reachable = self._ping_host(node.ip_address)
                    
                    # Update status
                    if reachable:
                        if node.status == "offline":
                            node.status = "unknown"  # Reset to unknown when it comes back online
                            logger.info(f"Node {node_id} is now reachable")
                        
                        node.last_seen = time.time()
                    else:
                        if node.status != "offline":
                            node.status = "offline"
                            logger.warning(f"Node {node_id} is unreachable")
                    
                    node.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for node {node_id}: {e}")
    
    def _update_service_statuses(self):
        """Update status of all services"""
        logger.info("Updating service statuses")
        
        with self.services_lock, self.nodes_lock:
            for service_id, service in list(self.services.items()):
                try:
                    # Skip recently updated services
                    if time.time() - service.last_updated < 60:  # Don't update services updated in the last minute
                        continue
                    
                    # Check node status for each service node
                    offline_nodes = 0
                    for node_id in service.nodes:
                        if node_id in self.nodes and self.nodes[node_id].status == "offline":
                            offline_nodes += 1
                    
                    # Update service status based on node status
                    if not service.nodes:
                        service.status = "unavailable"
                    elif offline_nodes == len(service.nodes):
                        service.status = "unavailable"
                    elif offline_nodes > 0:
                        service.status = "degraded"
                    else:
                        # Check if any endpoint is reachable
                        available = False
                        for endpoint in service.endpoints:
                            host = endpoint.get("host")
                            port = endpoint.get("port")
                            
                            if host and port and self._check_port(host, port):
                                available = True
                                break
                        
                        service.status = "available" if available else "degraded"
                    
                    service.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for service {service_id}: {e}")
    
    def _update_link_statuses(self):
        """Update status of all network links"""
        logger.info("Updating link statuses")
        
        with self.links_lock, self.nodes_lock:
            for link in list(self.links):
                try:
                    # Skip recently updated links
                    if time.time() - link.last_updated < 300:  # Don't update links updated in the last 5 minutes
                        continue
                    
                    # Get node information
                    source_node = self.nodes.get(link.source_node)
                    target_node = self.nodes.get(link.target_node)
                    
                    if not source_node or not target_node:
                        continue
                    
                    # Check connectivity
                    if source_node.status != "offline" and target_node.status != "offline":
                        connectivity = self._check_connectivity(source_node.ip_address, target_node.ip_address)
                        
                        # Update status
                        link.status = "up" if connectivity else "down"
                        
                        # Update metrics if needed
                        if connectivity and (link.bandwidth is None or time.time() - link.last_updated > 3600):
                            metrics = self._measure_network_metrics(source_node.ip_address, target_node.ip_address)
                            
                            if metrics:
                                link.bandwidth = metrics.get("bandwidth")
                                link.latency = metrics.get("latency")
                                link.packet_loss = metrics.get("packet_loss")
                    elif source_node.status == "offline" or target_node.status == "offline":
                        # Mark as down if either node is offline
                        link.status = "down"
                    
                    link.last#!/usr/bin/env python3
"""
Cluster Topology Manager for ClusterSentry

This module discovers and maintains the topology of the cluster, providing
information about network structure, node relationships, and service dependencies
for intelligent failure impact assessment and recovery planning.
"""

import os
import sys
import time
import json
import logging
import threading
import ipaddress
import socket
import subprocess
import redis
import networkx as nx
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Union, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("topology_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TopologyManager")

@dataclass
class NodeInfo:
    """Information about a node in the cluster topology"""
    node_id: str
    hostname: str
    ip_address: str
    status: str = "unknown"  # unknown, healthy, degraded, critical, offline
    labels: Dict[str, str] = field(default_factory=dict)
    rack: Optional[str] = None
    datacenter: Optional[str] = None
    region: Optional[str] = None
    node_type: str = "worker"  # master, worker, storage, etc.
    services: List[str] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    neighbors: List[str] = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)

@dataclass
class ServiceInfo:
    """Information about a service in the cluster topology"""
    service_id: str
    service_name: str
    service_type: str
    nodes: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    endpoints: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "unknown"  # unknown, available, degraded, unavailable
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

@dataclass
class NetworkLink:
    """Information about a network link between nodes"""
    source_node: str
    target_node: str
    link_type: str = "ethernet"  # ethernet, infiniband, fiber, etc.
    bandwidth: Optional[float] = None  # Mbps
    latency: Optional[float] = None  # ms
    packet_loss: Optional[float] = None  # percentage
    status: str = "unknown"  # unknown, up, down, degraded
    last_updated: float = field(default_factory=time.time)

class TopologyDiscoveryMethod(Enum):
    """Methods for topology discovery"""
    NETWORK_SCAN = "network_scan"
    KUBERNETES = "kubernetes"
    CONSUL = "consul"
    ZOOKEEPER = "zookeeper"
    MANUAL = "manual"
    IMPORT = "import"

class ClusterTopologyManager:
    """
    Manages the topology of the cluster, providing information about
    network structure, node relationships, and service dependencies.
    
    Features:
    - Automated topology discovery
    - Topology visualization
    - Critical path analysis
    - Failure impact prediction
    - Historical topology tracking
    """
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the topology manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        
        # Set default configuration values
        self.discovery_interval = self.config.get("discovery_interval", 3600)
        self.update_interval = self.config.get("update_interval", 300)
        self.max_history = self.config.get("max_history", 100)
        self.discovery_method = TopologyDiscoveryMethod(self.config.get("discovery_method", "network_scan"))
        
        # Initialize Redis client if configured
        self.redis_client = None
        if self.config.get("redis_enabled", True):
            self.redis_client = self._init_redis_client()
        
        # Directories for persistent storage
        self.data_dir = Path(self.config.get("data_directory", "data/topology"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.history_dir = self.data_dir / "history"
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize topology data structures
        self.nodes = {}  # Dict[node_id, NodeInfo]
        self.services = {}  # Dict[service_id, ServiceInfo]
        self.links = []  # List[NetworkLink]
        
        # Network graph for topology operations
        self.graph = nx.Graph()
        
        # Locks for thread safety
        self.nodes_lock = threading.RLock()
        self.services_lock = threading.RLock()
        self.links_lock = threading.RLock()
        self.graph_lock = threading.RLock()
        
        # Topology history
        self.topology_history = []  # List of timestamped topology snapshots
        
        # Event listeners
        self.listeners = {}  # Dict[event_type, List[callback]]
        self.listeners_lock = threading.RLock()
        
        # Threads
        self.running = False
        self.discovery_thread = None
        self.update_thread = None
        self.pubsub_thread = None
        self.pubsub = None
        
        logger.info("Cluster Topology Manager initialized")
    
    def _load_config(self, config_path: Optional[Union[str, Path]]) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            "discovery_interval": 3600,  # 1 hour
            "update_interval": 300,      # 5 minutes
            "max_history": 100,
            "redis_enabled": True,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_password": None,
            "redis_namespace": "clustersentry",
            "discovery_method": "network_scan",
            "discovery_subnets": ["192.168.1.0/24"],
            "discovery_exclude_ips": ["192.168.1.1"],
            "node_stale_threshold": 86400,  # 24 hours
            "data_directory": "data/topology",
            "log_level": "INFO"
        }
        
        if not config_path:
            logger.warning("No configuration path provided, using defaults")
            return default_config
        
        try:
            path = Path(config_path)
            if not path.exists():
                logger.warning(f"Configuration file not found: {path}, using defaults")
                return default_config
            
            with open(path, 'r') as f:
                if path.suffix.lower() == '.yaml' or path.suffix.lower() == '.yml':
                    import yaml
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            
            # Merge with defaults for backward compatibility
            merged_config = default_config.copy()
            merged_config.update(config)
            
            logger.info(f"Loaded configuration from {path}")
            return merged_config
        
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return default_config
    
    def _init_redis_client(self) -> Optional[redis.Redis]:
        """Initialize Redis client"""
        try:
            client = redis.Redis(
                host=self.config.get("redis_host", "localhost"),
                port=self.config.get("redis_port", 6379),
                password=self.config.get("redis_password"),
                db=self.config.get("redis_db", 0),
                decode_responses=True
            )
            
            # Test connection
            client.ping()
            logger.info("Connected to Redis")
            return client
        
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return None
    
    def start(self):
        """Start topology management threads"""
        if self.running:
            logger.warning("Topology manager is already running")
            return
        
        self.running = True
        
        # Start discovery thread
        self.discovery_thread = threading.Thread(
            target=self._discovery_loop,
            daemon=True
        )
        self.discovery_thread.start()
        
        # Start update thread
        self.update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )
        self.update_thread.start()
        
        # Start Redis pubsub thread if Redis is available
        if self.redis_client:
            self._setup_redis_subscription()
        
        # Load existing topology data if available
        self._load_topology_data()
        
        logger.info("Topology manager started")
    
    def stop(self):
        """Stop topology management threads"""
        if not self.running:
            logger.warning("Topology manager is not running")
            return
        
        self.running = False
        
        # Save current topology data
        self._save_topology_data()
        
        # Wait for threads to stop
        for thread in [self.discovery_thread, self.update_thread, self.pubsub_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5.0)
        
        # Clean up Redis subscription
        if self.pubsub:
            self.pubsub.unsubscribe()
        
        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
        
        logger.info("Topology manager stopped")
    
    def _setup_redis_subscription(self):
        """Set up Redis subscription for topology updates"""
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(f"{namespace}:topology_updates")
            
            # Start pubsub listener thread
            self.pubsub_thread = threading.Thread(
                target=self._pubsub_listener,
                daemon=True
            )
            self.pubsub_thread.start()
            
            logger.info("Redis subscription set up")
        except Exception as e:
            logger.error(f"Failed to set up Redis subscription: {e}")
    
    def _pubsub_listener(self):
        """Listen for topology updates from Redis"""
        logger.info("Redis pubsub listener started")
        
        try:
            while self.running:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        data = json.loads(message["data"])
                        update_type = data.get("type")
                        
                        if update_type == "node_update":
                            node_id = data.get("node_id")
                            node_data = data.get("node_info")
                            
                            if node_id and node_data:
                                # Update node info
                                node_info = NodeInfo(**node_data)
                                self.update_node(node_info)
                        
                        elif update_type == "service_update":
                            service_id = data.get("service_id")
                            service_data = data.get("service_info")
                            
                            if service_id and service_data:
                                # Update service info
                                service_info = ServiceInfo(**service_data)
                                self.update_service(service_info)
                        
                        elif update_type == "link_update":
                            link_data = data.get("link_info")
                            
                            if link_data:
                                # Update link info
                                link_info = NetworkLink(**link_data)
                                self.update_link(link_info)
                        
                        elif update_type == "topology_updated":
                            # Reload entire topology
                            if self.redis_client:
                                self._load_topology_from_redis()
                    
                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON in topology update")
                    except Exception as e:
                        logger.error(f"Error processing topology update: {e}")
                
                time.sleep(0.1)  # Prevent CPU spinning
        
        except Exception as e:
            logger.error(f"Error in Redis pubsub listener: {e}")
        
        logger.info("Redis pubsub listener stopped")
    
    def _load_topology_data(self):
        """Load topology data from persistent storage"""
        # Try to load from Redis first if available
        if self.redis_client and self._load_topology_from_redis():
            logger.info("Loaded topology data from Redis")
            return
        
        # Otherwise, load from files
        try:
            # Load nodes
            nodes_path = self.data_dir / "nodes.json"
            if nodes_path.exists():
                with open(nodes_path, 'r') as f:
                    nodes_data = json.load(f)
                
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_data in nodes_data.items():
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_path = self.data_dir / "services.json"
            if services_path.exists():
                with open(services_path, 'r') as f:
                    services_data = json.load(f)
                
                with self.services_lock:
                    self.services = {}
                    for service_id, service_data in services_data.items():
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_path = self.data_dir / "links.json"
            if links_path.exists():
                with open(links_path, 'r') as f:
                    links_data = json.load(f)
                
                with self.links_lock:
                    self.links = []
                    for link_data in links_data:
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            logger.info("Loaded topology data from files")
        
        except Exception as e:
            logger.error(f"Error loading topology data: {e}")
    
    def _load_topology_from_redis(self) -> bool:
        """Load topology data from Redis"""
        if not self.redis_client:
            return False
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Load nodes
            nodes_key = f"{namespace}:topology:nodes"
            nodes_data = self.redis_client.hgetall(nodes_key)
            
            if nodes_data:
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_json in nodes_data.items():
                        node_data = json.loads(node_json)
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_key = f"{namespace}:topology:services"
            services_data = self.redis_client.hgetall(services_key)
            
            if services_data:
                with self.services_lock:
                    self.services = {}
                    for service_id, service_json in services_data.items():
                        service_data = json.loads(service_json)
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_key = f"{namespace}:topology:links"
            links_data = self.redis_client.lrange(links_key, 0, -1)
            
            if links_data:
                with self.links_lock:
                    self.links = []
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            return True
        
        except Exception as e:
            logger.error(f"Error loading topology from Redis: {e}")
            return False
    
    def _save_topology_data(self):
        """Save topology data to persistent storage"""
        try:
            # Save to Redis if available
            if self.redis_client:
                self._save_topology_to_redis()
            
            # Save nodes
            with self.nodes_lock:
                nodes_data = {node_id: asdict(node) for node_id, node in self.nodes.items()}
                
                with open(self.data_dir / "nodes.json", 'w') as f:
                    json.dump(nodes_data, f, indent=2)
            
            # Save services
            with self.services_lock:
                services_data = {service_id: asdict(service) for service_id, service in self.services.items()}
                
                with open(self.data_dir / "services.json", 'w') as f:
                    json.dump(services_data, f, indent=2)
            
            # Save links
            with self.links_lock:
                links_data = [asdict(link) for link in self.links]
                
                with open(self.data_dir / "links.json", 'w') as f:
                    json.dump(links_data, f, indent=2)
            
            logger.info("Saved topology data to files")
        
        except Exception as e:
            logger.error(f"Error saving topology data: {e}")
    
    def _save_topology_to_redis(self):
        """Save topology data to Redis"""
        if not self.redis_client:
            return
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Save nodes
            with self.nodes_lock:
                nodes_key = f"{namespace}:topology:nodes"
                nodes_data = {}
                
                for node_id, node in self.nodes.items():
                    nodes_data[node_id] = json.dumps(asdict(node))
                
                if nodes_data:
                    self.redis_client.delete(nodes_key)
                    self.redis_client.hset(nodes_key, mapping=nodes_data)
            
            # Save services
            with self.services_lock:
                services_key = f"{namespace}:topology:services"
                services_data = {}
                
                for service_id, service in self.services.items():
                    services_data[service_id] = json.dumps(asdict(service))
                
                if services_data:
                    self.redis_client.delete(services_key)
                    self.redis_client.hset(services_key, mapping=services_data)
            
            # Save links
            with self.links_lock:
                links_key = f"{namespace}:topology:links"
                
                self.redis_client.delete(links_key)
                
                for link in self.links:
                    self.redis_client.rpush(links_key, json.dumps(asdict(link)))
            
            logger.info("Saved topology data to Redis")
        
        except Exception as e:
            logger.error(f"Error saving topology to Redis: {e}")
    
    def _rebuild_graph(self):
        """Rebuild the network graph from nodes and links"""
        with self.graph_lock:
            # Create new graph
            self.graph = nx.Graph()
            
            # Add nodes
            with self.nodes_lock:
                for node_id, node in self.nodes.items():
                    self.graph.add_node(node_id, **asdict(node))
            
            # Add links
            with self.links_lock:
                for link in self.links:
                    self.graph.add_edge(
                        link.source_node,
                        link.target_node,
                        **asdict(link)
                    )
    
    def _discovery_loop(self):
        """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
                link.status = "down"
                    
                link.last_updated = time.time()
                         except Exception as e:
                    logger.error(f"Error updating status for link {link.source_node} - {link.target_node}: {e}")
    
        def _cleanup_stale_nodes(self):
        """Remove nodes that haven't been seen for a long time"""
        logger.info("Cleaning up stale nodes")
        
        stale_threshold = self.config.get("node_stale_threshold", 86400)  # Default: 24 hours
        
        with self.nodes_lock:
            current_time = time.time()
            stale_nodes = []
            
            for node_id, node in self.nodes.items():
                if current_time - node.last_seen > stale_threshold:
                    stale_nodes.append(node_id)
            
            for node_id in stale_nodes:
                logger.info(f"Removing stale node: {node_id}")
                self.remove_node(node_id)
    
    def _create_topology_snapshot(self):
        """Create a snapshot of the current topology state"""
        logger.info("Creating topology snapshot")
        
        timestamp = time.time()
        
        # Create snapshot
        snapshot = {
            "timestamp": timestamp,
            "node_count": len(self.nodes),
            "service_count": len(self.services),
            "link_count": len(self.links),
            "topology": self.export_topology()
        }
        
        # Add to history
        self.topology_history.append(snapshot)
        
        # Limit history size
        if len(self.topology_history) > self.max_history:
            self.topology_history = self.topology_history[-self.max_history:]
        
        # Save snapshot to file
        try:
            snapshot_file = self.history_dir / f"{int(timestamp)}.json"
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot, f, indent=2)
            logger.info(f"Saved topology snapshot to {snapshot_file}")
        except Exception as e:
            logger.error(f"Error saving topology snapshot: {e}")
        
        # Save snapshot to Redis if available
        if self.redis_client:
            try:
                namespace = self.config.get("redis_namespace", "clustersentry")
                snapshot_key = f"{namespace}:topology_snapshots"
                
                self.redis_client.lpush(snapshot_key, json.dumps(snapshot))
                self.redis_client.ltrim(snapshot_key, 0, self.max_history - 1)
                logger.info("Saved topology snapshot to Redis")
            except Exception as e:
                logger.error(f"Error saving topology snapshot to Redis: {e}")
    
    def _notify_listeners(self, event_type: str, data: Dict[str, Any]):
        """Notify event listeners"""
        with self.listeners_lock:
            if event_type in self.listeners:
                for callback in self.listeners[event_type]:
                    try:
                        callback(event_type, data)
                    except Exception as e:
                        logger.error(f"Error in event listener: {e}")
    
    def add_listener(self, event_type: str, callback: callable) -> bool:
        """
        Add an event listener
        
        Args:
            event_type: Type of event to listen for
            callback: Function to call when event occurs
            
        Returns:
            True if successful, False otherwise
        """
        with self.listeners_lock:
            if event_type not in self.listeners:
                self.listeners[event_type] = []
            
            if callback not in self.listeners[event_type]:
                self.listeners[event_type].append(callback)
                logger.debug(f"Added listener for {event_type}")
                return True
            
            return False
    
    def remove_listener(self, event_type: str, callback: callable) -> bool:
        """
        Remove an event listener
        
        Args:
            event_type: Type of event
            callback: Function to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.listeners_lock:
            if event_type in self.listeners and callback in self.listeners[event_type]:
                self.listeners[event_type].remove(callback)
                logger.debug(f"Removed listener for {event_type}")
                return True
            
            return False
    
    def update_node(self, node_info: NodeInfo) -> bool:
        """
        Update or add a node
        
        Args:
            node_info: Node information
            
        Returns:
            True if successful, False otherwise
        """
        with self.nodes_lock:
            node_id = node_info.node_id
            
            # Check if node exists
            if node_id in self.nodes:
                # Update existing node
                existing_node = self.nodes[node_id]
                
                # Update fields while preserving some existing values
                node_info.services = existing_node.services
                node_info.neighbors = existing_node.neighbors
                
                # Keep the current status if it's more specific
                if existing_node.status not in ["unknown", "offline"] and node_info.status == "unknown":
                    node_info.status = existing_node.status
                
                # Keep existing location info if not provided
                if not node_info.rack and existing_node.rack:
                    node_info.rack = existing_node.rack
                if not node_info.datacenter and existing_node.datacenter:
                    node_info.datacenter = existing_node.datacenter
                if not node_info.region and existing_node.region:
                    node_info.region = existing_node.region
                
                # Keep last seen time if it's more recent
                node_info.last_seen = max(existing_node.last_seen, node_info.last_seen)
            
            # Update node
            self.nodes[node_id] = node_info
            
            # Update graph
            with self.graph_lock:
                if not self.graph.has_node(node_id):
                    self.graph.add_node(node_id, **asdict(node_info))
                else:
                    # Update node attributes
                    nx.set_node_attributes(self.graph, {node_id: asdict(node_info)})
            
            # Notify listeners
            self._notify_listeners("node_updated", {"node_id": node_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    nodes_key = f"{namespace}:topology:nodes"
                    
                    self.redis_client.hset(nodes_key, node_id, json.dumps(asdict(node_info)))
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "node_update",
                            "node_id": node_id,
                            "node_info": asdict(node_info)
                        })
                    )
                except Exception as e:
                    logger.error(f"Error updating node in Redis: {e}")
            
            return True
    
    def remove_node(self, node_id: str) -> bool:
        """
        Remove a node
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.nodes_lock:
            if node_id not in self.nodes:
                return False
            
            # Remove from nodes dictionary
            del self.nodes[node_id]
            
            # Remove from graph
            with self.graph_lock:
                if self.graph.has_node(node_id):
                    self.graph.remove_node(node_id)
            
            # Update services
            with self.services_lock:
                for service_id, service in self.services.items():
                    if node_id in service.nodes:
                        service.nodes.remove(node_id)
            
            # Remove links
            with self.links_lock:
                self.links = [link for link in self.links 
                            if link.source_node != node_id and link.target_node != node_id]
            
            # Notify listeners
            self._notify_listeners("node_removed", {"node_id": node_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    nodes_key = f"{namespace}:topology:nodes"
                    
                    self.redis_client.hdel(nodes_key, node_id)
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "node_removed",
                            "node_id": node_id
                        })
                    )
                except Exception as e:
                    logger.error(f"Error removing node from Redis: {e}")
            
            return True
    
    def update_service(self, service_info: ServiceInfo) -> bool:
        """
        Update or add a service
        
        Args:
            service_info: Service information
            
        Returns:
            True if successful, False otherwise
        """
        with self.services_lock:
            service_id = service_info.service_id
            
            # Update service
            self.services[service_id] = service_info
            
            # Notify listeners
            self._notify_listeners("service_updated", {"service_id": service_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    services_key = f"{namespace}:topology:services"
                    
                    self.redis_client.hset(services_key, service_id, json.dumps(asdict(service_info)))
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "service_update",
                            "service_id": service_id,
                            "service_info": asdict(service_info)
                        })
                    )
                except Exception as e:
                    logger.error(f"Error updating service in Redis: {e}")
            
            return True
    
    def remove_service(self, service_id: str) -> bool:
        """
        Remove a service
        
        Args:
            service_id: ID of the service to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.services_lock:
            if service_id not in self.services:
                return False
            
            # Remove from services dictionary
            del self.services[service_id]
            
            # Update nodes
            with self.nodes_lock:
                for node_id, node in self.nodes.items():
                    if service_id in node.services:
                        node.services.remove(service_id)
            
            # Notify listeners
            self._notify_listeners("service_removed", {"service_id": service_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    services_key = f"{namespace}:topology:services"
                    
                    self.redis_client.hdel(services_key, service_id)
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "service_removed",
                            "service_id": service_id
                        })
                    )
                except Exception as e:
                    logger.error(f"Error removing service from Redis: {e}")
            
            return True
    
    def update_link(self, link_info: NetworkLink) -> bool:
        """
        Update or add a network link
        
        Args:
            link_info: Link information
            
        Returns:
            True if successful, False otherwise
        """
        with self.links_lock:
            # Check if link exists
            found = False
            for i, link in enumerate(self.links):
                if (link.source_node == link_info.source_node and link.target_node == link_info.target_node) or \
                   (link.source_node == link_info.target_node and link.target_node == link_info.source_node):
                    # Update existing link
                    self.links[i] = link_info
                    found = True
                    break
            
            if not found:
                # Add new link
                self.links.append(link_info)
            
            # Update graph
            with self.graph_lock:
                if not self.graph.has_edge(link_info.source_node, link_info.target_node):
                    self.graph.add_edge(
                        link_info.source_node,
                        link_info.target_node,
                        **asdict(link_info)
                    )
                else:
                    # Update edge attributes
                    self.graph[link_info.source_node][link_info.target_node].update(asdict(link_info))
            
            # Notify listeners
            self._notify_listeners("link_updated", {
                "source_node": link_info.source_node,
                "target_node": link_info.target_node
            })
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    links_key = f"{namespace}:topology:links"
                    
                    # Get current links
                    links_data = self.redis_client.lrange(links_key, 0, -1)
                    links = []
                    
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        source = link_data.get("source_node")
                        target = link_data.get("target_node")
                        
                        if (source == link_info.source_node and target == link_info.target_node) or \
                           (source == link_info.target_node and target == link_info.source_node):
                            # Skip existing link
                            continue
                        
                        links.append(link_data)
                    
                    # Add updated link
                    links.append(asdict(link_info))
                    
                    # Replace links in Redis
                    pipeline = self.redis_client.pipeline()
                    pipeline.delete(links_key)
                    
                    for link in links:
                        pipeline.rpush(links_key, json.dumps(link))
                    
                    pipeline.execute()
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "link_update",
                            "link_info": asdict(link_info)
                        })
                    )
                except Exception as e:
                    logger.error(f"Error updating link in Redis: {e}")
            
            return True
    
    def remove_link(self, source_node: str, target_node: str) -> bool:
        """
        Remove a network link
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            
        Returns:
            True if successful, False otherwise
        """
        with self.links_lock:
            # Find link
            found = False
            for i, link in enumerate(self.links):
                if (link.source_node == source_node and link.target_node == target_node) or \
                   (link.source_node == target_node and link.target_node == source_node):
                    del self.links[i]
                    found = True
                    break
            
            if not found:
                return False
            
            # Update graph
            with self.graph_lock:
                if self.graph.has_edge(source_node, target_node):
                    self.graph.remove_edge(source_node, target_node)
            
            # Notify listeners
            self._notify_listeners("link_removed", {
                "source_node": source_node,
                "target_node": target_node
            })
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    links_key = f"{namespace}:topology:links"
                    
                    # Get current links
                    links_data = self.redis_client.lrange(links_key, 0, -1)
                    links = []
                    
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        link_source = link_data.get("source_node")
                        link_target = link_data.get("target_node")
                        
                        if (link_source == source_node and link_target == target_node) or \
                           (link_source == target_node and link_target == source_node):
                            # Skip removed link
                            continue
                        
                        links.append(link_data)
                    
                    # Replace links in Redis
                    pipeline = self.redis_client.pipeline()
                    pipeline.delete(links_key)
                    
                    for link in links:
                        pipeline.rpush(links_key, json.dumps(link))
                    
                    pipeline.execute()
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "link_removed",
                            "source_node": source_node,
                            "target_node": target_node
                        })
                    )
                except Exception as e:
                    logger.error(f"Error removing link from Redis: {e}")
            
            return True
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """
        Get node information
        
        Args:
            node_id: ID of the node
            
        Returns:
            NodeInfo if found, None otherwise
        """
        with self.nodes_lock:
            return self.nodes.get(node_id)
    
    def get_service(self, service_id: str) -> Optional[ServiceInfo]:
        """
        Get service information
        
        Args:
            service_id: ID of the service
            
        Returns:
            ServiceInfo if found, None otherwise
        """
        with self.services_lock:
            return self.services.get(service_id)
    
    def get_link(self, source_node: str, target_node: str) -> Optional[NetworkLink]:
        """
        Get link information
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            
        Returns:
            NetworkLink if found, None otherwise
        """
        with self.links_lock:
            for link in self.links:
                if (link.source_node == source_node and link.target_node == target_node) or \
                   (link.source_node == target_node and link.target_node == source_node):
                    return link
            
            return None
    
    def get_nodes(self) -> Dict[str, NodeInfo]:
        """
        Get all nodes
        
        Returns:
            Dictionary of node ID to NodeInfo
        """
        with self.nodes_lock:
            return self.nodes.copy()
    
    def get_services(self) -> Dict[str, ServiceInfo]:
        """
        Get all services
        
        Returns:
            Dictionary of service ID to ServiceInfo
        """
        with self.services_lock:
            return self.services.copy()
    
    def get_links(self) -> List[NetworkLink]:
        """
        Get all links
        
        Returns:
            List of NetworkLink
        """
        with self.links_lock:
            return self.links.copy()
    
    def get_node_services(self, node_id: str) -> List[ServiceInfo]:
        """
        Get services running on a node
        
        Args:
            node_id: ID of the node
            
        Returns:
            List of ServiceInfo
        """
        with self.services_lock:
            return [service for service in self.services.values() if node_id in service.nodes]
    
    def get_service_nodes(self, service_id: str) -> List[NodeInfo]:
        """
        Get nodes running a service
        
        Args:
            service_id: ID of the service
            
        Returns:
            List of NodeInfo
        """
        with self.nodes_lock, self.services_lock:
            service = self.services.get(service_id)
            if not service:
                return []
            
            return [self.nodes[node_id] for node_id in service.nodes if node_id in self.nodes]
    
    def get_node_neighbors(self, node_id: str) -> List[NodeInfo]:
        """
        Get neighbors of a node
        
        Args:
            node_id: ID of the node
            
        Returns:
            List of NodeInfo
        """
        with self.nodes_lock, self.graph_lock:
            if not self.graph.has_node(node_id):
                return []
            
            neighbor_ids = list(self.graph.neighbors(node_id))
            return [self.nodes[neighbor_id] for neighbor_id in neighbor_ids if neighbor_id in self.nodes]
    
    def find_path(self, source_node: str, target_node: str) -> List[str]:
        """
        Find shortest path between two nodes
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            
        Returns:
            List of node IDs in the path, empty if no path exists
        """
        with self.graph_lock:
            try:
                if not (self.graph.has_node(source_node) and self.graph.has_node(target_node)):
                    return []
                
                if not nx.has_path(self.graph, source_node, target_node):
                    return []
                
                return nx.shortest_path(self.graph, source_node, target_node)
            except nx.NetworkXNoPath:
                return []
            except Exception as e:
                logger.error(f"Error finding path: {e}")
                return []
    
    def find_nodes_by_label(self, key: str, value: str) -> List[NodeInfo]:
        """
        Find nodes with a specific label
        
        Args:
            key: Label key
            value: Label value
            
        Returns:
            List of matching NodeInfo
        """
        with self.nodes_lock:
            return [node for node in self.nodes.values() if node.labels.get(key) == value]
    
    def find_critical_nodes(self) -> List[str]:
        """
        Find critical nodes (articulation points) in the network
        
        Returns:
            List of node IDs
        """
        with self.graph_lock:
            try:
                return list(nx.articulation_points(self.graph))
            except Exception as e:
                logger.error(f"Error finding critical nodes: {e}")
                return []
    
    def find_service_dependencies(self, service_id: str) -> List[str]:
        """
        Find direct dependencies of a service
        
        Args:
            service_id: ID of the service
            
        Returns:
            List of dependent service IDs
        """
        with self.services_lock:
            service = self.services.get(service_id)
            if not service:
                return []
            
            return service.dependencies
    
    def find_dependent_services(self, service_id: str) -> List[str]:
        """
        Find services that depend on a given service
        
        Args:
            service_id: ID of the service
            
        Returns:
            List of service IDs that depend on this service
        """
        with self.services_lock:
            dependents = []
            for other_id, other_service in self.services.items():
                if service_id in other_service.dependencies:
                    dependents.append(other_id)
            
            return dependents
    
    def predict_failure_impact(self, node_id: str) -> Dict[str, Any]:
        """
        Predict the impact of a node failure
        
        Args:
            node_id: ID of the node to simulate failure for
            
        Returns:
            Impact assessment
        """
        with self.nodes_lock, self.services_lock, self.graph_lock:
            try:
                # Check if node exists
                if node_id not in self.nodes:
                    return {"error": f"Node {node_id} not found"}
                
                # Create a copy of the graph for simulation
                g = self.graph.copy()
                
                # Get pre-failure connected components
                pre_components = list(nx.connected_components(g))
                
                # Remove the node to simulate failure
                g.remove_node(node_id)
                
                # Get post-failure connected components
                post_components = list(nx.connected_components(g))
                
                # Find affected nodes (disconnected or isolated)
                affected_nodes = []
                
                if len(pre_components) != len(post_components):
                    # Network has been partitioned
                    # Find nodes that are now disconnected from the main component
                    
                    # Find the main component (largest)
                    main_component = max(post_components, key=len)
                    
                    # Find nodes not in the main component
                    for node in g.nodes():
                        if node not in main_component:
                            affected_nodes.append(node)
                
                # Find affected services
                affected_services = []
                
                # Services running on the failed node
                for service_id, service in self.services.items():
                    if node_id in service.nodes:
                        affected_services.append(service_id)
                
                # Services depending on affected services
                service_dependencies = {}
                for service_id in affected_services:
                    dependents = self.find_dependent_services(service_id)
                    for dependent in dependents:
                        if dependent not in affected_services:
                            affected_services.append(dependent)
                            service_dependencies[dependent] = service_id
                
                # Calculate impact severity
                severity = len(affected_nodes) + len(affected_services)
                
                return {
                    "node_id": node_id,
                    "network_partition": len(pre_components) != len(post_components),
                    "affected_nodes": affected_nodes,
                    "affected_services": affected_services,
                    "service_dependencies": service_dependencies,
                    "severity": severity
                }
            
            except Exception as e:
                logger.error(f"Error predicting failure impact: {e}")
                return {"error": str(e)}
    
    def export_topology(self) -> Dict[str, Any]:
        """
        Export the current topology
        
        Returns:
            Dictionary with topology data
        """
        with self.nodes_lock, self.services_lock, self.links_lock:
            return {
                "timestamp": time.time(),
                "nodes": {node_id: asdict(node) for node_id, node in self.nodes.items()},
                "services": {service_id: asdict(service) for service_id, service in self.services.items()},
                "links": [asdict(link) for link in self.links]
            }
    
    def import_topology(self, topology_data: Dict[str, Any]) -> bool:
        """
        Import topology data
        
        Args:
            topology_data: Topology data to import
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate data
            if not all(key in topology_data for key in ["nodes", "services", "links"]):
                logger.error("Invalid topology data: missing required keys")
                return False
            
            # Clear existing data
            with self.nodes_lock:
                self.nodes = {}
            
            with self.services_lock:
                self.services = {}
            
            with self.links_lock:
                self.links = []
            
            # Import nodes
            for node_id, node_data in topology_data["nodes"].items():
                try:
                    node_info = NodeInfo(**node_data)
                    self.update_node(node_info)
                except Exception as e:
                    logger.error(f"Error importing node {node_id}: {e}")
            
            # Import services
            for service_id, service_data in topology_data["services"].items():
                try:
                    service_info = ServiceInfo(**service_data)
                    self.update_service(service_info)
                except Exception as e:
                    logger.error(f"Error importing service {service_id}: {e}")
            
            # Import links
            for link_data in topology_data["links"]:
                try:
                    link_info = NetworkLink(**link_data)
                    self.update_link(link_info)
                except Exception as e:
                    logger.error(f"Error importing link: {e}")
            
            # Take a new snapshot
            self._create_topology_snapshot()
            
            logger.info("Topology import completed")
            return True
        
        except Exception as e:
            logger.error(f"Error importing topology: {e}")
            return False
    
    def compute_network_statistics(self) -> Dict[str, Any]:
        """
        Compute statistics about the network topology
        
        Returns:
            Dictionary with statistics
        """
        with self.graph_lock:
            try:
                stats = {
                    "node_count": self.graph.number_of_nodes(),
                    "link_count": self.graph.number_of_edges(),
                    "connected_components": nx.number_connected_components(self.graph),
                    "density": nx.density(self.graph),
                    "clustering_coefficient": nx.average_clustering(self.graph),
                }
                
                # Calculate additional metrics if the graph is connected
                if nx.is_connected(self.graph) and self.graph.number_of_nodes() > 1:
                    stats.update({
                        "diameter": nx.diameter(self.graph),
                        "radius": nx.radius(self.graph),
                        "average_shortest_path_length": nx.average_shortest_path_length(self.graph),
                        "critical_nodes": len(list(nx.articulation_points(self.graph)))
                    })
                
                # Node degree statistics
                degrees = [d for _, d in self.graph.degree()]
                if degrees:
                    stats.update({
                        "average_degree": sum(degrees) / len(degrees),
                        "max_degree": max(degrees),
                        "min_degree": min(degrees)
                    })
                
                return stats
            
            except Exception as e:
                logger.error(f"Error computing network statistics: {e}")
                return {
                    "error": str(e),
                    "node_count": len(self.nodes),
                    "link_count": len(self.links),
                    "service_count": len(self.services)
                }
    
    def generate_graphviz(self) -> str:
        """
        Generate Graphviz DOT representation of the topology
        
        Returns:
            Graphviz DOT string
        """
        with self.nodes_lock, self.services_lock, self.links_lock:
            try:
                dot = ["digraph ClusterTopology {"]
                dot.append("  graph [rankdir=LR, overlap=false, splines=true];")
                dot.append("  node [shape=box, style=filled, fontname=Arial];")
                dot.append("  edge [fontname=Arial];        """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
        
        logger.info("Discovery loop stopped")
    
    def _update_loop(self):
        """Loop for updating existing topology information"""
        logger.info("Update loop started")
        
        while self.running:
            try:
                logger.info("Starting topology update cycle")
                
                # Update node statuses
                self._update_node_statuses()
                
                # Update service statuses
                self._update_service_statuses()
                
                # Update link statuses
                self._update_link_statuses()
                
                # Remove stale nodes
                self._cleanup_stale_nodes()
                
                # Notify listeners
                self._notify_listeners("topology_status_updated", {})
            
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
            
            # Sleep until next update cycle
            sleep_interval = min(30, self.update_interval)  # Check at most every 30 seconds
            for _ in range(int(self.update_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
        
        logger.info("Update loop stopped")
    
    def _discover_by_network_scan(self):
        """Discover topology by network scanning"""
        logger.info("Starting network scan discovery")
        
        # Get subnets to scan from configuration
        subnets = self.config.get("discovery_subnets", ["127.0.0.1/24"])
        exclude_ips = set(self.config.get("discovery_exclude_ips", []))
        
        discovered_nodes = []
        
        for subnet_str in subnets:
            try:
                # Parse subnet
                subnet = ipaddress.ip_network(subnet_str)
                logger.info(f"Scanning subnet: {subnet}")
                
                # Limit scan to prevent excessive scanning
                host_count = 0
                host_limit = min(1024, subnet.num_addresses)  # Reasonable limit for scanning
                
                # Scan subnet
                for ip in subnet.hosts():
                    ip_str = str(ip)
                    
                    # Skip excluded IPs
                    if ip_str in exclude_ips:
                        continue
                    
                    # Check if host is reachable
                    if self._ping_host(ip_str):
                        # Try to resolve hostname
                        hostname = self._resolve_hostname(ip_str) or ip_str
                        
                        # Create node ID from hostname
                        node_id = self._generate_node_id(hostname, ip_str)
                        
                        # Create node information
                        node_info = NodeInfo(
                            node_id=node_id,
                            hostname=hostname,
                            ip_address=ip_str,
                            status="unknown",
                            last_seen=time.time(),
                            last_updated=time.time()
                        )
                        
                        # Update or add node
                        self.update_node(node_info)
                        discovered_nodes.append(node_id)
                        
                        logger.info(f"Discovered node: {node_id} ({ip_str})")
                    
                    # Increment host count and check limit
                    host_count += 1
                    if host_count >= host_limit:
                        logger.warning(f"Host limit reached for subnet {subnet}, stopping scan")
                        break
            
            except Exception as e:
                logger.error(f"Error scanning subnet {subnet_str}: {e}")
        
        # Discover network links between nodes
        self._discover_network_links(discovered_nodes)
        
        # Discover services on nodes
        self._discover_services(discovered_nodes)
        
        logger.info(f"Network scan discovery completed: {len(discovered_nodes)} nodes discovered")
    
    def _discover_by_kubernetes(self):
        """Discover topology from Kubernetes API"""
        logger.info("Starting Kubernetes discovery")
        
        try:
            # Check if the kubernetes module is available
            try:
                from kubernetes import client, config
            except ImportError:
                logger.error("Kubernetes module not installed. Use 'pip install kubernetes'")
                return
            
            # Load configuration from default location
            try:
                config.load_kube_config()
            except Exception:
                # Try in-cluster config
                try:
                    config.load_incluster_config()
                except Exception as e:
                    logger.error(f"Failed to load Kubernetes configuration: {e}")
                    return
            
            # Create API clients
            core_api = client.CoreV1Api()
            apps_api = client.AppsV1Api()
            
            # Get nodes
            nodes = core_api.list_node().items
            for k8s_node in nodes:
                node_name = k8s_node.metadata.name
                
                # Get IP address
                ip_address = None
                for address in k8s_node.status.addresses:
                    if address.type == "InternalIP":
                        ip_address = address.address
                        break
                
                if not ip_address:
                    logger.warning(f"No IP address found for node {node_name}")
                    continue
                
                # Extract labels
                labels = k8s_node.metadata.labels or {}
                
                # Create node ID
                node_id = self._generate_node_id(node_name, ip_address)
                
                # Get resources
                resources = {}
                if k8s_node.status.capacity:
                    resources = {
                        "cpu": k8s_node.status.capacity.get("cpu"),
                        "memory": k8s_node.status.capacity.get("memory"),
                        "pods": k8s_node.status.capacity.get("pods")
                    }
                
                # Determine node status
                status = "unknown"
                if k8s_node.status.conditions:
                    for condition in k8s_node.status.conditions:
                        if condition.type == "Ready":
                            status = "healthy" if condition.status == "True" else "degraded"
                            break
                
                # Get location information
                region = labels.get("topology.kubernetes.io/region")
                zone = labels.get("topology.kubernetes.io/zone")
                
                # Create node information
                node_info = NodeInfo(
                    node_id=node_id,
                    hostname=node_name,
                    ip_address=ip_address,
                    status=status,
                    labels=labels,
                    region=region,
                    datacenter=zone,
                    node_type="worker",
                    resources=resources,
                    last_seen=time.time(),
                    last_updated=time.time()
                )
                
                # Update or add node
                self.update_node(node_info)
                logger.info(f"Discovered Kubernetes node: {node_id}")
            
            # Get services
            services = core_api.list_service_for_all_namespaces().items
            for k8s_service in services:
                service_name = k8s_service.metadata.name
                namespace = k8s_service.metadata.namespace
                
                # Create service ID
                service_id = f"{namespace}/{service_name}"
                
                # Get endpoints
                endpoints = []
                if k8s_service.spec.ports:
                    for port in k8s_service.spec.ports:
                        endpoint = {
                            "port": port.port,
                            "protocol": port.protocol,
                            "target_port": port.target_port
                        }
                        endpoints.append(endpoint)
                
                # Get selector
                selector = k8s_service.spec.selector or {}
                
                # Create service information
                service_info = ServiceInfo(
                    service_id=service_id,
                    service_name=service_name,
                    service_type="kubernetes",
                    endpoints=endpoints,
                    metadata={
                        "namespace": namespace,
                        "selector": selector,
                        "type": k8s_service.spec.type,
                        "cluster_ip": k8s_service.spec.cluster_ip
                    },
                    last_updated=time.time()
                )
                
                # Find nodes running this service
                if selector:
                    # Get pods with this selector
                    selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
                    pods = core_api.list_pod_for_all_namespaces(
                        label_selector=selector_str
                    ).items
                    
                    # Extract node names
                    node_names = set()
                    for pod in pods:
                        if pod.spec.node_name:
                            node_names.add(pod.spec.node_name)
                    
                    # Find corresponding node IDs
                    node_ids = []
                    with self.nodes_lock:
                        for node_id, node in self.nodes.items():
                            if node.hostname in node_names:
                                node_ids.append(node_id)
                    
                    service_info.nodes = node_ids
                
                # Find service dependencies
                # This would require more complex analysis of Kubernetes resources
                
                # Update or add service
                self.update_service(service_info)
                logger.info(f"Discovered Kubernetes service: {service_id}")
            
            # Discover network links
            # In Kubernetes, nodes are typically all connected to each other
            node_ids = []
            with self.nodes_lock:
                node_ids = list(self.nodes.keys())
            
            for i, source_id in enumerate(node_ids):
                for target_id in node_ids[i+1:]:
                    # Create link
                    link = NetworkLink(
                        source_node=source_id,
                        target_node=target_id,
                        link_type="kubernetes",
                        status="up",
                        last_updated=time.time()
                    )
                    
                    # Update or add link
                    self.update_link(link)
            
            logger.info(f"Kubernetes discovery completed: {len(node_ids)} nodes, {len(services)} services")
        
        except Exception as e:
            logger.error(f"Error in Kubernetes discovery: {e}")
    
    def _discover_by_consul(self):
        """Discover topology from Consul API"""
        logger.info("Starting Consul discovery")
        
        try:
            # Check if the consul module is available
            try:
                import consul
            except ImportError:
                logger.error("Consul module not installed. Use 'pip install python-consul'")
                return
            
            # Get Consul connection parameters from configuration
            consul_host = self.config.get("consul_host", "localhost")
            consul_port = self.config.get("consul_port", 8500)
            consul_token = self.config.get("consul_token")
            consul_scheme = self.config.get("consul_scheme", "http")
            
            # Create Consul client
            consul_client = consul.Consul(
                host=consul_host,
                port=consul_port,
                token=consul_token,
                scheme=consul_scheme
            )
            
            # Get nodes
            index, nodes = consul_client.catalog.nodes()
            
            for node in nodes:
                node_name = node["Node"]
                ip_address = node["Address"]
                
                # Create node ID
                node_id = self._generate_node_id(node_name, ip_address)
                
                # Get node health
                index, health = consul_client.health.node(node_name)
                
                # Determine node status
                status = "unknown"
                if health:
                    status_counts = {"passing": 0, "warning": 0, "critical": 0}
                    for check in health:
                        status_counts[check["Status"]] = status_counts.get(check["Status"], 0) + 1
                    
                    if status_counts.get("critical", 0) > 0:
                        status = "critical"
                    elif status_counts.get("warning", 0) > 0:
                        status = "degraded"
                    elif status_counts.get("passing", 0) > 0:
                        status = "healthy"
                
                # Get node metadata
                index, node_info = consul_client.catalog.node(node_name)
                datacenter = node_info.get("Datacenter")
                
                # Create node information
                node_info = NodeInfo(
                    node_id=node_id,
                    hostname=node_name,
                    ip_address=ip_address,
                    status=status,
                    datacenter=datacenter,
                    last_seen=time.time(),
                    last_updated=time.time()
                )
                
                # Update or add node
                self.update_node(node_info)
                logger.info(f"Discovered Consul node: {node_id}")
            
            # Get services
            index, services = consul_client.catalog.services()
            
            for service_name, tags in services.items():
                # Get instances of this service
                index, service_instances = consul_client.catalog.service(service_name)
                
                if not service_instances:
                    continue
                
                # Create service ID
                service_id = f"consul/{service_name}"
                
                # Collect nodes running this service
                node_ids = []
                endpoints = []
                
                for instance in service_instances:
                    node_name = instance["Node"]
                    
                    # Find corresponding node ID
                    with self.nodes_lock:
                        for node_id, node in self.nodes.items():
                            if node.hostname == node_name:
                                node_ids.append(node_id)
                                break
                    
                    # Create endpoint
                    endpoint = {
                        "host": instance["ServiceAddress"] or instance["Address"],
                        "port": instance["ServicePort"],
                        "tags": instance["ServiceTags"]
                    }
                    endpoints.append(endpoint)
                
                # Create service information
                service_info = ServiceInfo(
                    service_id=service_id,
                    service_name=service_name,
                    service_type="consul",
                    nodes=node_ids,
                    endpoints=endpoints,
                    metadata={"tags": tags},
                    last_updated=time.time()
                )
                
                # Update or add service
                self.update_service(service_info)
                logger.info(f"Discovered Consul service: {service_id}")
            
            # Discover dependencies through service health checks
            # This is more complex and would require analyzing service health checks
            
            logger.info(f"Consul discovery completed: {len(nodes)} nodes, {len(services)} services")
        
        except Exception as e:
            logger.error(f"Error in Consul discovery: {e}")
    
    def _discover_by_zookeeper(self):
        """Discover topology from ZooKeeper"""
        logger.info("Starting ZooKeeper discovery")
        
        try:
            # Check if the kazoo module is available
            try:
                from kazoo.client import KazooClient
            except ImportError:
                logger.error("Kazoo module not installed. Use 'pip install kazoo'")
                return
            
            # Get ZooKeeper connection parameters from configuration
            zk_hosts = self.config.get("zookeeper_hosts", "localhost:2181")
            
            # Create ZooKeeper client
            zk = KazooClient(hosts=zk_hosts)
            zk.start()
            
            try:
                # ZooKeeper doesn't have a built-in service registry like Consul
                # This is just a basic implementation that assumes services register under /services
                
                # Get services
                if zk.exists("/services"):
                    services = zk.get_children("/services")
                    
                    for service_name in services:
                        # Get instances of this service
                        service_path = f"/services/{service_name}"
                        if zk.exists(service_path):
                            instances = zk.get_children(service_path)
                            
                            if not instances:
                                continue
                            
                            # Create service ID
                            service_id = f"zookeeper/{service_name}"
                            
                            # Collect endpoints
                            endpoints = []
                            metadata = {}
                            
                            for instance_id in instances:
                                instance_path = f"{service_path}/{instance_id}"
                                if zk.exists(instance_path):
                                    data, _ = zk.get(instance_path)
                                    
                                    try:
                                        instance_data = json.loads(data.decode('utf-8'))
                                        
                                        # Extract endpoint information
                                        if "host" in instance_data and "port" in instance_data:
                                            endpoint = {
                                                "host": instance_data["host"],
                                                "port": instance_data["port"]
                                            }
                                            endpoints.append(endpoint)
                                        
                                        # Extract metadata
                                        if "metadata" in instance_data:
                                            metadata.update(instance_data["metadata"])
                                    
                                    except (json.JSONDecodeError, UnicodeDecodeError):
                                        logger.warning(f"Invalid data format for instance {instance_id}")
                            
                            # Create service information
                            service_info = ServiceInfo(
                                service_id=service_id,
                                service_name=service_name,
                                service_type="zookeeper",
                                endpoints=endpoints,
                                metadata=metadata,
                                last_updated=time.time()
                            )
                            
                            # Update or add service
                            self.update_service(service_info)
                            logger.info(f"Discovered ZooKeeper service: {service_id}")
                
                logger.info(f"ZooKeeper discovery completed")
            
            finally:
                zk.stop()
                zk.close()
        
        except Exception as e:
            logger.error(f"Error in ZooKeeper discovery: {e}")
    
    def _discover_network_links(self, node_ids: List[str]):
        """Discover network links between nodes"""
        logger.info(f"Discovering network links for {len(node_ids)} nodes")
        
        # Get node information
        nodes = {}
        with self.nodes_lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    nodes[node_id] = self.nodes[node_id]
        
        # Check connectivity between nodes
        for i, (source_id, source_node) in enumerate(nodes.items()):
            for target_id, target_node in list(nodes.items())[i+1:]:
                try:
                    # Check connectivity
                    if self._check_connectivity(source_node.ip_address, target_node.ip_address):
                        # Measure network metrics
                        metrics = self._measure_network_metrics(source_node.ip_address, target_node.ip_address)
                        
                        # Create link
                        link = NetworkLink(
                            source_node=source_id,
                            target_node=target_id,
                            link_type="ethernet",
                            status="up",
                            last_updated=time.time()
                        )
                        
                        if metrics:
                            link.bandwidth = metrics.get("bandwidth")
                            link.latency = metrics.get("latency")
                            link.packet_loss = metrics.get("packet_loss")
                        
                        # Update or add link
                        self.update_link(link)
                        logger.info(f"Discovered network link: {source_id} - {target_id}")
                
                except Exception as e:
                    logger.error(f"Error discovering link {source_id} - {target_id}: {e}")
    
    def _discover_services(self, node_ids: List[str]):
        """Discover services running on nodes"""
        logger.info(f"Discovering services for {len(node_ids)} nodes")
        
        # Get node information
        nodes = {}
        with self.nodes_lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    nodes[node_id] = self.nodes[node_id]
        
        # Common service ports to check
        service_ports = {
            "http": 80,
            "https": 443,
            "ssh": 22,
            "mysql": 3306,
            "postgresql": 5432,
            "redis": 6379,
            "mongodb": 27017,
            "elasticsearch": 9200,
            "kafka": 9092,
            "zookeeper": 2181
        }
        
        # Check for services on each node
        for node_id, node in nodes.items():
            services_found = []
            
            for service_name, port in service_ports.items():
                try:
                    if self._check_port(node.ip_address, port):
                        # Create service ID
                        service_id = f"{service_name}-{node_id}"
                        
                        # Check if service exists
                        with self.services_lock:
                            if service_id in self.services:
                                # Update existing service
                                service = self.services[service_id]
                                if node_id not in service.nodes:
                                    service.nodes.append(node_id)
                                service.last_updated = time.time()
                            else:
                                # Create new service
                                service_info = ServiceInfo(
                                    service_id=service_id,
                                    service_name=service_name,
                                    service_type="detected",
                                    nodes=[node_id],
                                    endpoints=[{
                                        "host": node.ip_address,
                                        "port": port,
                                        "protocol": "tcp"
                                    }],
                                    status="unknown",
                                    last_updated=time.time()
                                )
                                
                                self.update_service(service_info)
                        
                        services_found.append(service_name)
                        logger.info(f"Discovered service: {service_name} on {node_id}")
                
                except Exception as e:
                    logger.error(f"Error checking service {service_name} on {node_id}: {e}")
            
            # Update node's service list
            if services_found:
                with self.nodes_lock:
                    if node_id in self.nodes:
                        self.nodes[node_id].services = services_found
            
            # Try to detect services using zeroconf/mDNS if available
            self._discover_mdns_services(node_id, node)
    
    def _discover_mdns_services(self, node_id: str, node: NodeInfo):
        """Discover services using mDNS/Zeroconf"""
        try:
            # Check if the zeroconf module is available
            try:
                from zeroconf import ServiceBrowser, Zeroconf
            except ImportError:
                return  # Silently skip if not available
            
            class ServiceListener:
                def __init__(self, outer, node_id):
                    self.outer = outer
                    self.node_id = node_id
                    self.services_found = []
                
                def add_service(self, zc, type, name):
                    info = zc.get_service_info(type, name)
                    if info:
                        service_name = name.split('.')[0]
                        service_type = type.split('.')[0]
                        
                        # Extract IP and port
                        ip = ".".join(str(x) for x in info.addresses[0]) if info.addresses else None
                        port = info.port
                        
                        if ip and port:
                            # Create service ID
                            service_id = f"{service_type}-{self.node_id}"
                            
                            # Create service info
                            service_info = ServiceInfo(
                                service_id=service_id,
                                service_name=service_name,
                                service_type="mdns",
                                nodes=[self.node_id],
                                endpoints=[{
                                    "host": ip,
                                    "port": port,
                                    "protocol": "tcp"
                                }],
                                status="unknown",
                                metadata={"mdns_type": type},
                                last_updated=time.time()
                            )
                            
                            self.outer.update_service(service_info)
                            self.services_found.append(service_type)
                            logger.info(f"Discovered mDNS service: {service_name} on {self.node_id}")
            
            # Create zeroconf instance
            zc = Zeroconf()
            listener = ServiceListener(self, node_id)
            
            # Browse for common service types
            browser = ServiceBrowser(zc, "_http._tcp.local.", listener)
            browser = ServiceBrowser(zc, "_ssh._tcp.local.", listener)
            
            # Wait a short time for discovery
            time.sleep(3)
            
            # Clean up
            zc.close()
            
            # Update node's service list with mDNS services
            if listener.services_found:
                with self.nodes_lock:
                    if node_id in self.nodes:
                        self.nodes[node_id].services.extend(listener.services_found)
        
        except Exception as e:
            logger.debug(f"Error in mDNS discovery for {node_id}: {e}")
    
    def _update_node_statuses(self):
        """Update status of all nodes"""
        logger.info("Updating node statuses")
        
        with self.nodes_lock:
            for node_id, node in list(self.nodes.items()):
                try:
                    # Skip recently updated nodes
                    if time.time() - node.last_updated < 60:  # Don't update nodes updated in the last minute
                        continue
                    
                    # Check if node is reachable
                    reachable = self._ping_host(node.ip_address)
                    
                    # Update status
                    if reachable:
                        if node.status == "offline":
                            node.status = "unknown"  # Reset to unknown when it comes back online
                            logger.info(f"Node {node_id} is now reachable")
                        
                        node.last_seen = time.time()
                    else:
                        if node.status != "offline":
                            node.status = "offline"
                            logger.warning(f"Node {node_id} is unreachable")
                    
                    node.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for node {node_id}: {e}")
    
    def _update_service_statuses(self):
        """Update status of all services"""
        logger.info("Updating service statuses")
        
        with self.services_lock, self.nodes_lock:
            for service_id, service in list(self.services.items()):
                try:
                    # Skip recently updated services
                    if time.time() - service.last_updated < 60:  # Don't update services updated in the last minute
                        continue
                    
                    # Check node status for each service node
                    offline_nodes = 0
                    for node_id in service.nodes:
                        if node_id in self.nodes and self.nodes[node_id].status == "offline":
                            offline_nodes += 1
                    
                    # Update service status based on node status
                    if not service.nodes:
                        service.status = "unavailable"
                    elif offline_nodes == len(service.nodes):
                        service.status = "unavailable"
                    elif offline_nodes > 0:
                        service.status = "degraded"
                    else:
                        # Check if any endpoint is reachable
                        available = False
                        for endpoint in service.endpoints:
                            host = endpoint.get("host")
                            port = endpoint.get("port")
                            
                            if host and port and self._check_port(host, port):
                                available = True
                                break
                        
                        service.status = "available" if available else "degraded"
                    
                    service.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for service {service_id}: {e}")
    
    def _update_link_statuses(self):
        """Update status of all network links"""
        logger.info("Updating link statuses")
        
        with self.links_lock, self.nodes_lock:
            for link in list(self.links):
                try:
                    # Skip recently updated links
                    if time.time() - link.last_updated < 300:  # Don't update links updated in the last 5 minutes
                        continue
                    
                    # Get node information
                    source_node = self.nodes.get(link.source_node)
                    target_node = self.nodes.get(link.target_node)
                    
                    if not source_node or not target_node:
                        continue
                    
                    # Check connectivity
                    if source_node.status != "offline" and target_node.status != "offline":
                        connectivity = self._check_connectivity(source_node.ip_address, target_node.ip_address)
                        
                        # Update status
                        link.status = "up" if connectivity else "down"
                        
                        # Update metrics if needed
                        if connectivity and (link.bandwidth is None or time.time() - link.last_updated > 3600):
                            metrics = self._measure_network_metrics(source_node.ip_address, target_node.ip_address)
                            
                            if metrics:
                                link.bandwidth = metrics.get("bandwidth")
                                link.latency = metrics.get("latency")
                                link.packet_loss = metrics.get("packet_loss")
                    elif source_node.status == "offline" or target_node.status == "offline":
                        # Mark as down if either node is offline
                        link.status = "down"
                    
                    link.last#!/usr/bin/env python3
"""
Cluster Topology Manager for ClusterSentry

This module discovers and maintains the topology of the cluster, providing
information about network structure, node relationships, and service dependencies
for intelligent failure impact assessment and recovery planning.
"""

import os
import sys
import time
import json
import logging
import threading
import ipaddress
import socket
import subprocess
import redis
import networkx as nx
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Union, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("topology_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TopologyManager")

@dataclass
class NodeInfo:
    """Information about a node in the cluster topology"""
    node_id: str
    hostname: str
    ip_address: str
    status: str = "unknown"  # unknown, healthy, degraded, critical, offline
    labels: Dict[str, str] = field(default_factory=dict)
    rack: Optional[str] = None
    datacenter: Optional[str] = None
    region: Optional[str] = None
    node_type: str = "worker"  # master, worker, storage, etc.
    services: List[str] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    neighbors: List[str] = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)

@dataclass
class ServiceInfo:
    """Information about a service in the cluster topology"""
    service_id: str
    service_name: str
    service_type: str
    nodes: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    endpoints: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "unknown"  # unknown, available, degraded, unavailable
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

@dataclass
class NetworkLink:
    """Information about a network link between nodes"""
    source_node: str
    target_node: str
    link_type: str = "ethernet"  # ethernet, infiniband, fiber, etc.
    bandwidth: Optional[float] = None  # Mbps
    latency: Optional[float] = None  # ms
    packet_loss: Optional[float] = None  # percentage
    status: str = "unknown"  # unknown, up, down, degraded
    last_updated: float = field(default_factory=time.time)

class TopologyDiscoveryMethod(Enum):
    """Methods for topology discovery"""
    NETWORK_SCAN = "network_scan"
    KUBERNETES = "kubernetes"
    CONSUL = "consul"
    ZOOKEEPER = "zookeeper"
    MANUAL = "manual"
    IMPORT = "import"

class ClusterTopologyManager:
    """
    Manages the topology of the cluster, providing information about
    network structure, node relationships, and service dependencies.
    
    Features:
    - Automated topology discovery
    - Topology visualization
    - Critical path analysis
    - Failure impact prediction
    - Historical topology tracking
    """
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the topology manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        
        # Set default configuration values
        self.discovery_interval = self.config.get("discovery_interval", 3600)
        self.update_interval = self.config.get("update_interval", 300)
        self.max_history = self.config.get("max_history", 100)
        self.discovery_method = TopologyDiscoveryMethod(self.config.get("discovery_method", "network_scan"))
        
        # Initialize Redis client if configured
        self.redis_client = None
        if self.config.get("redis_enabled", True):
            self.redis_client = self._init_redis_client()
        
        # Directories for persistent storage
        self.data_dir = Path(self.config.get("data_directory", "data/topology"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.history_dir = self.data_dir / "history"
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize topology data structures
        self.nodes = {}  # Dict[node_id, NodeInfo]
        self.services = {}  # Dict[service_id, ServiceInfo]
        self.links = []  # List[NetworkLink]
        
        # Network graph for topology operations
        self.graph = nx.Graph()
        
        # Locks for thread safety
        self.nodes_lock = threading.RLock()
        self.services_lock = threading.RLock()
        self.links_lock = threading.RLock()
        self.graph_lock = threading.RLock()
        
        # Topology history
        self.topology_history = []  # List of timestamped topology snapshots
        
        # Event listeners
        self.listeners = {}  # Dict[event_type, List[callback]]
        self.listeners_lock = threading.RLock()
        
        # Threads
        self.running = False
        self.discovery_thread = None
        self.update_thread = None
        self.pubsub_thread = None
        self.pubsub = None
        
        logger.info("Cluster Topology Manager initialized")
    
    def _load_config(self, config_path: Optional[Union[str, Path]]) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            "discovery_interval": 3600,  # 1 hour
            "update_interval": 300,      # 5 minutes
            "max_history": 100,
            "redis_enabled": True,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_password": None,
            "redis_namespace": "clustersentry",
            "discovery_method": "network_scan",
            "discovery_subnets": ["192.168.1.0/24"],
            "discovery_exclude_ips": ["192.168.1.1"],
            "node_stale_threshold": 86400,  # 24 hours
            "data_directory": "data/topology",
            "log_level": "INFO"
        }
        
        if not config_path:
            logger.warning("No configuration path provided, using defaults")
            return default_config
        
        try:
            path = Path(config_path)
            if not path.exists():
                logger.warning(f"Configuration file not found: {path}, using defaults")
                return default_config
            
            with open(path, 'r') as f:
                if path.suffix.lower() == '.yaml' or path.suffix.lower() == '.yml':
                    import yaml
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            
            # Merge with defaults for backward compatibility
            merged_config = default_config.copy()
            merged_config.update(config)
            
            logger.info(f"Loaded configuration from {path}")
            return merged_config
        
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return default_config
    
    def _init_redis_client(self) -> Optional[redis.Redis]:
        """Initialize Redis client"""
        try:
            client = redis.Redis(
                host=self.config.get("redis_host", "localhost"),
                port=self.config.get("redis_port", 6379),
                password=self.config.get("redis_password"),
                db=self.config.get("redis_db", 0),
                decode_responses=True
            )
            
            # Test connection
            client.ping()
            logger.info("Connected to Redis")
            return client
        
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return None
    
    def start(self):
        """Start topology management threads"""
        if self.running:
            logger.warning("Topology manager is already running")
            return
        
        self.running = True
        
        # Start discovery thread
        self.discovery_thread = threading.Thread(
            target=self._discovery_loop,
            daemon=True
        )
        self.discovery_thread.start()
        
        # Start update thread
        self.update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )
        self.update_thread.start()
        
        # Start Redis pubsub thread if Redis is available
        if self.redis_client:
            self._setup_redis_subscription()
        
        # Load existing topology data if available
        self._load_topology_data()
        
        logger.info("Topology manager started")
    
    def stop(self):
        """Stop topology management threads"""
        if not self.running:
            logger.warning("Topology manager is not running")
            return
        
        self.running = False
        
        # Save current topology data
        self._save_topology_data()
        
        # Wait for threads to stop
        for thread in [self.discovery_thread, self.update_thread, self.pubsub_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5.0)
        
        # Clean up Redis subscription
        if self.pubsub:
            self.pubsub.unsubscribe()
        
        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
        
        logger.info("Topology manager stopped")
    
    def _setup_redis_subscription(self):
        """Set up Redis subscription for topology updates"""
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(f"{namespace}:topology_updates")
            
            # Start pubsub listener thread
            self.pubsub_thread = threading.Thread(
                target=self._pubsub_listener,
                daemon=True
            )
            self.pubsub_thread.start()
            
            logger.info("Redis subscription set up")
        except Exception as e:
            logger.error(f"Failed to set up Redis subscription: {e}")
    
    def _pubsub_listener(self):
        """Listen for topology updates from Redis"""
        logger.info("Redis pubsub listener started")
        
        try:
            while self.running:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        data = json.loads(message["data"])
                        update_type = data.get("type")
                        
                        if update_type == "node_update":
                            node_id = data.get("node_id")
                            node_data = data.get("node_info")
                            
                            if node_id and node_data:
                                # Update node info
                                node_info = NodeInfo(**node_data)
                                self.update_node(node_info)
                        
                        elif update_type == "service_update":
                            service_id = data.get("service_id")
                            service_data = data.get("service_info")
                            
                            if service_id and service_data:
                                # Update service info
                                service_info = ServiceInfo(**service_data)
                                self.update_service(service_info)
                        
                        elif update_type == "link_update":
                            link_data = data.get("link_info")
                            
                            if link_data:
                                # Update link info
                                link_info = NetworkLink(**link_data)
                                self.update_link(link_info)
                        
                        elif update_type == "topology_updated":
                            # Reload entire topology
                            if self.redis_client:
                                self._load_topology_from_redis()
                    
                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON in topology update")
                    except Exception as e:
                        logger.error(f"Error processing topology update: {e}")
                
                time.sleep(0.1)  # Prevent CPU spinning
        
        except Exception as e:
            logger.error(f"Error in Redis pubsub listener: {e}")
        
        logger.info("Redis pubsub listener stopped")
    
    def _load_topology_data(self):
        """Load topology data from persistent storage"""
        # Try to load from Redis first if available
        if self.redis_client and self._load_topology_from_redis():
            logger.info("Loaded topology data from Redis")
            return
        
        # Otherwise, load from files
        try:
            # Load nodes
            nodes_path = self.data_dir / "nodes.json"
            if nodes_path.exists():
                with open(nodes_path, 'r') as f:
                    nodes_data = json.load(f)
                
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_data in nodes_data.items():
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_path = self.data_dir / "services.json"
            if services_path.exists():
                with open(services_path, 'r') as f:
                    services_data = json.load(f)
                
                with self.services_lock:
                    self.services = {}
                    for service_id, service_data in services_data.items():
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_path = self.data_dir / "links.json"
            if links_path.exists():
                with open(links_path, 'r') as f:
                    links_data = json.load(f)
                
                with self.links_lock:
                    self.links = []
                    for link_data in links_data:
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            logger.info("Loaded topology data from files")
        
        except Exception as e:
            logger.error(f"Error loading topology data: {e}")
    
    def _load_topology_from_redis(self) -> bool:
        """Load topology data from Redis"""
        if not self.redis_client:
            return False
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Load nodes
            nodes_key = f"{namespace}:topology:nodes"
            nodes_data = self.redis_client.hgetall(nodes_key)
            
            if nodes_data:
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_json in nodes_data.items():
                        node_data = json.loads(node_json)
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_key = f"{namespace}:topology:services"
            services_data = self.redis_client.hgetall(services_key)
            
            if services_data:
                with self.services_lock:
                    self.services = {}
                    for service_id, service_json in services_data.items():
                        service_data = json.loads(service_json)
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_key = f"{namespace}:topology:links"
            links_data = self.redis_client.lrange(links_key, 0, -1)
            
            if links_data:
                with self.links_lock:
                    self.links = []
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            return True
        
        except Exception as e:
            logger.error(f"Error loading topology from Redis: {e}")
            return False
    
    def _save_topology_data(self):
        """Save topology data to persistent storage"""
        try:
            # Save to Redis if available
            if self.redis_client:
                self._save_topology_to_redis()
            
            # Save nodes
            with self.nodes_lock:
                nodes_data = {node_id: asdict(node) for node_id, node in self.nodes.items()}
                
                with open(self.data_dir / "nodes.json", 'w') as f:
                    json.dump(nodes_data, f, indent=2)
            
            # Save services
            with self.services_lock:
                services_data = {service_id: asdict(service) for service_id, service in self.services.items()}
                
                with open(self.data_dir / "services.json", 'w') as f:
                    json.dump(services_data, f, indent=2)
            
            # Save links
            with self.links_lock:
                links_data = [asdict(link) for link in self.links]
                
                with open(self.data_dir / "links.json", 'w') as f:
                    json.dump(links_data, f, indent=2)
            
            logger.info("Saved topology data to files")
        
        except Exception as e:
            logger.error(f"Error saving topology data: {e}")
    
    def _save_topology_to_redis(self):
        """Save topology data to Redis"""
        if not self.redis_client:
            return
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Save nodes
            with self.nodes_lock:
                nodes_key = f"{namespace}:topology:nodes"
                nodes_data = {}
                
                for node_id, node in self.nodes.items():
                    nodes_data[node_id] = json.dumps(asdict(node))
                
                if nodes_data:
                    self.redis_client.delete(nodes_key)
                    self.redis_client.hset(nodes_key, mapping=nodes_data)
            
            # Save services
            with self.services_lock:
                services_key = f"{namespace}:topology:services"
                services_data = {}
                
                for service_id, service in self.services.items():
                    services_data[service_id] = json.dumps(asdict(service))
                
                if services_data:
                    self.redis_client.delete(services_key)
                    self.redis_client.hset(services_key, mapping=services_data)
            
            # Save links
            with self.links_lock:
                links_key = f"{namespace}:topology:links"
                
                self.redis_client.delete(links_key)
                
                for link in self.links:
                    self.redis_client.rpush(links_key, json.dumps(asdict(link)))
            
            logger.info("Saved topology data to Redis")
        
        except Exception as e:
            logger.error(f"Error saving topology to Redis: {e}")
    
    def _rebuild_graph(self):
        """Rebuild the network graph from nodes and links"""
        with self.graph_lock:
            # Create new graph
            self.graph = nx.Graph()
            
            # Add nodes
            with self.nodes_lock:
                for node_id, node in self.nodes.items():
                    self.graph.add_node(node_id, **asdict(node))
            
            # Add links
            with self.links_lock:
                for link in self.links:
                    self.graph.add_edge(
                        link.source_node,
                        link.target_node,
                        **asdict(link)
                    )
    
    def _discovery_loop(self):
        """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)

                dot.append("  graph [rankdir=LR, overlap=false, splines=true];")
                dot.append("  node [shape=box, style=filled, fontname=Arial];")
                dot.append("  edge [fontname=Arial];")
                
                # Add nodes
                for node_id, node in self.nodes.items():
                    # Set color based on status
                    color = "#DDDDDD"  # Default gray
                    if node.status == "healthy":
                        color = "#90EE90"  # Light green
                    elif node.status == "degraded":
                        color = "#FFFF99"  # Light yellow
                    elif node.status == "critical":
                        color = "#FFA07A"  # Light salmon
                    elif node.status == "offline":
                        color = "#D3D3D3"  # Light gray
                    
                    # Create label with node information
                    label = f"{node.hostname}\\n({node.ip_address})"
                    
                    dot.append(f'  "{node_id}" [label="{label}", fillcolor="{color}"];')
                
                # Add services
                for service_id, service in self.services.items():
                    # Set color based on status
                    color = "#DDDDDD"  # Default gray
                    if service.status == "available":
                        color = "#ADD8E6"  # Light blue
                    elif service.status == "degraded":
                        color = "#FFFACD"  # Lemon chiffon
                    elif service.status == "unavailable":
                        color = "#FFB6C1"  # Light pink
                    
                    # Create label with service information
                    label = f"{service.service_name}\\n({service.service_type})"
                    
                    # Add service node
                    dot.append(f'  "service:{service_id}" [label="{label}", fillcolor="{color}", shape=ellipse];')
                    
                    # Add edges from service to nodes
                    for node_id in service.nodes:
                        dot.append(f'  "service:{service_id}" -> "{node_id}" [style=dashed];')
                    
                    # Add edges for dependencies
                    for dep_id in service.dependencies:
                        dot.append(f'  "service:{service_id}" -> "service:{dep_id}" [style=dotted, color=blue];')
                
                # Add links
                for link in self.links:
                    # Set color and style based on status
                    color = "black"
                    style = "solid"
                    
                    if link.status == "up":
                        color = "green"
                    elif link.status == "degraded":
                        color = "orange"
                        style = "dashed"
                    elif link.status == "down":
                        color = "red"
                        style = "dotted"
                    
                    # Add label with link information
                    label = ""
                    if link.bandwidth is not None:
                        label = f"{link.bandwidth} Mbps"
                    if link.latency is not None:
                        label += f"\\n{link.latency} ms"
                    
                    dot.append(f'  "{link.source_node}" -> "{link.target_node}" [dir=none, color="{color}", style="{style}", label="{label}"];')
                
                dot.append("}")
                
                return "\n".join(dot)
            
            except Exception as e:
                logger.error(f"Error generating Graphviz: {e}")
                return f"digraph {{ node [label=\"Error: {str(e)}\"] }}"
    
    def _ping_host(self, ip_address: str) -> bool:
        """Check if a host is reachable via ping"""
        try:
            # Platform-specific ping command
            if sys.platform == "win32":
                cmd = ["ping", "-n", "1", "-w", "1000", ip_address]
            else:
                cmd = ["ping", "-c", "1", "-W", "1", ip_address]
            
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2)
            return result.returncode == 0
        
        except Exception:
            return False
    
    def _resolve_hostname(self, ip_address: str) -> Optional[str]:
        """Resolve hostname from IP address"""
        try:
            hostname, _, _ = socket.gethostbyaddr(ip_address)
            return hostname
        except Exception:
            return None
    
    def _check_connectivity(self, source_ip: str, target_ip: str) -> bool:
        """Check connectivity between two hosts"""
        # In a real implementation, this would use more sophisticated methods
        # For now, we just ping the target from the current host
        return self._ping_host(target_ip)
    
    def _check_port(self, ip_address: str, port: int) -> bool:
        """Check if a TCP port is open"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((ip_address, port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def _measure_network_metrics(self, source_ip: str, target_ip: str) -> Optional[Dict[str, float]]:
        """Measure network metrics between two hosts"""
        try:
            # In a real implementation, this would use tools like iperf, ping, etc.
            # For demonstration, we'll just ping to get latency
            
            if sys.platform == "win32":
                cmd = ["ping", "-n", "5", "-w", "1000", target_ip]
            else:
                cmd = ["ping", "-c", "5", "-W", "1", target_ip]
            
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
            
            if result.returncode == 0:
                # Parse output to extract metrics
                output = result.stdout
                
                # Extract latency
                latency = None
                
                if sys.platform == "win32":
                    # Windows format
                    for line in output.splitlines():
                        if "Average" in line:
                            try:
                                avg_part = line.split("=")[1].strip()
                                latency = float(avg_part.split("ms")[0].strip())
                            except (IndexError, ValueError):
                                pass
                else:
                    # Unix format
                    for line in output.splitlines():
                        if "min/avg/max" in line:
                            try:
                                stats = line.split("=")[1].strip().split("/")
                                latency = float(stats[1])  # avg
                            except (IndexError, ValueError):
                                pass
                
                # Extract packet loss
                packet_loss = None
                
                if sys.platform == "win32":
                    # Windows format
                    for line in output.splitlines():
                        if "Lost" in line:
                            try:
                                loss_part = line.split("(")[1].split("%")[0]
                                packet_loss = float(loss_part)
                            except (IndexError, ValueError):
                                pass
                else:
                    # Unix format
                    for line in output.splitlines():
                        if "packet loss" in line:
                            try:
                                loss_part = line.split(",")[2].strip().split("%")[0]
                                packet_loss = float(loss_part)
                            except (IndexError, ValueError):
                                pass
                
                # For bandwidth, we would need iperf or similar
                # For now, we'll just return latency and packet loss
                
                return {
                    "latency": latency,
                    "packet_loss": packet_loss,
                    "bandwidth": None
                }
        
        except Exception as e:
            logger.debug(f"Error measuring network metrics: {e}")
        
        return None
    
    def _generate_node_id(self, hostname: str, ip_address: str) -> str:
        """Generate a node ID from hostname and IP address"""
        # Use hostname if available, otherwise use IP
        if hostname and not hostname.startswith("localhost"):
            return f"node-{hostname.split('.')[0]}"
        else:
            return f"node-{ip_address.replace('.', '-')}"

    @staticmethod
    def create_default_config(output_path: Union[str, Path]):
        """
        Create a default configuration file
        
        Args:
            output_path: Path to write the configuration file
        """
        default_config = {
            "discovery_interval": 3600,  # 1 hour
            "update_interval": 300,      # 5 minutes
            "max_history": 100,
            "redis_enabled": True,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_password": None,
            "redis_namespace": "clustersentry",
            "discovery_method": "network_scan",
            "discovery_subnets": ["192.168.1.0/24"],
            "discovery_exclude_ips": ["192.168.1.1"],
            "node_stale_threshold": 86400,  # 24 hours
            "data_directory": "data/topology",
            "log_level": "INFO"
        }
        
        # Ensure parent directory exists
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write configuration file
        with open(output_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"Created default topology manager configuration at {output_path}")


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ClusterSentry Topology Manager")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--create-config", help="Create default configuration file")
    parser.add_argument("--discover", action="store_true", help="Run immediate discovery")
    parser.add_argument("--list-nodes", action="store_true", help="List all nodes")
    parser.add_argument("--list-services", action="store_true", help="List all services")
    parser.add_argument("--node-info", help="Get information about a node")
    parser.add_argument("--service-info", help="Get information about a service")
    parser.add_argument("--node-services", help="Get services running on a node")
    parser.add_argument("--critical-nodes", action="store_true", help="Find critical nodes")
    parser.add_argument("--failure-impact", help="Predict the impact of a node failure")
    parser.add_argument("--export", help="Export topology to a file")
    parser.add_argument("--import-file", help="Import topology from a file")
    parser.add_argument("--graphviz", help="Generate Graphviz visualization to a file")
    parser.add_argument("--stats", action="store_true", help="Show network statistics")
    args = parser.parse_args()
    
    # Create default configuration file if requested
    if args.create_config:
        ClusterTopologyManager.create_default_config(args.create_config)
        sys.exit(0)
    
    # Initialize topology manager
    manager = ClusterTopologyManager(config_path=args.config)
    
    try:
        manager.start()
        
        # Export topology
        if args.export:
            topology = manager.export_topology()
            with open(args.export, 'w') as f:
                json.dump(topology, f, indent=2)
            print(f"Exported topology to {args.export}")
        
        # Import topology
        elif args.import_file:
            with open(args.import_file, 'r') as f:
                topology = json.load(f)
            
            if manager.import_topology(topology):
                print(f"Imported topology from {args.import_file}")
            else:
                print(f"Failed to import topology from {args.import_file}")
        
        # Run immediate discovery
        elif args.discover:
            print("Running topology discovery...")
            
            if manager.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                manager._discover_by_network_scan()
            elif manager.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                manager._discover_by_kubernetes()
            elif manager.discovery_method == TopologyDiscoveryMethod.CONSUL:
                manager._discover_by_consul()
            elif manager.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                manager._discover_by_zookeeper()
            
            print(f"Discovery completed: {len(manager.get_nodes())} nodes, {len(manager.get_services())} services")
        
        # List nodes
        elif args.list_nodes:
            nodes = manager.get_nodes()
            print(f"Nodes ({len(nodes)}):")
            for node_id, node in nodes.items():
                print(f"  {node_id} ({node.hostname}, {node.ip_address}): {node.status}")
        
        # List services
        elif args.list_services:
            services = manager.get_services()
            print(f"Services ({len(services)}):")
            for service_id, service in services.items():
                nodes_str = ", ".join(service.nodes)
                print(f"  {service_id} ({service.service_name}): {service.status} on [{nodes_str}]")
        
        # Get node information
        elif args.node_info:
            node = manager.get_node(args.node_info)
            if node:
                print(f"Node: {node.node_id}")
                print(f"  Hostname: {node.hostname}")
                print(f"  IP: {node.ip_address}")
                print(f"  Status: {node.status}")
                print(f"  Type: {node.node_type}")
                if node.rack:
                    print(f"  Rack: {node.rack}")
                if node.datacenter:
                    print(f"  Datacenter: {node.datacenter}")
                if node.region:
                    print(f"  Region: {node.region}")
                print(f"  Services: {', '.join(node.services)}")
                print(f"  Last seen: {time.ctime(node.last_seen)}")
                
                neighbors = manager.get_node_neighbors(args.node_info)
                neighbor_ids = [n.node_id for n in neighbors]
                print(f"  Neighbors: {', '.join(neighbor_ids)}")
            else:
                print(f"Node {args.node_info} not found")
        
        # Get service information
        elif args.service_info:
            service = manager.get_service(args.service_info)
            if service:
                print(f"Service: {service.service_id}")
                print(f"  Name: {service.service_name}")
                print(f"  Type: {service.service_type}")
                print(f"  Status: {service.status}")
                print(f"  Nodes: {', '.join(service.nodes)}")
                
                if service.endpoints:
                    print("  Endpoints:")
                    for endpoint in service.endpoints:
                        host = endpoint.get("host", "unknown")
                        port = endpoint.get("port", "unknown")
                        print(f"    {host}:{port}")
                
                if service.dependencies:
                    print(f"  Dependencies: {', '.join(service.dependencies)}")
                
                dependents = manager.find_dependent_services(args.service_info)
                if dependents:
                    print(f"  Dependents: {', '.join(dependents)}")
            else:
                print(f"Service {args.service_info} not found")
        
        # Get services running on a node
        elif args.node_services:
            services = manager.get_node_services(args.node_services)
            if services:
                print(f"Services on node {args.node_services} ({len(services)}):")
                for service in services:
                    print(f"  {service.service_id} ({service.service_name}): {service.status}")
            else:
                print(f"No services found on node {args.node_services}")
        
        # Find critical nodes
        elif args.critical_nodes:
            critical_nodes = manager.find_critical_nodes()
            print(f"Critical nodes ({len(critical_nodes)}):")
            for node_id in critical_nodes:
                node = manager.get_node(node_id)
                hostname = node.hostname if node else "unknown"
                print(f"  {node_id} ({hostname})")
        
        # Predict failure impact
        elif args.failure_impact:
            impact = manager.predict_failure_impact(args.failure_impact)
            if "error" in impact:
                print(f"Error: {impact['error']}")
            else:
                print(f"Impact of failure of node {args.failure_impact}:")
                print(f"  Network partition: {impact['network_partition']}")
                print(f"  Affected nodes ({len(impact['affected_nodes'])}):")
                for node_id in impact['affected_nodes']:
                    node = manager.get_node(node_id)
                    hostname = node.hostname if node else "unknown"
                    print(f"    {node_id} ({hostname})")
                
                print(f"  Affected services ({len(impact['affected_services'])}):")
                for service_id in impact['affected_services']:
                    service = manager.get_service(service_id)
                    service_name = service.service_name if service else "unknown"
                    print(f"    {service_id} ({service_name})")
                
                print(f"  Severity: {impact['severity']}")
        
        # Generate Graphviz visualization
        elif args.graphviz:
            dot = manager.generate_graphviz()
            with open(args.graphviz, 'w') as f:
                f.write(dot)
            print(f"Generated Graphviz visualization to {args.graphviz}")
        
        # Show network statistics
        elif args.stats:
            stats = manager.compute_network_statistics()
            print("Network statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        
        # No operation specified, keep running
        else:
            print("Topology manager running. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
    
    except KeyboardInterrupt:
        print("Stopping topology manager...")
    
    finally:
        manager.stop()
                    link.status = "down"
                    
                    link.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for link {link.source_node} - {link.target_node}: {e}")
    
    def _cleanup_stale_nodes(self):
        """Remove nodes that haven't been seen for a long time"""
        logger.info("Cleaning up stale nodes")
        
        stale_threshold = self.config.get("node_stale_threshold", 86400)  # Default: 24 hours
        
        with self.nodes_lock:
            current_time = time.time()
            stale_nodes = []
            
            for node_id, node in self.nodes.items():
                if current_time - node.last_seen > stale_threshold:
                    stale_nodes.append(node_id)
            
            for node_id in stale_nodes:
                logger.info(f"Removing stale node: {node_id}")
                self.remove_node(node_id)
    
    def _create_topology_snapshot(self):
        """Create a snapshot of the current topology state"""
        logger.info("Creating topology snapshot")
        
        timestamp = time.time()
        
        # Create snapshot
        snapshot = {
            "timestamp": timestamp,
            "node_count": len(self.nodes),
            "service_count": len(self.services),
            "link_count": len(self.links),
            "topology": self.export_topology()
        }
        
        # Add to history
        self.topology_history.append(snapshot)
        
        # Limit history size
        if len(self.topology_history) > self.max_history:
            self.topology_history = self.topology_history[-self.max_history:]
        
        # Save snapshot to file
        try:
            snapshot_file = self.history_dir / f"{int(timestamp)}.json"
            with open(snapshot_file, 'w') as f:
                json.dump(snapshot, f, indent=2)
            logger.info(f"Saved topology snapshot to {snapshot_file}")
        except Exception as e:
            logger.error(f"Error saving topology snapshot: {e}")
        
        # Save snapshot to Redis if available
        if self.redis_client:
            try:
                namespace = self.config.get("redis_namespace", "clustersentry")
                snapshot_key = f"{namespace}:topology_snapshots"
                
                self.redis_client.lpush(snapshot_key, json.dumps(snapshot))
                self.redis_client.ltrim(snapshot_key, 0, self.max_history - 1)
                logger.info("Saved topology snapshot to Redis")
            except Exception as e:
                logger.error(f"Error saving topology snapshot to Redis: {e}")
    
    def _notify_listeners(self, event_type: str, data: Dict[str, Any]):
        """Notify event listeners"""
        with self.listeners_lock:
            if event_type in self.listeners:
                for callback in self.listeners[event_type]:
                    try:
                        callback(event_type, data)
                    except Exception as e:
                        logger.error(f"Error in event listener: {e}")
    
    def add_listener(self, event_type: str, callback: callable) -> bool:
        """
        Add an event listener
        
        Args:
            event_type: Type of event to listen for
            callback: Function to call when event occurs
            
        Returns:
            True if successful, False otherwise
        """
        with self.listeners_lock:
            if event_type not in self.listeners:
                self.listeners[event_type] = []
            
            if callback not in self.listeners[event_type]:
                self.listeners[event_type].append(callback)
                logger.debug(f"Added listener for {event_type}")
                return True
            
            return False
    
    def remove_listener(self, event_type: str, callback: callable) -> bool:
        """
        Remove an event listener
        
        Args:
            event_type: Type of event
            callback: Function to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.listeners_lock:
            if event_type in self.listeners and callback in self.listeners[event_type]:
                self.listeners[event_type].remove(callback)
                logger.debug(f"Removed listener for {event_type}")
                return True
            
            return False
    
    def update_node(self, node_info: NodeInfo) -> bool:
        """
        Update or add a node
        
        Args:
            node_info: Node information
            
        Returns:
            True if successful, False otherwise
        """
        with self.nodes_lock:
            node_id = node_info.node_id
            
            # Check if node exists
            if node_id in self.nodes:
                # Update existing node
                existing_node = self.nodes[node_id]
                
                # Update fields while preserving some existing values
                node_info.services = existing_node.services
                node_info.neighbors = existing_node.neighbors
                
                # Keep the current status if it's more specific
                if existing_node.status not in ["unknown", "offline"] and node_info.status == "unknown":
                    node_info.status = existing_node.status
                
                # Keep existing location info if not provided
                if not node_info.rack and existing_node.rack:
                    node_info.rack = existing_node.rack
                if not node_info.datacenter and existing_node.datacenter:
                    node_info.datacenter = existing_node.datacenter
                if not node_info.region and existing_node.region:
                    node_info.region = existing_node.region
                
                # Keep last seen time if it's more recent
                node_info.last_seen = max(existing_node.last_seen, node_info.last_seen)
            
            # Update node
            self.nodes[node_id] = node_info
            
            # Update graph
            with self.graph_lock:
                if not self.graph.has_node(node_id):
                    self.graph.add_node(node_id, **asdict(node_info))
                else:
                    # Update node attributes
                    nx.set_node_attributes(self.graph, {node_id: asdict(node_info)})
            
            # Notify listeners
            self._notify_listeners("node_updated", {"node_id": node_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    nodes_key = f"{namespace}:topology:nodes"
                    
                    self.redis_client.hset(nodes_key, node_id, json.dumps(asdict(node_info)))
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "node_update",
                            "node_id": node_id,
                            "node_info": asdict(node_info)
                        })
                    )
                except Exception as e:
                    logger.error(f"Error updating node in Redis: {e}")
            
            return True
    
    def remove_node(self, node_id: str) -> bool:
        """
        Remove a node
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.nodes_lock:
            if node_id not in self.nodes:
                return False
            
            # Remove from nodes dictionary
            del self.nodes[node_id]
            
            # Remove from graph
            with self.graph_lock:
                if self.graph.has_node(node_id):
                    self.graph.remove_node(node_id)
            
            # Update services
            with self.services_lock:
                for service_id, service in self.services.items():
                    if node_id in service.nodes:
                        service.nodes.remove(node_id)
            
            # Remove links
            with self.links_lock:
                self.links = [link for link in self.links 
                            if link.source_node != node_id and link.target_node != node_id]
            
            # Notify listeners
            self._notify_listeners("node_removed", {"node_id": node_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    nodes_key = f"{namespace}:topology:nodes"
                    
                    self.redis_client.hdel(nodes_key, node_id)
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "node_removed",
                            "node_id": node_id
                        })
                    )
                except Exception as e:
                    logger.error(f"Error removing node from Redis: {e}")
            
            return True
    
    def update_service(self, service_info: ServiceInfo) -> bool:
        """
        Update or add a service
        
        Args:
            service_info: Service information
            
        Returns:
            True if successful, False otherwise
        """
        with self.services_lock:
            service_id = service_info.service_id
            
            # Update service
            self.services[service_id] = service_info
            
            # Notify listeners
            self._notify_listeners("service_updated", {"service_id": service_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    services_key = f"{namespace}:topology:services"
                    
                    self.redis_client.hset(services_key, service_id, json.dumps(asdict(service_info)))
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "service_update",
                            "service_id": service_id,
                            "service_info": asdict(service_info)
                        })
                    )
                except Exception as e:
                    logger.error(f"Error updating service in Redis: {e}")
            
            return True
    
    def remove_service(self, service_id: str) -> bool:
        """
        Remove a service
        
        Args:
            service_id: ID of the service to remove
            
        Returns:
            True if successful, False otherwise
        """
        with self.services_lock:
            if service_id not in self.services:
                return False
            
            # Remove from services dictionary
            del self.services[service_id]
            
            # Update nodes
            with self.nodes_lock:
                for node_id, node in self.nodes.items():
                    if service_id in node.services:
                        node.services.remove(service_id)
            
            # Notify listeners
            self._notify_listeners("service_removed", {"service_id": service_id})
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    services_key = f"{namespace}:topology:services"
                    
                    self.redis_client.hdel(services_key, service_id)
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "service_removed",
                            "service_id": service_id
                        })
                    )
                except Exception as e:
                    logger.error(f"Error removing service from Redis: {e}")
            
            return True
    
    def update_link(self, link_info: NetworkLink) -> bool:
        """
        Update or add a network link
        
        Args:
            link_info: Link information
            
        Returns:
            True if successful, False otherwise
        """
        with self.links_lock:
            # Check if link exists
            found = False
            for i, link in enumerate(self.links):
                if (link.source_node == link_info.source_node and link.target_node == link_info.target_node) or \
                   (link.source_node == link_info.target_node and link.target_node == link_info.source_node):
                    # Update existing link
                    self.links[i] = link_info
                    found = True
                    break
            
            if not found:
                # Add new link
                self.links.append(link_info)
            
            # Update graph
            with self.graph_lock:
                if not self.graph.has_edge(link_info.source_node, link_info.target_node):
                    self.graph.add_edge(
                        link_info.source_node,
                        link_info.target_node,
                        **asdict(link_info)
                    )
                else:
                    # Update edge attributes
                    self.graph[link_info.source_node][link_info.target_node].update(asdict(link_info))
            
            # Notify listeners
            self._notify_listeners("link_updated", {
                "source_node": link_info.source_node,
                "target_node": link_info.target_node
            })
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    links_key = f"{namespace}:topology:links"
                    
                    # Get current links
                    links_data = self.redis_client.lrange(links_key, 0, -1)
                    links = []
                    
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        source = link_data.get("source_node")
                        target = link_data.get("target_node")
                        
                        if (source == link_info.source_node and target == link_info.target_node) or \
                           (source == link_info.target_node and target == link_info.source_node):
                            # Skip existing link
                            continue
                        
                        links.append(link_data)
                    
                    # Add updated link
                    links.append(asdict(link_info))
                    
                    # Replace links in Redis
                    pipeline = self.redis_client.pipeline()
                    pipeline.delete(links_key)
                    
                    for link in links:
                        pipeline.rpush(links_key, json.dumps(link))
                    
                    pipeline.execute()
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "link_update",
                            "link_info": asdict(link_info)
                        })
                    )
                except Exception as e:
                    logger.error(f"Error updating link in Redis: {e}")
            
            return True
    
    def remove_link(self, source_node: str, target_node: str) -> bool:
        """
        Remove a network link
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            
        Returns:
            True if successful, False otherwise
        """
        with self.links_lock:
            # Find link
            found = False
            for i, link in enumerate(self.links):
                if (link.source_node == source_node and link.target_node == target_node) or \
                   (link.source_node == target_node and link.target_node == source_node):
                    del self.links[i]
                    found = True
                    break
            
            if not found:
                return False
            
            # Update graph
            with self.graph_lock:
                if self.graph.has_edge(source_node, target_node):
                    self.graph.remove_edge(source_node, target_node)
            
            # Notify listeners
            self._notify_listeners("link_removed", {
                "source_node": source_node,
                "target_node": target_node
            })
            
            # Update Redis if available
            if self.redis_client:
                try:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    links_key = f"{namespace}:topology:links"
                    
                    # Get current links
                    links_data = self.redis_client.lrange(links_key, 0, -1)
                    links = []
                    
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        link_source = link_data.get("source_node")
                        link_target = link_data.get("target_node")
                        
                        if (link_source == source_node and link_target == target_node) or \
                           (link_source == target_node and link_target == source_node):
                            # Skip removed link
                            continue
                        
                        links.append(link_data)
                    
                    # Replace links in Redis
                    pipeline = self.redis_client.pipeline()
                    pipeline.delete(links_key)
                    
                    for link in links:
                        pipeline.rpush(links_key, json.dumps(link))
                    
                    pipeline.execute()
                    
                    # Publish update notification
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "link_removed",
                            "source_node": source_node,
                            "target_node": target_node
                        })
                    )
                except Exception as e:
                    logger.error(f"Error removing link from Redis: {e}")
            
            return True
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """
        Get node information
        
        Args:
            node_id: ID of the node
            
        Returns:
            NodeInfo if found, None otherwise
        """
        with self.nodes_lock:
            return self.nodes.get(node_id)
    
    def get_service(self, service_id: str) -> Optional[ServiceInfo]:
        """
        Get service information
        
        Args:
            service_id: ID of the service
            
        Returns:
            ServiceInfo if found, None otherwise
        """
        with self.services_lock:
            return self.services.get(service_id)
    
    def get_link(self, source_node: str, target_node: str) -> Optional[NetworkLink]:
        """
        Get link information
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            
        Returns:
            NetworkLink if found, None otherwise
        """
        with self.links_lock:
            for link in self.links:
                if (link.source_node == source_node and link.target_node == target_node) or \
                   (link.source_node == target_node and link.target_node == source_node):
                    return link
            
            return None
    
    def get_nodes(self) -> Dict[str, NodeInfo]:
        """
        Get all nodes
        
        Returns:
            Dictionary of node ID to NodeInfo
        """
        with self.nodes_lock:
            return self.nodes.copy()
    
    def get_services(self) -> Dict[str, ServiceInfo]:
        """
        Get all services
        
        Returns:
            Dictionary of service ID to ServiceInfo
        """
        with self.services_lock:
            return self.services.copy()
    
    def get_links(self) -> List[NetworkLink]:
        """
        Get all links
        
        Returns:
            List of NetworkLink
        """
        with self.links_lock:
            return self.links.copy()
    
    def get_node_services(self, node_id: str) -> List[ServiceInfo]:
        """
        Get services running on a node
        
        Args:
            node_id: ID of the node
            
        Returns:
            List of ServiceInfo
        """
        with self.services_lock:
            return [service for service in self.services.values() if node_id in service.nodes]
    
    def get_service_nodes(self, service_id: str) -> List[NodeInfo]:
        """
        Get nodes running a service
        
        Args:
            service_id: ID of the service
            
        Returns:
            List of NodeInfo
        """
        with self.nodes_lock, self.services_lock:
            service = self.services.get(service_id)
            if not service:
                return []
            
            return [self.nodes[node_id] for node_id in service.nodes if node_id in self.nodes]
    
    def get_node_neighbors(self, node_id: str) -> List[NodeInfo]:
        """
        Get neighbors of a node
        
        Args:
            node_id: ID of the node
            
        Returns:
            List of NodeInfo
        """
        with self.nodes_lock, self.graph_lock:
            if not self.graph.has_node(node_id):
                return []
            
            neighbor_ids = list(self.graph.neighbors(node_id))
            return [self.nodes[neighbor_id] for neighbor_id in neighbor_ids if neighbor_id in self.nodes]
    
    def find_path(self, source_node: str, target_node: str) -> List[str]:
        """
        Find shortest path between two nodes
        
        Args:
            source_node: Source node ID
            target_node: Target node ID
            
        Returns:
            List of node IDs in the path, empty if no path exists
        """
        with self.graph_lock:
            try:
                if not (self.graph.has_node(source_node) and self.graph.has_node(target_node)):
                    return []
                
                if not nx.has_path(self.graph, source_node, target_node):
                    return []
                
                return nx.shortest_path(self.graph, source_node, target_node)
            except nx.NetworkXNoPath:
                return []
            except Exception as e:
                logger.error(f"Error finding path: {e}")
                return []
    
    def find_nodes_by_label(self, key: str, value: str) -> List[NodeInfo]:
        """
        Find nodes with a specific label
        
        Args:
            key: Label key
            value: Label value
            
        Returns:
            List of matching NodeInfo
        """
        with self.nodes_lock:
            return [node for node in self.nodes.values() if node.labels.get(key) == value]
    
    def find_critical_nodes(self) -> List[str]:
        """
        Find critical nodes (articulation points) in the network
        
        Returns:
            List of node IDs
        """
        with self.graph_lock:
            try:
                return list(nx.articulation_points(self.graph))
            except Exception as e:
                logger.error(f"Error finding critical nodes: {e}")
                return []
    
    def find_service_dependencies(self, service_id: str) -> List[str]:
        """
        Find direct dependencies of a service
        
        Args:
            service_id: ID of the service
            
        Returns:
            List of dependent service IDs
        """
        with self.services_lock:
            service = self.services.get(service_id)
            if not service:
                return []
            
            return service.dependencies
    
    def find_dependent_services(self, service_id: str) -> List[str]:
        """
        Find services that depend on a given service
        
        Args:
            service_id: ID of the service
            
        Returns:
            List of service IDs that depend on this service
        """
        with self.services_lock:
            dependents = []
            for other_id, other_service in self.services.items():
                if service_id in other_service.dependencies:
                    dependents.append(other_id)
            
            return dependents
    
    def predict_failure_impact(self, node_id: str) -> Dict[str, Any]:
        """
        Predict the impact of a node failure
        
        Args:
            node_id: ID of the node to simulate failure for
            
        Returns:
            Impact assessment
        """
        with self.nodes_lock, self.services_lock, self.graph_lock:
            try:
                # Check if node exists
                if node_id not in self.nodes:
                    return {"error": f"Node {node_id} not found"}
                
                # Create a copy of the graph for simulation
                g = self.graph.copy()
                
                # Get pre-failure connected components
                pre_components = list(nx.connected_components(g))
                
                # Remove the node to simulate failure
                g.remove_node(node_id)
                
                # Get post-failure connected components
                post_components = list(nx.connected_components(g))
                
                # Find affected nodes (disconnected or isolated)
                affected_nodes = []
                
                if len(pre_components) != len(post_components):
                    # Network has been partitioned
                    # Find nodes that are now disconnected from the main component
                    
                    # Find the main component (largest)
                    main_component = max(post_components, key=len)
                    
                    # Find nodes not in the main component
                    for node in g.nodes():
                        if node not in main_component:
                            affected_nodes.append(node)
                
                # Find affected services
                affected_services = []
                
                # Services running on the failed node
                for service_id, service in self.services.items():
                    if node_id in service.nodes:
                        affected_services.append(service_id)
                
                # Services depending on affected services
                service_dependencies = {}
                for service_id in affected_services:
                    dependents = self.find_dependent_services(service_id)
                    for dependent in dependents:
                        if dependent not in affected_services:
                            affected_services.append(dependent)
                            service_dependencies[dependent] = service_id
                
                # Calculate impact severity
                severity = len(affected_nodes) + len(affected_services)
                
                return {
                    "node_id": node_id,
                    "network_partition": len(pre_components) != len(post_components),
                    "affected_nodes": affected_nodes,
                    "affected_services": affected_services,
                    "service_dependencies": service_dependencies,
                    "severity": severity
                }
            
            except Exception as e:
                logger.error(f"Error predicting failure impact: {e}")
                return {"error": str(e)}
    
    def export_topology(self) -> Dict[str, Any]:
        """
        Export the current topology
        
        Returns:
            Dictionary with topology data
        """
        with self.nodes_lock, self.services_lock, self.links_lock:
            return {
                "timestamp": time.time(),
                "nodes": {node_id: asdict(node) for node_id, node in self.nodes.items()},
                "services": {service_id: asdict(service) for service_id, service in self.services.items()},
                "links": [asdict(link) for link in self.links]
            }
    
    def import_topology(self, topology_data: Dict[str, Any]) -> bool:
        """
        Import topology data
        
        Args:
            topology_data: Topology data to import
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate data
            if not all(key in topology_data for key in ["nodes", "services", "links"]):
                logger.error("Invalid topology data: missing required keys")
                return False
            
            # Clear existing data
            with self.nodes_lock:
                self.nodes = {}
            
            with self.services_lock:
                self.services = {}
            
            with self.links_lock:
                self.links = []
            
            # Import nodes
            for node_id, node_data in topology_data["nodes"].items():
                try:
                    node_info = NodeInfo(**node_data)
                    self.update_node(node_info)
                except Exception as e:
                    logger.error(f"Error importing node {node_id}: {e}")
            
            # Import services
            for service_id, service_data in topology_data["services"].items():
                try:
                    service_info = ServiceInfo(**service_data)
                    self.update_service(service_info)
                except Exception as e:
                    logger.error(f"Error importing service {service_id}: {e}")
            
            # Import links
            for link_data in topology_data["links"]:
                try:
                    link_info = NetworkLink(**link_data)
                    self.update_link(link_info)
                except Exception as e:
                    logger.error(f"Error importing link: {e}")
            
            # Take a new snapshot
            self._create_topology_snapshot()
            
            logger.info("Topology import completed")
            return True
        
        except Exception as e:
            logger.error(f"Error importing topology: {e}")
            return False
    
    def compute_network_statistics(self) -> Dict[str, Any]:
        """
        Compute statistics about the network topology
        
        Returns:
            Dictionary with statistics
        """
        with self.graph_lock:
            try:
                stats = {
                    "node_count": self.graph.number_of_nodes(),
                    "link_count": self.graph.number_of_edges(),
                    "connected_components": nx.number_connected_components(self.graph),
                    "density": nx.density(self.graph),
                    "clustering_coefficient": nx.average_clustering(self.graph),
                }
                
                # Calculate additional metrics if the graph is connected
                if nx.is_connected(self.graph) and self.graph.number_of_nodes() > 1:
                    stats.update({
                        "diameter": nx.diameter(self.graph),
                        "radius": nx.radius(self.graph),
                        "average_shortest_path_length": nx.average_shortest_path_length(self.graph),
                        "critical_nodes": len(list(nx.articulation_points(self.graph)))
                    })
                
                # Node degree statistics
                degrees = [d for _, d in self.graph.degree()]
                if degrees:
                    stats.update({
                        "average_degree": sum(degrees) / len(degrees),
                        "max_degree": max(degrees),
                        "min_degree": min(degrees)
                    })
                
                return stats
            
            except Exception as e:
                logger.error(f"Error computing network statistics: {e}")
                return {
                    "error": str(e),
                    "node_count": len(self.nodes),
                    "link_count": len(self.links),
                    "service_count": len(self.services)
                }
    
    def generate_graphviz(self) -> str:
        """
        Generate Graphviz DOT representation of the topology
        
        Returns:
            Graphviz DOT string
        """
        with self.nodes_lock, self.services_lock, self.links_lock:
            try:
                dot = ["digraph ClusterTopology {"]
                dot.append("  graph [rankdir=LR, overlap=false, splines=true];")
                dot.append("  node [shape=box, style=filled, fontname=Arial];")
                dot.append("  edge [fontname=Arial];        """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
        
        logger.info("Discovery loop stopped")
    
    def _update_loop(self):
        """Loop for updating existing topology information"""
        logger.info("Update loop started")
        
        while self.running:
            try:
                logger.info("Starting topology update cycle")
                
                # Update node statuses
                self._update_node_statuses()
                
                # Update service statuses
                self._update_service_statuses()
                
                # Update link statuses
                self._update_link_statuses()
                
                # Remove stale nodes
                self._cleanup_stale_nodes()
                
                # Notify listeners
                self._notify_listeners("topology_status_updated", {})
            
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
            
            # Sleep until next update cycle
            sleep_interval = min(30, self.update_interval)  # Check at most every 30 seconds
            for _ in range(int(self.update_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)
        
        logger.info("Update loop stopped")
    
    def _discover_by_network_scan(self):
        """Discover topology by network scanning"""
        logger.info("Starting network scan discovery")
        
        # Get subnets to scan from configuration
        subnets = self.config.get("discovery_subnets", ["127.0.0.1/24"])
        exclude_ips = set(self.config.get("discovery_exclude_ips", []))
        
        discovered_nodes = []
        
        for subnet_str in subnets:
            try:
                # Parse subnet
                subnet = ipaddress.ip_network(subnet_str)
                logger.info(f"Scanning subnet: {subnet}")
                
                # Limit scan to prevent excessive scanning
                host_count = 0
                host_limit = min(1024, subnet.num_addresses)  # Reasonable limit for scanning
                
                # Scan subnet
                for ip in subnet.hosts():
                    ip_str = str(ip)
                    
                    # Skip excluded IPs
                    if ip_str in exclude_ips:
                        continue
                    
                    # Check if host is reachable
                    if self._ping_host(ip_str):
                        # Try to resolve hostname
                        hostname = self._resolve_hostname(ip_str) or ip_str
                        
                        # Create node ID from hostname
                        node_id = self._generate_node_id(hostname, ip_str)
                        
                        # Create node information
                        node_info = NodeInfo(
                            node_id=node_id,
                            hostname=hostname,
                            ip_address=ip_str,
                            status="unknown",
                            last_seen=time.time(),
                            last_updated=time.time()
                        )
                        
                        # Update or add node
                        self.update_node(node_info)
                        discovered_nodes.append(node_id)
                        
                        logger.info(f"Discovered node: {node_id} ({ip_str})")
                    
                    # Increment host count and check limit
                    host_count += 1
                    if host_count >= host_limit:
                        logger.warning(f"Host limit reached for subnet {subnet}, stopping scan")
                        break
            
            except Exception as e:
                logger.error(f"Error scanning subnet {subnet_str}: {e}")
        
        # Discover network links between nodes
        self._discover_network_links(discovered_nodes)
        
        # Discover services on nodes
        self._discover_services(discovered_nodes)
        
        logger.info(f"Network scan discovery completed: {len(discovered_nodes)} nodes discovered")
    
    def _discover_by_kubernetes(self):
        """Discover topology from Kubernetes API"""
        logger.info("Starting Kubernetes discovery")
        
        try:
            # Check if the kubernetes module is available
            try:
                from kubernetes import client, config
            except ImportError:
                logger.error("Kubernetes module not installed. Use 'pip install kubernetes'")
                return
            
            # Load configuration from default location
            try:
                config.load_kube_config()
            except Exception:
                # Try in-cluster config
                try:
                    config.load_incluster_config()
                except Exception as e:
                    logger.error(f"Failed to load Kubernetes configuration: {e}")
                    return
            
            # Create API clients
            core_api = client.CoreV1Api()
            apps_api = client.AppsV1Api()
            
            # Get nodes
            nodes = core_api.list_node().items
            for k8s_node in nodes:
                node_name = k8s_node.metadata.name
                
                # Get IP address
                ip_address = None
                for address in k8s_node.status.addresses:
                    if address.type == "InternalIP":
                        ip_address = address.address
                        break
                
                if not ip_address:
                    logger.warning(f"No IP address found for node {node_name}")
                    continue
                
                # Extract labels
                labels = k8s_node.metadata.labels or {}
                
                # Create node ID
                node_id = self._generate_node_id(node_name, ip_address)
                
                # Get resources
                resources = {}
                if k8s_node.status.capacity:
                    resources = {
                        "cpu": k8s_node.status.capacity.get("cpu"),
                        "memory": k8s_node.status.capacity.get("memory"),
                        "pods": k8s_node.status.capacity.get("pods")
                    }
                
                # Determine node status
                status = "unknown"
                if k8s_node.status.conditions:
                    for condition in k8s_node.status.conditions:
                        if condition.type == "Ready":
                            status = "healthy" if condition.status == "True" else "degraded"
                            break
                
                # Get location information
                region = labels.get("topology.kubernetes.io/region")
                zone = labels.get("topology.kubernetes.io/zone")
                
                # Create node information
                node_info = NodeInfo(
                    node_id=node_id,
                    hostname=node_name,
                    ip_address=ip_address,
                    status=status,
                    labels=labels,
                    region=region,
                    datacenter=zone,
                    node_type="worker",
                    resources=resources,
                    last_seen=time.time(),
                    last_updated=time.time()
                )
                
                # Update or add node
                self.update_node(node_info)
                logger.info(f"Discovered Kubernetes node: {node_id}")
            
            # Get services
            services = core_api.list_service_for_all_namespaces().items
            for k8s_service in services:
                service_name = k8s_service.metadata.name
                namespace = k8s_service.metadata.namespace
                
                # Create service ID
                service_id = f"{namespace}/{service_name}"
                
                # Get endpoints
                endpoints = []
                if k8s_service.spec.ports:
                    for port in k8s_service.spec.ports:
                        endpoint = {
                            "port": port.port,
                            "protocol": port.protocol,
                            "target_port": port.target_port
                        }
                        endpoints.append(endpoint)
                
                # Get selector
                selector = k8s_service.spec.selector or {}
                
                # Create service information
                service_info = ServiceInfo(
                    service_id=service_id,
                    service_name=service_name,
                    service_type="kubernetes",
                    endpoints=endpoints,
                    metadata={
                        "namespace": namespace,
                        "selector": selector,
                        "type": k8s_service.spec.type,
                        "cluster_ip": k8s_service.spec.cluster_ip
                    },
                    last_updated=time.time()
                )
                
                # Find nodes running this service
                if selector:
                    # Get pods with this selector
                    selector_str = ",".join([f"{k}={v}" for k, v in selector.items()])
                    pods = core_api.list_pod_for_all_namespaces(
                        label_selector=selector_str
                    ).items
                    
                    # Extract node names
                    node_names = set()
                    for pod in pods:
                        if pod.spec.node_name:
                            node_names.add(pod.spec.node_name)
                    
                    # Find corresponding node IDs
                    node_ids = []
                    with self.nodes_lock:
                        for node_id, node in self.nodes.items():
                            if node.hostname in node_names:
                                node_ids.append(node_id)
                    
                    service_info.nodes = node_ids
                
                # Find service dependencies
                # This would require more complex analysis of Kubernetes resources
                
                # Update or add service
                self.update_service(service_info)
                logger.info(f"Discovered Kubernetes service: {service_id}")
            
            # Discover network links
            # In Kubernetes, nodes are typically all connected to each other
            node_ids = []
            with self.nodes_lock:
                node_ids = list(self.nodes.keys())
            
            for i, source_id in enumerate(node_ids):
                for target_id in node_ids[i+1:]:
                    # Create link
                    link = NetworkLink(
                        source_node=source_id,
                        target_node=target_id,
                        link_type="kubernetes",
                        status="up",
                        last_updated=time.time()
                    )
                    
                    # Update or add link
                    self.update_link(link)
            
            logger.info(f"Kubernetes discovery completed: {len(node_ids)} nodes, {len(services)} services")
        
        except Exception as e:
            logger.error(f"Error in Kubernetes discovery: {e}")
    
    def _discover_by_consul(self):
        """Discover topology from Consul API"""
        logger.info("Starting Consul discovery")
        
        try:
            # Check if the consul module is available
            try:
                import consul
            except ImportError:
                logger.error("Consul module not installed. Use 'pip install python-consul'")
                return
            
            # Get Consul connection parameters from configuration
            consul_host = self.config.get("consul_host", "localhost")
            consul_port = self.config.get("consul_port", 8500)
            consul_token = self.config.get("consul_token")
            consul_scheme = self.config.get("consul_scheme", "http")
            
            # Create Consul client
            consul_client = consul.Consul(
                host=consul_host,
                port=consul_port,
                token=consul_token,
                scheme=consul_scheme
            )
            
            # Get nodes
            index, nodes = consul_client.catalog.nodes()
            
            for node in nodes:
                node_name = node["Node"]
                ip_address = node["Address"]
                
                # Create node ID
                node_id = self._generate_node_id(node_name, ip_address)
                
                # Get node health
                index, health = consul_client.health.node(node_name)
                
                # Determine node status
                status = "unknown"
                if health:
                    status_counts = {"passing": 0, "warning": 0, "critical": 0}
                    for check in health:
                        status_counts[check["Status"]] = status_counts.get(check["Status"], 0) + 1
                    
                    if status_counts.get("critical", 0) > 0:
                        status = "critical"
                    elif status_counts.get("warning", 0) > 0:
                        status = "degraded"
                    elif status_counts.get("passing", 0) > 0:
                        status = "healthy"
                
                # Get node metadata
                index, node_info = consul_client.catalog.node(node_name)
                datacenter = node_info.get("Datacenter")
                
                # Create node information
                node_info = NodeInfo(
                    node_id=node_id,
                    hostname=node_name,
                    ip_address=ip_address,
                    status=status,
                    datacenter=datacenter,
                    last_seen=time.time(),
                    last_updated=time.time()
                )
                
                # Update or add node
                self.update_node(node_info)
                logger.info(f"Discovered Consul node: {node_id}")
            
            # Get services
            index, services = consul_client.catalog.services()
            
            for service_name, tags in services.items():
                # Get instances of this service
                index, service_instances = consul_client.catalog.service(service_name)
                
                if not service_instances:
                    continue
                
                # Create service ID
                service_id = f"consul/{service_name}"
                
                # Collect nodes running this service
                node_ids = []
                endpoints = []
                
                for instance in service_instances:
                    node_name = instance["Node"]
                    
                    # Find corresponding node ID
                    with self.nodes_lock:
                        for node_id, node in self.nodes.items():
                            if node.hostname == node_name:
                                node_ids.append(node_id)
                                break
                    
                    # Create endpoint
                    endpoint = {
                        "host": instance["ServiceAddress"] or instance["Address"],
                        "port": instance["ServicePort"],
                        "tags": instance["ServiceTags"]
                    }
                    endpoints.append(endpoint)
                
                # Create service information
                service_info = ServiceInfo(
                    service_id=service_id,
                    service_name=service_name,
                    service_type="consul",
                    nodes=node_ids,
                    endpoints=endpoints,
                    metadata={"tags": tags},
                    last_updated=time.time()
                )
                
                # Update or add service
                self.update_service(service_info)
                logger.info(f"Discovered Consul service: {service_id}")
            
            # Discover dependencies through service health checks
            # This is more complex and would require analyzing service health checks
            
            logger.info(f"Consul discovery completed: {len(nodes)} nodes, {len(services)} services")
        
        except Exception as e:
            logger.error(f"Error in Consul discovery: {e}")
    
    def _discover_by_zookeeper(self):
        """Discover topology from ZooKeeper"""
        logger.info("Starting ZooKeeper discovery")
        
        try:
            # Check if the kazoo module is available
            try:
                from kazoo.client import KazooClient
            except ImportError:
                logger.error("Kazoo module not installed. Use 'pip install kazoo'")
                return
            
            # Get ZooKeeper connection parameters from configuration
            zk_hosts = self.config.get("zookeeper_hosts", "localhost:2181")
            
            # Create ZooKeeper client
            zk = KazooClient(hosts=zk_hosts)
            zk.start()
            
            try:
                # ZooKeeper doesn't have a built-in service registry like Consul
                # This is just a basic implementation that assumes services register under /services
                
                # Get services
                if zk.exists("/services"):
                    services = zk.get_children("/services")
                    
                    for service_name in services:
                        # Get instances of this service
                        service_path = f"/services/{service_name}"
                        if zk.exists(service_path):
                            instances = zk.get_children(service_path)
                            
                            if not instances:
                                continue
                            
                            # Create service ID
                            service_id = f"zookeeper/{service_name}"
                            
                            # Collect endpoints
                            endpoints = []
                            metadata = {}
                            
                            for instance_id in instances:
                                instance_path = f"{service_path}/{instance_id}"
                                if zk.exists(instance_path):
                                    data, _ = zk.get(instance_path)
                                    
                                    try:
                                        instance_data = json.loads(data.decode('utf-8'))
                                        
                                        # Extract endpoint information
                                        if "host" in instance_data and "port" in instance_data:
                                            endpoint = {
                                                "host": instance_data["host"],
                                                "port": instance_data["port"]
                                            }
                                            endpoints.append(endpoint)
                                        
                                        # Extract metadata
                                        if "metadata" in instance_data:
                                            metadata.update(instance_data["metadata"])
                                    
                                    except (json.JSONDecodeError, UnicodeDecodeError):
                                        logger.warning(f"Invalid data format for instance {instance_id}")
                            
                            # Create service information
                            service_info = ServiceInfo(
                                service_id=service_id,
                                service_name=service_name,
                                service_type="zookeeper",
                                endpoints=endpoints,
                                metadata=metadata,
                                last_updated=time.time()
                            )
                            
                            # Update or add service
                            self.update_service(service_info)
                            logger.info(f"Discovered ZooKeeper service: {service_id}")
                
                logger.info(f"ZooKeeper discovery completed")
            
            finally:
                zk.stop()
                zk.close()
        
        except Exception as e:
            logger.error(f"Error in ZooKeeper discovery: {e}")
    
    def _discover_network_links(self, node_ids: List[str]):
        """Discover network links between nodes"""
        logger.info(f"Discovering network links for {len(node_ids)} nodes")
        
        # Get node information
        nodes = {}
        with self.nodes_lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    nodes[node_id] = self.nodes[node_id]
        
        # Check connectivity between nodes
        for i, (source_id, source_node) in enumerate(nodes.items()):
            for target_id, target_node in list(nodes.items())[i+1:]:
                try:
                    # Check connectivity
                    if self._check_connectivity(source_node.ip_address, target_node.ip_address):
                        # Measure network metrics
                        metrics = self._measure_network_metrics(source_node.ip_address, target_node.ip_address)
                        
                        # Create link
                        link = NetworkLink(
                            source_node=source_id,
                            target_node=target_id,
                            link_type="ethernet",
                            status="up",
                            last_updated=time.time()
                        )
                        
                        if metrics:
                            link.bandwidth = metrics.get("bandwidth")
                            link.latency = metrics.get("latency")
                            link.packet_loss = metrics.get("packet_loss")
                        
                        # Update or add link
                        self.update_link(link)
                        logger.info(f"Discovered network link: {source_id} - {target_id}")
                
                except Exception as e:
                    logger.error(f"Error discovering link {source_id} - {target_id}: {e}")
    
    def _discover_services(self, node_ids: List[str]):
        """Discover services running on nodes"""
        logger.info(f"Discovering services for {len(node_ids)} nodes")
        
        # Get node information
        nodes = {}
        with self.nodes_lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    nodes[node_id] = self.nodes[node_id]
        
        # Common service ports to check
        service_ports = {
            "http": 80,
            "https": 443,
            "ssh": 22,
            "mysql": 3306,
            "postgresql": 5432,
            "redis": 6379,
            "mongodb": 27017,
            "elasticsearch": 9200,
            "kafka": 9092,
            "zookeeper": 2181
        }
        
        # Check for services on each node
        for node_id, node in nodes.items():
            services_found = []
            
            for service_name, port in service_ports.items():
                try:
                    if self._check_port(node.ip_address, port):
                        # Create service ID
                        service_id = f"{service_name}-{node_id}"
                        
                        # Check if service exists
                        with self.services_lock:
                            if service_id in self.services:
                                # Update existing service
                                service = self.services[service_id]
                                if node_id not in service.nodes:
                                    service.nodes.append(node_id)
                                service.last_updated = time.time()
                            else:
                                # Create new service
                                service_info = ServiceInfo(
                                    service_id=service_id,
                                    service_name=service_name,
                                    service_type="detected",
                                    nodes=[node_id],
                                    endpoints=[{
                                        "host": node.ip_address,
                                        "port": port,
                                        "protocol": "tcp"
                                    }],
                                    status="unknown",
                                    last_updated=time.time()
                                )
                                
                                self.update_service(service_info)
                        
                        services_found.append(service_name)
                        logger.info(f"Discovered service: {service_name} on {node_id}")
                
                except Exception as e:
                    logger.error(f"Error checking service {service_name} on {node_id}: {e}")
            
            # Update node's service list
            if services_found:
                with self.nodes_lock:
                    if node_id in self.nodes:
                        self.nodes[node_id].services = services_found
            
            # Try to detect services using zeroconf/mDNS if available
            self._discover_mdns_services(node_id, node)
    
    def _discover_mdns_services(self, node_id: str, node: NodeInfo):
        """Discover services using mDNS/Zeroconf"""
        try:
            # Check if the zeroconf module is available
            try:
                from zeroconf import ServiceBrowser, Zeroconf
            except ImportError:
                return  # Silently skip if not available
            
            class ServiceListener:
                def __init__(self, outer, node_id):
                    self.outer = outer
                    self.node_id = node_id
                    self.services_found = []
                
                def add_service(self, zc, type, name):
                    info = zc.get_service_info(type, name)
                    if info:
                        service_name = name.split('.')[0]
                        service_type = type.split('.')[0]
                        
                        # Extract IP and port
                        ip = ".".join(str(x) for x in info.addresses[0]) if info.addresses else None
                        port = info.port
                        
                        if ip and port:
                            # Create service ID
                            service_id = f"{service_type}-{self.node_id}"
                            
                            # Create service info
                            service_info = ServiceInfo(
                                service_id=service_id,
                                service_name=service_name,
                                service_type="mdns",
                                nodes=[self.node_id],
                                endpoints=[{
                                    "host": ip,
                                    "port": port,
                                    "protocol": "tcp"
                                }],
                                status="unknown",
                                metadata={"mdns_type": type},
                                last_updated=time.time()
                            )
                            
                            self.outer.update_service(service_info)
                            self.services_found.append(service_type)
                            logger.info(f"Discovered mDNS service: {service_name} on {self.node_id}")
            
            # Create zeroconf instance
            zc = Zeroconf()
            listener = ServiceListener(self, node_id)
            
            # Browse for common service types
            browser = ServiceBrowser(zc, "_http._tcp.local.", listener)
            browser = ServiceBrowser(zc, "_ssh._tcp.local.", listener)
            
            # Wait a short time for discovery
            time.sleep(3)
            
            # Clean up
            zc.close()
            
            # Update node's service list with mDNS services
            if listener.services_found:
                with self.nodes_lock:
                    if node_id in self.nodes:
                        self.nodes[node_id].services.extend(listener.services_found)
        
        except Exception as e:
            logger.debug(f"Error in mDNS discovery for {node_id}: {e}")
    
    def _update_node_statuses(self):
        """Update status of all nodes"""
        logger.info("Updating node statuses")
        
        with self.nodes_lock:
            for node_id, node in list(self.nodes.items()):
                try:
                    # Skip recently updated nodes
                    if time.time() - node.last_updated < 60:  # Don't update nodes updated in the last minute
                        continue
                    
                    # Check if node is reachable
                    reachable = self._ping_host(node.ip_address)
                    
                    # Update status
                    if reachable:
                        if node.status == "offline":
                            node.status = "unknown"  # Reset to unknown when it comes back online
                            logger.info(f"Node {node_id} is now reachable")
                        
                        node.last_seen = time.time()
                    else:
                        if node.status != "offline":
                            node.status = "offline"
                            logger.warning(f"Node {node_id} is unreachable")
                    
                    node.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for node {node_id}: {e}")
    
    def _update_service_statuses(self):
        """Update status of all services"""
        logger.info("Updating service statuses")
        
        with self.services_lock, self.nodes_lock:
            for service_id, service in list(self.services.items()):
                try:
                    # Skip recently updated services
                    if time.time() - service.last_updated < 60:  # Don't update services updated in the last minute
                        continue
                    
                    # Check node status for each service node
                    offline_nodes = 0
                    for node_id in service.nodes:
                        if node_id in self.nodes and self.nodes[node_id].status == "offline":
                            offline_nodes += 1
                    
                    # Update service status based on node status
                    if not service.nodes:
                        service.status = "unavailable"
                    elif offline_nodes == len(service.nodes):
                        service.status = "unavailable"
                    elif offline_nodes > 0:
                        service.status = "degraded"
                    else:
                        # Check if any endpoint is reachable
                        available = False
                        for endpoint in service.endpoints:
                            host = endpoint.get("host")
                            port = endpoint.get("port")
                            
                            if host and port and self._check_port(host, port):
                                available = True
                                break
                        
                        service.status = "available" if available else "degraded"
                    
                    service.last_updated = time.time()
                
                except Exception as e:
                    logger.error(f"Error updating status for service {service_id}: {e}")
    
    def _update_link_statuses(self):
        """Update status of all network links"""
        logger.info("Updating link statuses")
        
        with self.links_lock, self.nodes_lock:
            for link in list(self.links):
                try:
                    # Skip recently updated links
                    if time.time() - link.last_updated < 300:  # Don't update links updated in the last 5 minutes
                        continue
                    
                    # Get node information
                    source_node = self.nodes.get(link.source_node)
                    target_node = self.nodes.get(link.target_node)
                    
                    if not source_node or not target_node:
                        continue
                    
                    # Check connectivity
                    if source_node.status != "offline" and target_node.status != "offline":
                        connectivity = self._check_connectivity(source_node.ip_address, target_node.ip_address)
                        
                        # Update status
                        link.status = "up" if connectivity else "down"
                        
                        # Update metrics if needed
                        if connectivity and (link.bandwidth is None or time.time() - link.last_updated > 3600):
                            metrics = self._measure_network_metrics(source_node.ip_address, target_node.ip_address)
                            
                            if metrics:
                                link.bandwidth = metrics.get("bandwidth")
                                link.latency = metrics.get("latency")
                                link.packet_loss = metrics.get("packet_loss")
                    elif source_node.status == "offline" or target_node.status == "offline":
                        # Mark as down if either node is offline
                        link.status = "down"
                    
                    link.last#!/usr/bin/env python3
"""
Cluster Topology Manager for ClusterSentry

This module discovers and maintains the topology of the cluster, providing
information about network structure, node relationships, and service dependencies
for intelligent failure impact assessment and recovery planning.
"""

import os
import sys
import time
import json
import logging
import threading
import ipaddress
import socket
import subprocess
import redis
import networkx as nx
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Union, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("topology_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TopologyManager")

@dataclass
class NodeInfo:
    """Information about a node in the cluster topology"""
    node_id: str
    hostname: str
    ip_address: str
    status: str = "unknown"  # unknown, healthy, degraded, critical, offline
    labels: Dict[str, str] = field(default_factory=dict)
    rack: Optional[str] = None
    datacenter: Optional[str] = None
    region: Optional[str] = None
    node_type: str = "worker"  # master, worker, storage, etc.
    services: List[str] = field(default_factory=list)
    resources: Dict[str, Any] = field(default_factory=dict)
    neighbors: List[str] = field(default_factory=list)
    last_seen: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)

@dataclass
class ServiceInfo:
    """Information about a service in the cluster topology"""
    service_id: str
    service_name: str
    service_type: str
    nodes: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    endpoints: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "unknown"  # unknown, available, degraded, unavailable
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

@dataclass
class NetworkLink:
    """Information about a network link between nodes"""
    source_node: str
    target_node: str
    link_type: str = "ethernet"  # ethernet, infiniband, fiber, etc.
    bandwidth: Optional[float] = None  # Mbps
    latency: Optional[float] = None  # ms
    packet_loss: Optional[float] = None  # percentage
    status: str = "unknown"  # unknown, up, down, degraded
    last_updated: float = field(default_factory=time.time)

class TopologyDiscoveryMethod(Enum):
    """Methods for topology discovery"""
    NETWORK_SCAN = "network_scan"
    KUBERNETES = "kubernetes"
    CONSUL = "consul"
    ZOOKEEPER = "zookeeper"
    MANUAL = "manual"
    IMPORT = "import"

class ClusterTopologyManager:
    """
    Manages the topology of the cluster, providing information about
    network structure, node relationships, and service dependencies.
    
    Features:
    - Automated topology discovery
    - Topology visualization
    - Critical path analysis
    - Failure impact prediction
    - Historical topology tracking
    """
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the topology manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        
        # Set default configuration values
        self.discovery_interval = self.config.get("discovery_interval", 3600)
        self.update_interval = self.config.get("update_interval", 300)
        self.max_history = self.config.get("max_history", 100)
        self.discovery_method = TopologyDiscoveryMethod(self.config.get("discovery_method", "network_scan"))
        
        # Initialize Redis client if configured
        self.redis_client = None
        if self.config.get("redis_enabled", True):
            self.redis_client = self._init_redis_client()
        
        # Directories for persistent storage
        self.data_dir = Path(self.config.get("data_directory", "data/topology"))
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.history_dir = self.data_dir / "history"
        self.history_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize topology data structures
        self.nodes = {}  # Dict[node_id, NodeInfo]
        self.services = {}  # Dict[service_id, ServiceInfo]
        self.links = []  # List[NetworkLink]
        
        # Network graph for topology operations
        self.graph = nx.Graph()
        
        # Locks for thread safety
        self.nodes_lock = threading.RLock()
        self.services_lock = threading.RLock()
        self.links_lock = threading.RLock()
        self.graph_lock = threading.RLock()
        
        # Topology history
        self.topology_history = []  # List of timestamped topology snapshots
        
        # Event listeners
        self.listeners = {}  # Dict[event_type, List[callback]]
        self.listeners_lock = threading.RLock()
        
        # Threads
        self.running = False
        self.discovery_thread = None
        self.update_thread = None
        self.pubsub_thread = None
        self.pubsub = None
        
        logger.info("Cluster Topology Manager initialized")
    
    def _load_config(self, config_path: Optional[Union[str, Path]]) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            "discovery_interval": 3600,  # 1 hour
            "update_interval": 300,      # 5 minutes
            "max_history": 100,
            "redis_enabled": True,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_password": None,
            "redis_namespace": "clustersentry",
            "discovery_method": "network_scan",
            "discovery_subnets": ["192.168.1.0/24"],
            "discovery_exclude_ips": ["192.168.1.1"],
            "node_stale_threshold": 86400,  # 24 hours
            "data_directory": "data/topology",
            "log_level": "INFO"
        }
        
        if not config_path:
            logger.warning("No configuration path provided, using defaults")
            return default_config
        
        try:
            path = Path(config_path)
            if not path.exists():
                logger.warning(f"Configuration file not found: {path}, using defaults")
                return default_config
            
            with open(path, 'r') as f:
                if path.suffix.lower() == '.yaml' or path.suffix.lower() == '.yml':
                    import yaml
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
            
            # Merge with defaults for backward compatibility
            merged_config = default_config.copy()
            merged_config.update(config)
            
            logger.info(f"Loaded configuration from {path}")
            return merged_config
        
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return default_config
    
    def _init_redis_client(self) -> Optional[redis.Redis]:
        """Initialize Redis client"""
        try:
            client = redis.Redis(
                host=self.config.get("redis_host", "localhost"),
                port=self.config.get("redis_port", 6379),
                password=self.config.get("redis_password"),
                db=self.config.get("redis_db", 0),
                decode_responses=True
            )
            
            # Test connection
            client.ping()
            logger.info("Connected to Redis")
            return client
        
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return None
    
    def start(self):
        """Start topology management threads"""
        if self.running:
            logger.warning("Topology manager is already running")
            return
        
        self.running = True
        
        # Start discovery thread
        self.discovery_thread = threading.Thread(
            target=self._discovery_loop,
            daemon=True
        )
        self.discovery_thread.start()
        
        # Start update thread
        self.update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )
        self.update_thread.start()
        
        # Start Redis pubsub thread if Redis is available
        if self.redis_client:
            self._setup_redis_subscription()
        
        # Load existing topology data if available
        self._load_topology_data()
        
        logger.info("Topology manager started")
    
    def stop(self):
        """Stop topology management threads"""
        if not self.running:
            logger.warning("Topology manager is not running")
            return
        
        self.running = False
        
        # Save current topology data
        self._save_topology_data()
        
        # Wait for threads to stop
        for thread in [self.discovery_thread, self.update_thread, self.pubsub_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5.0)
        
        # Clean up Redis subscription
        if self.pubsub:
            self.pubsub.unsubscribe()
        
        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
        
        logger.info("Topology manager stopped")
    
    def _setup_redis_subscription(self):
        """Set up Redis subscription for topology updates"""
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            self.pubsub = self.redis_client.pubsub()
            self.pubsub.subscribe(f"{namespace}:topology_updates")
            
            # Start pubsub listener thread
            self.pubsub_thread = threading.Thread(
                target=self._pubsub_listener,
                daemon=True
            )
            self.pubsub_thread.start()
            
            logger.info("Redis subscription set up")
        except Exception as e:
            logger.error(f"Failed to set up Redis subscription: {e}")
    
    def _pubsub_listener(self):
        """Listen for topology updates from Redis"""
        logger.info("Redis pubsub listener started")
        
        try:
            while self.running:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    try:
                        data = json.loads(message["data"])
                        update_type = data.get("type")
                        
                        if update_type == "node_update":
                            node_id = data.get("node_id")
                            node_data = data.get("node_info")
                            
                            if node_id and node_data:
                                # Update node info
                                node_info = NodeInfo(**node_data)
                                self.update_node(node_info)
                        
                        elif update_type == "service_update":
                            service_id = data.get("service_id")
                            service_data = data.get("service_info")
                            
                            if service_id and service_data:
                                # Update service info
                                service_info = ServiceInfo(**service_data)
                                self.update_service(service_info)
                        
                        elif update_type == "link_update":
                            link_data = data.get("link_info")
                            
                            if link_data:
                                # Update link info
                                link_info = NetworkLink(**link_data)
                                self.update_link(link_info)
                        
                        elif update_type == "topology_updated":
                            # Reload entire topology
                            if self.redis_client:
                                self._load_topology_from_redis()
                    
                    except json.JSONDecodeError:
                        logger.warning("Received invalid JSON in topology update")
                    except Exception as e:
                        logger.error(f"Error processing topology update: {e}")
                
                time.sleep(0.1)  # Prevent CPU spinning
        
        except Exception as e:
            logger.error(f"Error in Redis pubsub listener: {e}")
        
        logger.info("Redis pubsub listener stopped")
    
    def _load_topology_data(self):
        """Load topology data from persistent storage"""
        # Try to load from Redis first if available
        if self.redis_client and self._load_topology_from_redis():
            logger.info("Loaded topology data from Redis")
            return
        
        # Otherwise, load from files
        try:
            # Load nodes
            nodes_path = self.data_dir / "nodes.json"
            if nodes_path.exists():
                with open(nodes_path, 'r') as f:
                    nodes_data = json.load(f)
                
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_data in nodes_data.items():
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_path = self.data_dir / "services.json"
            if services_path.exists():
                with open(services_path, 'r') as f:
                    services_data = json.load(f)
                
                with self.services_lock:
                    self.services = {}
                    for service_id, service_data in services_data.items():
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_path = self.data_dir / "links.json"
            if links_path.exists():
                with open(links_path, 'r') as f:
                    links_data = json.load(f)
                
                with self.links_lock:
                    self.links = []
                    for link_data in links_data:
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            logger.info("Loaded topology data from files")
        
        except Exception as e:
            logger.error(f"Error loading topology data: {e}")
    
    def _load_topology_from_redis(self) -> bool:
        """Load topology data from Redis"""
        if not self.redis_client:
            return False
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Load nodes
            nodes_key = f"{namespace}:topology:nodes"
            nodes_data = self.redis_client.hgetall(nodes_key)
            
            if nodes_data:
                with self.nodes_lock:
                    self.nodes = {}
                    for node_id, node_json in nodes_data.items():
                        node_data = json.loads(node_json)
                        self.nodes[node_id] = NodeInfo(**node_data)
            
            # Load services
            services_key = f"{namespace}:topology:services"
            services_data = self.redis_client.hgetall(services_key)
            
            if services_data:
                with self.services_lock:
                    self.services = {}
                    for service_id, service_json in services_data.items():
                        service_data = json.loads(service_json)
                        self.services[service_id] = ServiceInfo(**service_data)
            
            # Load links
            links_key = f"{namespace}:topology:links"
            links_data = self.redis_client.lrange(links_key, 0, -1)
            
            if links_data:
                with self.links_lock:
                    self.links = []
                    for link_json in links_data:
                        link_data = json.loads(link_json)
                        self.links.append(NetworkLink(**link_data))
            
            # Rebuild graph
            self._rebuild_graph()
            
            return True
        
        except Exception as e:
            logger.error(f"Error loading topology from Redis: {e}")
            return False
    
    def _save_topology_data(self):
        """Save topology data to persistent storage"""
        try:
            # Save to Redis if available
            if self.redis_client:
                self._save_topology_to_redis()
            
            # Save nodes
            with self.nodes_lock:
                nodes_data = {node_id: asdict(node) for node_id, node in self.nodes.items()}
                
                with open(self.data_dir / "nodes.json", 'w') as f:
                    json.dump(nodes_data, f, indent=2)
            
            # Save services
            with self.services_lock:
                services_data = {service_id: asdict(service) for service_id, service in self.services.items()}
                
                with open(self.data_dir / "services.json", 'w') as f:
                    json.dump(services_data, f, indent=2)
            
            # Save links
            with self.links_lock:
                links_data = [asdict(link) for link in self.links]
                
                with open(self.data_dir / "links.json", 'w') as f:
                    json.dump(links_data, f, indent=2)
            
            logger.info("Saved topology data to files")
        
        except Exception as e:
            logger.error(f"Error saving topology data: {e}")
    
    def _save_topology_to_redis(self):
        """Save topology data to Redis"""
        if not self.redis_client:
            return
        
        try:
            namespace = self.config.get("redis_namespace", "clustersentry")
            
            # Save nodes
            with self.nodes_lock:
                nodes_key = f"{namespace}:topology:nodes"
                nodes_data = {}
                
                for node_id, node in self.nodes.items():
                    nodes_data[node_id] = json.dumps(asdict(node))
                
                if nodes_data:
                    self.redis_client.delete(nodes_key)
                    self.redis_client.hset(nodes_key, mapping=nodes_data)
            
            # Save services
            with self.services_lock:
                services_key = f"{namespace}:topology:services"
                services_data = {}
                
                for service_id, service in self.services.items():
                    services_data[service_id] = json.dumps(asdict(service))
                
                if services_data:
                    self.redis_client.delete(services_key)
                    self.redis_client.hset(services_key, mapping=services_data)
            
            # Save links
            with self.links_lock:
                links_key = f"{namespace}:topology:links"
                
                self.redis_client.delete(links_key)
                
                for link in self.links:
                    self.redis_client.rpush(links_key, json.dumps(asdict(link)))
            
            logger.info("Saved topology data to Redis")
        
        except Exception as e:
            logger.error(f"Error saving topology to Redis: {e}")
    
    def _rebuild_graph(self):
        """Rebuild the network graph from nodes and links"""
        with self.graph_lock:
            # Create new graph
            self.graph = nx.Graph()
            
            # Add nodes
            with self.nodes_lock:
                for node_id, node in self.nodes.items():
                    self.graph.add_node(node_id, **asdict(node))
            
            # Add links
            with self.links_lock:
                for link in self.links:
                    self.graph.add_edge(
                        link.source_node,
                        link.target_node,
                        **asdict(link)
                    )
    
    def _discovery_loop(self):
        """Main discovery loop for topology information"""
        logger.info("Discovery loop started")
        
        while self.running:
            try:
                logger.info("Starting topology discovery cycle")
                
                # Perform discovery based on configured method
                if self.discovery_method == TopologyDiscoveryMethod.NETWORK_SCAN:
                    self._discover_by_network_scan()
                elif self.discovery_method == TopologyDiscoveryMethod.KUBERNETES:
                    self._discover_by_kubernetes()
                elif self.discovery_method == TopologyDiscoveryMethod.CONSUL:
                    self._discover_by_consul()
                elif self.discovery_method == TopologyDiscoveryMethod.ZOOKEEPER:
                    self._discover_by_zookeeper()
                else:
                    logger.warning(f"Unsupported discovery method: {self.discovery_method}")
                
                # Take a topology snapshot
                self._create_topology_snapshot()
                
                # Save topology data
                self._save_topology_data()
                
                # Notify listeners
                self._notify_listeners("topology_updated", {})
                
                # Publish update if Redis is available
                if self.redis_client:
                    namespace = self.config.get("redis_namespace", "clustersentry")
                    self.redis_client.publish(
                        f"{namespace}:topology_updates",
                        json.dumps({
                            "type": "topology_updated",
                            "timestamp": time.time()
                        })
                    )
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
            
            # Sleep until next discovery cycle
            sleep_interval = min(60, self.discovery_interval)  # Check at most every minute
            for _ in range(int(self.discovery_interval / sleep_interval)):
                if not self.running:
                    break
                time.sleep(sleep_interval)