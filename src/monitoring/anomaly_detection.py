#!/usr/bin/env python3
"""
Health Metrics Collector for ClusterSentry

This module collects health metrics from nodes and forwards them to the central
monitoring system for anomaly detection and storage.
"""

import os
import time
import json
import logging
import platform
import socket
import subprocess
import threading
import queue
import argparse
import psutil
import redis
import requests
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("metrics_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MetricsCollector")

@dataclass
class SystemMetrics:
    """Container for system metrics"""
    # Basic system information
    hostname: str
    timestamp: float
    platform: str
    
    # CPU metrics
    cpu_percent: float
    cpu_count: int
    cpu_freq_current: Optional[float] = None
    cpu_freq_min: Optional[float] = None
    cpu_freq_max: Optional[float] = None
    load_1: Optional[float] = None
    load_5: Optional[float] = None
    load_15: Optional[float] = None
    
    # Memory metrics
    memory_total: int
    memory_available: int
    memory_used: int
    memory_percent: float
    swap_total: int
    swap_used: int
    swap_percent: float
    
    # Disk metrics
    disk_total: int
    disk_used: int
    disk_free: int
    disk_percent: float
    disk_read_count: Optional[int] = None
    disk_write_count: Optional[int] = None
    disk_read_bytes: Optional[int] = None
    disk_write_bytes: Optional[int] = None
    disk_read_time: Optional[int] = None
    disk_write_time: Optional[int] = None
    
    # Network metrics
    network_bytes_sent: int
    network_bytes_recv: int
    network_packets_sent: int
    network_packets_recv: int
    network_error_in: int
    network_error_out: int
    network_drop_in: int
    network_drop_out: int
    
    # Process metrics
    process_count: int
    
    # Additional metrics
    custom_metrics: Dict[str, Any] = None

@dataclass
class ServiceMetrics:
    """Container for service-specific metrics"""
    hostname: str
    timestamp: float
    service_name: str
    status: str
    pid: Optional[int] = None
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    uptime: Optional[float] = None
    port_status: Dict[int, bool] = None
    error_count: Optional[int] = None
    log_metrics: Dict[str, Any] = None
    response_time: Optional[float] = None
    custom_metrics: Dict[str, Any] = None

class MetricsCollector:
    """Collects system and service metrics"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the metrics collector"""
        self.config = {}
        self.services = []
        self.custom_collectors = []
        self.stopped = threading.Event()
        self.collect_interval = 60  # Default: 60 seconds
        self.send_interval = 60  # Default: 60 seconds
        self.metrics_queue = queue.Queue()
        
        # Load configuration
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Set intervals from config
            self.collect_interval = self.config.get("collect_interval", 60)
            self.send_interval = self.config.get("send_interval", 60)
            
            # Load services to monitor
            self.services = self.config.get("services", [])
        
        # Initialize Redis client for sending metrics
        redis_host = self.config.get("redis_host", "localhost")
        redis_port = self.config.get("redis_port", 6379)
        redis_password = self.config.get("redis_password", None)
        
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password
        )
        
        # Get hostname for node identification
        self.hostname = socket.gethostname()
        
        logger.info(f"Metrics Collector initialized on {self.hostname}")
    
    def start(self):
        """Start the metrics collection threads"""
        # Reset the stopped event
        self.stopped.clear()
        
        # Start system metrics collection thread
        self.system_thread = threading.Thread(target=self._collect_system_metrics_loop)
        self.system_thread.daemon = True
        self.system_thread.start()
        
        # Start service metrics collection thread if services are configured
        if self.services:
            self.service_thread = threading.Thread(target=self._collect_service_metrics_loop)
            self.service_thread.daemon = True
            self.service_thread.start()
        
        # Start sender thread
        self.sender_thread = threading.Thread(target=self._send_metrics_loop)
        self.sender_thread.daemon = True
        self.sender_thread.start()
        
        logger.info("Metrics collection started")
    
    def stop(self):
        """Stop the metrics collection threads"""
        self.stopped.set()
        
        # Wait for threads to stop
        if hasattr(self, 'system_thread'):
            self.system_thread.join(timeout=5.0)
        
        if hasattr(self, 'service_thread'):
            self.service_thread.join(timeout=5.0)
        
        if hasattr(self, 'sender_thread'):
            self.sender_thread.join(timeout=5.0)
        
        # Close Redis connection
        try:
            self.redis_client.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}")
        
        logger.info("Metrics collection stopped")
    
    def _collect_system_metrics_loop(self):
        """Continuously collect system metrics at regular intervals"""
        logger.info(f"Starting system metrics collection (interval: {self.collect_interval}s)")
        
        while not self.stopped.is_set():
            try:
                # Collect metrics
                metrics = self._collect_system_metrics()
                
                # Put in queue for sending
                self.metrics_queue.put(("system", metrics))
                
                logger.debug("Collected system metrics")
            
            except Exception as e:
                logger.error(f"Error collecting system metrics: {str(e)}")
            
            # Wait for next collection interval
            self.stopped.wait(self.collect_interval)
    
    def _collect_service_metrics_loop(self):
        """Continuously collect service metrics at regular intervals"""
        logger.info(f"Starting service metrics collection for {len(self.services)} services")
        
        while not self.stopped.is_set():
            try:
                for service in self.services:
                    service_name = service.get("name")
                    if not service_name:
                        continue
                    
                    # Collect metrics for this service
                    metrics = self._collect_service_metrics(service)
                    
                    # Put in queue for sending
                    self.metrics_queue.put(("service", metrics))
                    
                    logger.debug(f"Collected metrics for service {service_name}")
                    
                    # Small delay between services to spread out collection
                    if not self.stopped.is_set():
                        self.stopped.wait(1.0)
            
            except Exception as e:
                logger.error(f"Error collecting service metrics: {str(e)}")
            
            # Wait for next collection interval
            self.stopped.wait(self.collect_interval)
    
    def _send_metrics_loop(self):
        """Continuously send collected metrics at regular intervals"""
        logger.info(f"Starting metrics sender (interval: {self.send_interval}s)")
        
        next_send_time = time.time() + self.send_interval
        metrics_batch = []
        
        while not self.stopped.is_set():
            try:
                # Get metrics from queue (with timeout to check stopped flag)
                try:
                    metrics_type, metrics = self.metrics_queue.get(timeout=1.0)
                    metrics_batch.append((metrics_type, metrics))
                    self.metrics_queue.task_done()
                except queue.Empty:
                    pass
                
                # Check if it's time to send
                current_time = time.time()
                if current_time >= next_send_time and metrics_batch:
                    # Send metrics
                    self._send_metrics_batch(metrics_batch)
                    
                    # Reset batch and set next send time
                    metrics_batch = []
                    next_send_time = current_time + self.send_interval
            
            except Exception as e:
                logger.error(f"Error in metrics sender: {str(e)}")
                # Sleep a bit to prevent tight loop in case of persistent errors
                self.stopped.wait(1.0)
    
    def _collect_system_metrics(self) -> SystemMetrics:
        """Collect system metrics"""
        # Current timestamp
        timestamp = time.time()
        
        # Platform information
        system_platform = platform.system()
        
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        
        # CPU frequency
        cpu_freq = psutil.cpu_freq()
        cpu_freq_current = cpu_freq.current if cpu_freq else None
        cpu_freq_min = cpu_freq.min if cpu_freq else None
        cpu_freq_max = cpu_freq.max if cpu_freq else None
        
        # Load average
        if system_platform != "Windows":
            load_1, load_5, load_15 = psutil.getloadavg()
        else:
            load_1 = load_5 = load_15 = None
        
        # Memory metrics
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        # Disk metrics (root filesystem)
        disk = psutil.disk_usage('/')
        
        # Disk I/O metrics
        disk_io = psutil.disk_io_counters()
        
        # Network metrics
        network = psutil.net_io_counters()
        
        # Process count
        process_count = len(psutil.pids())
        
        # Collect custom metrics from plugins
        custom_metrics = {}
        for collector in self.custom_collectors:
            try:
                custom_data = collector.collect()
                custom_metrics.update(custom_data)
            except Exception as e:
                logger.error(f"Error in custom collector: {str(e)}")
        
        # Create SystemMetrics object
        metrics = SystemMetrics(
            hostname=self.hostname,
            timestamp=timestamp,
            platform=system_platform,
            
            cpu_percent=cpu_percent,
            cpu_count=cpu_count,
            cpu_freq_current=cpu_freq_current,
            cpu_freq_min=cpu_freq_min,
            cpu_freq_max=cpu_freq_max,
            load_1=load_1,
            load_5=load_5,
            load_15=load_15,
            
            memory_total=memory.total,
            memory_available=memory.available,
            memory_used=memory.used,
            memory_percent=memory.percent,
            swap_total=swap.total,
            swap_used=swap.used,
            swap_percent=swap.percent,
            
            disk_total=disk.total,
            disk_used=disk.used,
            disk_free=disk.free,
            disk_percent=disk.percent,
            disk_read_count=disk_io.read_count if disk_io else None,
            disk_write_count=disk_io.write_count if disk_io else None,
            disk_read_bytes=disk_io.read_bytes if disk_io else None,
            disk_write_bytes=disk_io.write_bytes if disk_io else None,
            disk_read_time=disk_io.read_time if disk_io else None,
            disk_write_time=disk_io.write_time if disk_io else None,
            
            network_bytes_sent=network.bytes_sent,
            network_bytes_recv=network.bytes_recv,
            network_packets_sent=network.packets_sent,
            network_packets_recv=network.packets_recv,
            network_error_in=network.errin,
            network_error_out=network.errout,
            network_drop_in=network.dropin,
            network_drop_out=network.dropout,
            
            process_count=process_count,
            
            custom_metrics=custom_metrics
        )
        
        return metrics
    
    def _collect_service_metrics(self, service_config: Dict[str, Any]) -> ServiceMetrics:
        """Collect metrics for a specific service"""
        service_name = service_config.get("name")
        service_type = service_config.get("type", "systemd")
        check_ports = service_config.get("ports", [])
        check_process = service_config.get("process_name", service_name)
        check_url = service_config.get("url")
        log_path = service_config.get("log_path")
        
        timestamp = time.time()
        status = "unknown"
        pid = None
        cpu_percent = None
        memory_percent = None
        uptime = None
        port_status = {}
        error_count = None
        log_metrics = {}
        response_time = None
        
        # Check service status
        if service_type == "systemd":
            try:
                output = subprocess.check_output(
                    ["systemctl", "is-active", service_name],
                    stderr=subprocess.STDOUT,
                    universal_newlines=True
                ).strip()
                status = output
            except subprocess.CalledProcessError as e:
                status = e.output.strip()
        
        # Find the process
        if check_process:
            try:
                for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_percent', 'create_time']):
                    if (proc.info['name'] == check_process or 
                            (proc.info['cmdline'] and check_process in ' '.join(proc.info['cmdline']))):
                        pid = proc.info['pid']
                        cpu_percent = proc.info['cpu_percent']
                        memory_percent = proc.info['memory_percent']
                        uptime = time.time() - proc.info['create_time']
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Check ports
        for port in check_ports:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('127.0.0.1', port))
                port_status[port] = (result == 0)
                sock.close()
            except Exception:
                port_status[port] = False
        
        # Check URL if provided
        if check_url:
            try:
                start_time = time.time()
                response = requests.get(check_url, timeout=5)
                response_time = time.time() - start_time
                
                # Update status based on response
                if response.status_code < 400:
                    if status == "unknown":
                        status = "active"
                else:
                    status = "error"
            except requests.RequestException:
                response_time = None
                status = "error"
        
        # Check log file if provided
        if log_path and os.path.exists(log_path):
            try:
                # Count recent errors in log file
                error_count = 0
                
                # Read the last N lines of the log file
                last_lines = self._tail_file(log_path, 100)
                
                # Count errors and warnings
                for line in last_lines:
                    line_lower = line.lower()
                    if "error" in line_lower or "critical" in line_lower or "exception" in line_lower:
                        error_count += 1
                
                # Extract additional log metrics (example: response times)
                log_metrics = self._parse_log_metrics(last_lines, service_config.get("log_patterns", {}))
            
            except Exception as e:
                logger.error(f"Error analyzing log file {log_path}: {str(e)}")
        
        # Create ServiceMetrics object
        metrics = ServiceMetrics(
            hostname=self.hostname,
            timestamp=timestamp,
            service_name=service_name,
            status=status,
            pid=pid,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            uptime=uptime,
            port_status=port_status,
            error_count=error_count,
            log_metrics=log_metrics,
            response_time=response_time,
            custom_metrics={}
        )
        
        return metrics
    
    def _tail_file(self, file_path: str, n: int) -> List[str]:
        """Return the last n lines of a file"""
        lines = []
        
        try:
            with open(file_path, 'r') as f:
                # Simple implementation for small files
                lines = f.readlines()
                lines = lines[-n:]
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
        
        return lines
    
    def _parse_log_metrics(self, log_lines: List[str], patterns: Dict[str, str]) -> Dict[str, Any]:
        """Parse log lines to extract metrics based on patterns"""
        import re
        
        metrics = {}
        
        for metric_name, pattern in patterns.items():
            values = []
            
            for line in log_lines:
                match = re.search(pattern, line)
                if match:
                    try:
                        # Try to convert to float
                        value = float(match.group(1))
                        values.append(value)
                    except (ValueError, IndexError):
                        pass
            
            if values:
                metrics[metric_name] = {
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "count": len(values)
                }
        
        return metrics
    
    def _send_metrics_batch(self, metrics_batch: List[tuple]):
        """Send a batch of metrics to Redis"""
        if not metrics_batch:
            return
        
        logger.info(f"Sending batch of {len(metrics_batch)} metrics")
        
        # Group metrics by type
        system_metrics = []
        service_metrics = []
        
        for metrics_type, metrics in metrics_batch:
            if metrics_type == "system":
                system_metrics.append(asdict(metrics))
            elif metrics_type == "service":
                service_metrics.append(asdict(metrics))
        
        # Send system metrics
        if system_metrics:
            try:
                metrics_json = json.dumps({
                    "node_id": self.hostname,
                    "timestamp": time.time(),
                    "metrics_count": len(system_metrics),
                    "metrics": system_metrics
                })
                
                self.redis_client.publish("clustersentry:system_metrics", metrics_json)
                logger.debug(f"Sent {len(system_metrics)} system metrics")
            except Exception as e:
                logger.error(f"Error sending system metrics: {str(e)}")
        
        # Send service metrics
        if service_metrics:
            try:
                metrics_json = json.dumps({
                    "node_id": self.hostname,
                    "timestamp": time.time(),
                    "metrics_count": len(service_metrics),
                    "metrics": service_metrics
                })
                
                self.redis_client.publish("clustersentry:service_metrics", metrics_json)
                logger.debug(f"Sent {len(service_metrics)} service metrics")
            except Exception as e:
                logger.error(f"Error sending service metrics: {str(e)}")


class CustomMetricsCollector:
    """Base class for custom metrics collectors"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the custom collector"""
        self.config = config or {}
    
    def collect(self) -> Dict[str, Any]:
        """
        Collect custom metrics
        
        Returns:
            Dictionary of metric name to value
        """
        raise NotImplementedError("Subclasses must implement collect() method")


class GPUMetricsCollector(CustomMetricsCollector):
    """Collector for GPU metrics (NVIDIA)"""
    
    def collect(self) -> Dict[str, Any]:
        """Collect NVIDIA GPU metrics using nvidia-smi"""
        metrics = {}
        
        try:
            # Check if nvidia-smi is available
            result = subprocess.run(
                ["which", "nvidia-smi"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            if result.returncode != 0:
                # nvidia-smi not found
                return {"gpu_available": False}
            
            # Get GPU information
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=index,name,temperature.gpu,utilization.gpu,utilization.memory,memory.total,memory.used", "--format=csv,noheader,nounits"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if result.returncode != 0:
                # Error running nvidia-smi
                return {"gpu_available": False, "gpu_error": result.stderr.strip()}
            
            # Parse output
            gpu_metrics = []
            for line in result.stdout.strip().split('\n'):
                parts = [p.strip() for p in line.split(',')]
                if len(parts) >= 7:
                    gpu_metrics.append({
                        "index": int(parts[0]),
                        "name": parts[1],
                        "temperature": float(parts[2]),
                        "gpu_utilization": float(parts[3]),
                        "memory_utilization": float(parts[4]),
                        "memory_total": float(parts[5]),
                        "memory_used": float(parts[6])
                    })
            
            metrics = {
                "gpu_available": True,
                "gpu_count": len(gpu_metrics),
                "gpus": gpu_metrics
            }
            
            # Add summary metrics for overall GPU usage
            if gpu_metrics:
                metrics["gpu_utilization_avg"] = sum(gpu["gpu_utilization"] for gpu in gpu_metrics) / len(gpu_metrics)
                metrics["gpu_memory_utilization_avg"] = sum(gpu["memory_utilization"] for gpu in gpu_metrics) / len(gpu_metrics)
                metrics["gpu_temperature_avg"] = sum(gpu["temperature"] for gpu in gpu_metrics) / len(gpu_metrics)
        
        except Exception as e:
            metrics = {"gpu_available": False, "gpu_error": str(e)}
        
        return {"gpu": metrics}


class DockerMetricsCollector(CustomMetricsCollector):
    """Collector for Docker container metrics"""
    
    def collect(self) -> Dict[str, Any]:
        """Collect Docker container metrics"""
        metrics = {}
        
        try:
            # Check if docker is available
            result = subprocess.run(
                ["which", "docker"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            if result.returncode != 0:
                # docker not found
                return {"docker_available": False}
            
            # Get list of running containers
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.ID}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if result.returncode != 0:
                # Error running docker ps
                return {"docker_available": False, "docker_error": result.stderr.strip()}
            
            container_ids = result.stdout.strip().split('\n')
            container_ids = [cid for cid in container_ids if cid]
            
            # Get stats for each container
            container_metrics = []
            for container_id in container_ids:
                # Get container details
                result = subprocess.run(
                    ["docker", "inspect", container_id],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                if result.returncode != 0:
                    continue
                
                container_info = json.loads(result.stdout)[0]
                
                # Get container stats
                result = subprocess.run(
                    ["docker", "stats", container_id, "--no-stream", "--format", "{{.CPUPerc}}|{{.MemPerc}}|{{.MemUsage}}|{{.NetIO}}|{{.BlockIO}}"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                if result.returncode != 0:
                    continue
                
                stats_parts = result.stdout.strip().split('|')
                if len(stats_parts) >= 5:
                    # Parse stats
                    cpu_perc = float(stats_parts[0].strip('%'))
                    mem_perc = float(stats_parts[1].strip('%'))
                    
                    mem_usage_parts = stats_parts[2].strip().split('/')
                    mem_usage = mem_usage_parts[0].strip()
                    
                    # Parse memory usage value (e.g., "100MiB")
                    mem_value = 0
                    if mem_usage.endswith('GiB'):
                        mem_value = float(mem_usage[:-3]) * 1024
                    elif mem_usage.endswith('MiB'):
                        mem_value = float(mem_usage[:-3])
                    
                    container_metrics.append({
                        "id": container_id,
                        "name": container_info.get("Name", "").strip('/'),
                        "image": container_info.get("Config", {}).get("Image", ""),
                        "status": container_info.get("State", {}).get("Status", ""),
                        "cpu_percent": cpu_perc,
                        "memory_percent": mem_perc,
                        "memory_usage_mb": mem_value
                    })
            
            metrics = {
                "docker_available": True,
                "container_count": len(container_metrics),
                "containers": container_metrics
            }
            
            # Add summary metrics
            if container_metrics:
                metrics["container_cpu_avg"] = sum(c["cpu_percent"] for c in container_metrics) / len(container_metrics)
                metrics["container_memory_avg"] = sum(c["memory_percent"] for c in container_metrics) / len(container_metrics)
        
        except Exception as e:
            metrics = {"docker_available": False, "docker_error": str(e)}
        
        return {"docker": metrics}


class MetricsCollectorRegistry:
    """Registry for custom metrics collectors"""
    
    def __init__(self):
        """Initialize the registry"""
        self.collectors = {}
    
    def register(self, name: str, collector_class, config: Dict[str, Any] = None):
        """
        Register a custom metrics collector
        
        Args:
            name: Name for the collector
            collector_class: CustomMetricsCollector subclass
            config: Configuration for the collector
        """
        try:
            collector = collector_class(config)
            self.collectors[name] = collector
            logger.info(f"Registered custom metrics collector: {name}")
            return True
        except Exception as e:
            logger.error(f"Error registering collector {name}: {str(e)}")
            return False
    
    def unregister(self, name: str):
        """
        Unregister a custom metrics collector
        
        Args:
            name: Name of the collector to unregister
        """
        if name in self.collectors:
            del self.collectors[name]
            logger.info(f"Unregistered custom metrics collector: {name}")
            return True
        return False
    
    def get_collectors(self) -> List[CustomMetricsCollector]:
        """Get all registered collectors"""
        return list(self.collectors.values())


def create_default_config(output_path: str):
    """
    Create a default configuration file for the metrics collector
    
    Args:
        output_path: Path to write the configuration file
    """
    default_config = {
        "collect_interval": 60,  # Collect metrics every 60 seconds
        "send_interval": 300,    # Send metrics every 5 minutes
        "redis_host": "localhost",
        "redis_port": 6379,
        "redis_password": None,
        "services": [
            {
                "name": "nginx",
                "type": "systemd",
                "process_name": "nginx",
                "ports": [80, 443],
                "log_path": "/var/log/nginx/error.log",
                "log_patterns": {
                    "request_time": "request time: ([0-9.]+)",
                    "connections": "([0-9]+) connections"
                }
            },
            {
                "name": "postgresql",
                "type": "systemd",
                "process_name": "postgres",
                "ports": [5432],
                "log_path": "/var/log/postgresql/postgresql.log"
            }
        ],
        "custom_collectors": {
            "gpu": {
                "enabled": True
            },
            "docker": {
                "enabled": True
            }
        }
    }
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write configuration to file
    with open(output_path, 'w') as f:
        json.dump(default_config, f, indent=2)
    
    logger.info(f"Created default configuration at {output_path}")


# Entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Health Metrics Collector for ClusterSentry")
    parser.add_argument("--config", default="config/metrics_collector.json", help="Path to configuration file")
    parser.add_argument("--create-config", action="store_true", help="Create default configuration file")
    args = parser.parse_args()
    
    if args.create_config:
        create_default_config(args.config)
        exit(0)
    
    # Initialize collector
    collector = MetricsCollector(config_path=args.config)
    
    # Register custom collectors based on configuration
    registry = MetricsCollectorRegistry()
    
    custom_collectors_config = collector.config.get("custom_collectors", {})
    
    # GPU metrics
    if custom_collectors_config.get("gpu", {}).get("enabled", False):
        registry.register("gpu", GPUMetricsCollector, custom_collectors_config.get("gpu"))
    
    # Docker metrics
    if custom_collectors_config.get("docker", {}).get("enabled", False):
        registry.register("docker", DockerMetricsCollector, custom_collectors_config.get("docker"))
    
    # Add collectors to the metrics collector
    collector.custom_collectors = registry.get_collectors()
    
    try:
        # Start collection
        collector.start()
        
        # Keep running until interrupted
        print("Metrics collector running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("Stopping metrics collector...")
    
    finally:
        # Stop collection
        collector.stop()