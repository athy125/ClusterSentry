#!/bin/bash
# deploy_test_system.sh
# Deploys and starts the ClusterSentry system in the test environment

set -e

# Configuration
CONFIG_DIR="$(pwd)/test_env/config"
DATA_DIR="$(pwd)/test_env/data"
SSH_KEY_PATH="$(pwd)/test_env/ssh_key"
REDIS_PORT=6379
SENTINEL_PORT=50051
LOG_DIR="$(pwd)/test_env/logs/clustersentry"

# Banner
echo "====================================================="
echo "  ClusterSentry Test System Deployment"
echo "====================================================="

# Check if test environment exists
if [ ! -d "test_env" ]; then
    echo "Error: Test environment not found. Run setup_test_environment.sh first."
    exit 1
fi

# Create directories
mkdir -p "$DATA_DIR/redis"
mkdir -p "$DATA_DIR/topology"
mkdir -p "$LOG_DIR"

# Check if Redis is running
REDIS_CONTAINER="clustersentry-redis"
if docker ps -a --format '{{.Names}}' | grep -q "^$REDIS_CONTAINER$"; then
    echo "Redis container already exists, removing..."
    docker rm -f "$REDIS_CONTAINER" >/dev/null
fi

# Start Redis
echo "Starting Redis..."
docker run -d \
    --name "$REDIS_CONTAINER" \
    --network "clustersentry-test" \
    -p "$REDIS_PORT:6379" \
    -v "$DATA_DIR/redis:/data" \
    redis:6.2-alpine \
    redis-server --appendonly yes

echo "Redis started on port $REDIS_PORT"

# Build and start ClusterSentry components

# Check for source code
if [ ! -d "src" ]; then
    echo "Error: Source code not found. Please run this script from the project root."
    exit 1
fi

# Compile C++ components if needed
if [ ! -f "build/bin/health_monitor" ]; then
    echo "Building C++ components..."
    mkdir -p build
    cd build
    cmake ..
    make -j4
    cd ..
fi

# Check Python dependencies
echo "Checking Python dependencies..."
pip install -r requirements.txt

# Start the components
echo "Starting ClusterSentry components..."

# Start Sentinel
echo "Starting Sentinel..."
mkdir -p "$LOG_DIR/sentinel"
nohup python src/sentinel/sentinel.py \
    --config "$CONFIG_DIR/sentinel.json" \
    > "$LOG_DIR/sentinel/sentinel.log" 2>&1 &
SENTINEL_PID=$!
echo $SENTINEL_PID > "$LOG_DIR/sentinel.pid"
echo "Sentinel started with PID $SENTINEL_PID"

# Wait for Sentinel to initialize
sleep 2

# Start Recovery Orchestrator
echo "Starting Recovery Orchestrator..."
mkdir -p "$LOG_DIR/recovery"
export SSH_KEY_PATH
export SSH_USERNAME="root"
nohup python src/recovery/recovery_orchestrator.py \
    --config "$CONFIG_DIR/recovery-orchestrator.json" \
    > "$LOG_DIR/recovery/orchestrator.log" 2>&1 &
RECOVERY_PID=$!
echo $RECOVERY_PID > "$LOG_DIR/recovery.pid"
echo "Recovery Orchestrator started with PID $RECOVERY_PID"

# Start Topology Manager
echo "Starting Topology Manager..."
mkdir -p "$LOG_DIR/topology"
nohup python src/common/cluster_topology_manager.py \
    --config "$CONFIG_DIR/topology-manager.json" \
    > "$LOG_DIR/topology/manager.log" 2>&1 &
TOPOLOGY_PID=$!
echo $TOPOLOGY_PID > "$LOG_DIR/topology.pid"
echo "Topology Manager started with PID $TOPOLOGY_PID"

# Deploy Health Monitors to test nodes
echo "Deploying Health Monitors to test nodes..."
NODES=$(docker ps --format '{{.Names}}' | grep "csentrynode" | wc -l)

for ((i=1; i<=NODES; i++)); do
    NODE_NAME="csentrynode$i"
    NODE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NODE_NAME")
    echo "Deploying Health Monitor to $NODE_NAME ($NODE_IP)..."
    
    # Create directory for the node's logs
    mkdir -p "$LOG_DIR/$NODE_NAME"
    
    # Copy the Health Monitor binary and dependencies
    docker cp build/bin/health_monitor "$NODE_NAME:/usr/local/bin/"
    docker cp "$CONFIG_DIR/health-monitor-config.json" "$NODE_NAME:/etc/clustersentry/"
    
    # Start Health Monitor on the node
    docker exec -d "$NODE_NAME" bash -c "mkdir -p /etc/clustersentry /var/log/clustersentry && \
        /usr/local/bin/health_monitor \
        --config /etc/clustersentry/health-monitor-config.json \
        --node-id $NODE_NAME \
        --sentinel-address 172.18.0.1:$SENTINEL_PORT \
        > /var/log/clustersentry/health_monitor.log 2>&1"
    
    # Deploy and start Health Metrics Collector
    docker cp -r src "$NODE_NAME:/opt/clustersentry/"
    docker exec "$NODE_NAME" bash -c "python3 -m pip install redis psutil requests"
    docker exec -d "$NODE_NAME" bash -c "python3 /opt/clustersentry/src/monitoring/health_metrics_collector.py \
        --redis-host 172.18.0.1 \
        > /var/log/clustersentry/metrics_collector.log 2>&1"
    
    echo "Health monitoring deployed to $NODE_NAME"
done

# Create a utility script to check component status
cat > test_env/check_status.sh << EOF
#!/bin/bash
# Check status of ClusterSentry components

echo "====================================================="
echo "  ClusterSentry Test System Status"
echo "====================================================="

# Check if components are running
if [ -f "$LOG_DIR/sentinel.pid" ]; then
    PID=\$(cat "$LOG_DIR/sentinel.pid")
    if ps -p \$PID > /dev/null; then
        echo "✓ Sentinel is running (PID \$PID)"
    else
        echo "✗ Sentinel is not running"
    fi
else
    echo "✗ Sentinel is not running"
fi

if [ -f "$LOG_DIR/recovery.pid" ]; then
    PID=\$(cat "$LOG_DIR/recovery.pid")
    if ps -p \$PID > /dev/null; then
        echo "✓ Recovery Orchestrator is running (PID \$PID)"
    else
        echo "✗ Recovery Orchestrator is not running"
    fi
else
    echo "✗ Recovery Orchestrator is not running"
fi

if [ -f "$LOG_DIR/topology.pid" ]; then
    PID=\$(cat "$LOG_DIR/topology.pid")
    if ps -p \$PID > /dev/null; then
        echo "✓ Topology Manager is running (PID \$PID)"
    else
        echo "✗ Topology Manager is not running"
    fi
else
    echo "✗ Topology Manager is not running"
fi

echo ""
echo "Health Monitors on nodes:"
for NODE in \$(docker ps --format '{{.Names}}' | grep "csentrynode"); do
    if docker exec \$NODE pgrep health_monitor > /dev/null; then
        echo "✓ Health Monitor on \$NODE is running"
    else
        echo "✗ Health Monitor on \$NODE is not running"
    fi
    
    if docker exec \$NODE pgrep -f "health_metrics_collector.py" > /dev/null; then
        echo "✓ Metrics Collector on \$NODE is running"
    else
        echo "✗ Metrics Collector on \$NODE is not running"
    fi
done

echo ""
echo "Redis status:"
if docker ps | grep -q clustersentry-redis; then
    echo "✓ Redis is running"
else
    echo "✗ Redis is not running"
fi

echo "====================================================="
echo "Logs are available in $LOG_DIR"
echo "====================================================="
EOF

chmod +x test_env/check_status.sh

# Create a utility script to stop the system
cat > test_env/stop_system.sh << EOF
#!/bin/bash
# Stop the ClusterSentry test system

echo "Stopping ClusterSentry components..."

# Stop Sentinel
if [ -f "$LOG_DIR/sentinel.pid" ]; then
    PID=\$(cat "$LOG_DIR/sentinel.pid")
    if ps -p \$PID > /dev/null; then
        echo "Stopping Sentinel (PID \$PID)..."
        kill \$PID
    fi
    rm "$LOG_DIR/sentinel.pid"
fi

# Stop Recovery Orchestrator
if [ -f "$LOG_DIR/recovery.pid" ]; then
    PID=\$(cat "$LOG_DIR/recovery.pid")
    if ps -p \$PID > /dev/null; then
        echo "Stopping Recovery Orchestrator (PID \$PID)..."
        kill \$PID
    fi
    rm "$LOG_DIR/recovery.pid"
fi

# Stop Topology Manager
if [ -f "$LOG_DIR/topology.pid" ]; then
    PID=\$(cat "$LOG_DIR/topology.pid")
    if ps -p \$PID > /dev/null; then
        echo "Stopping Topology Manager (PID \$PID)..."
        kill \$PID
    fi
    rm "$LOG_DIR/topology.pid"
fi

# Stop Health Monitors on nodes
for NODE in \$(docker ps --format '{{.Names}}' | grep "csentrynode"); do
    echo "Stopping components on \$NODE..."
    docker exec \$NODE pkill health_monitor || true
    docker exec \$NODE pkill -f "health_metrics_collector.py" || true
done

# Stop Redis
if docker ps | grep -q clustersentry-redis; then
    echo "Stopping Redis..."
    docker stop clustersentry-redis
    docker rm clustersentry-redis
fi

echo "ClusterSentry system stopped"
EOF

chmod +x test_env/stop_system.sh

echo "====================================================="
echo "  ClusterSentry Test System Deployment Complete"
echo "====================================================="
echo ""
echo "All components started successfully"
echo ""
echo "Useful commands:"
echo "  ./test_env/check_status.sh - Check status of all components"
echo "  ./test_env/stop_system.sh - Stop all components"
echo "  ./test_env/generate_load.sh - Generate load on test nodes"
echo ""
echo "Component logs are in $LOG_DIR"
echo "====================================================="