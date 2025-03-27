import asyncio
import logging

async def recover_node(redis_client, node_id):
    """Recover a failed node by restarting its service."""
    logging.info(f"Recovering node {node_id}")
    process = await asyncio.create_subprocess_exec("systemctl", "restart", f"compute-node@{node_id}.service")
    await process.wait()
    await redis_client.hset("node_status", node_id, "RECOVERING")