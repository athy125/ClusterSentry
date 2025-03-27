import asyncio
import redis.asyncio as redis
from aiohttp import web
from recovery import recover_node
from ai_engine import analyze_root_cause
import logging

logging.basicConfig(level=logging.INFO)

class Coordinator:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.pubsub = self.redis.pubsub()

    async def process_events(self):
        await self.pubsub.subscribe("node_events")
        async for message in self.pubsub.listen():
            if message["type"] != "message":
                continue
            data = message["data"].decode().split(":", 6)
            event = {
                "node_id": data[0], "event": data[1], "memory": float(data[2]),
                "cpu": float(data[3]), "io": int(data[4]), "is_anomaly": "ANOMALY" in data[5],
                "details": data[6] if len(data) > 6 else ""
            }
            await self.redis.hset("node_status", event["node_id"], f"{event['event']}:{event['memory']}")
            if event["is_anomaly"] or event["event"] in ["NODE_CRASH", "MEMORY_EXCEEDED"]:
                await self.redis.hset("anomalies", f"{event['node_id']}:{int(time.time())}", 
                                     f"{event['event']}:{event['details']}")
                if event["event"] in ["NODE_CRASH", "MEMORY_EXCEEDED"]:
                    asyncio.create_task(recover_node(self.redis, event["node_id"]))

    async def websocket_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        while True:
            status = await self.redis.hgetall("node_status")
            anomalies = await self.redis.hgetall("anomalies")
            causes = await self.redis.hgetall("root_causes")
            await ws.send_json({
                "status": {k.decode(): v.decode() for k in status},
                "anomalies": {k.decode(): v.decode() for k in anomalies},
                "causes": {k.decode(): v.decode() for k in causes}
            })
            await asyncio.sleep(1)
        return ws

async def index_handler(request):
    return web.FileResponse("../ui/public/index.html")

app = web.Application()
coord = Coordinator()
app.router.add_get('/', index_handler)
app.router.add_get('/ws', coord.websocket_handler)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(coord.process_events())
    loop.create_task(analyze_root_cause(coord.redis))
    loop.run_until_complete(web.run_app(app, host="0.0.0.0", port=5000))