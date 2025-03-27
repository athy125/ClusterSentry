import unittest
import asyncio
import redis.asyncio as redis

class TestEvents(unittest.TestCase):
    def setUp(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.loop = asyncio.get_event_loop()

    def test_event_publish(self):
        async def publish():
            await self.redis.publish("node_events", "node1:MEMORY:100:0.1:10::")
            status = await self.redis.hget("node_status", "node1")
            self.assertEqual(status.decode(), "MEMORY:100")

        self.loop.run_until_complete(publish())

if __name__ == "__main__":
    unittest.main()