import unittest
import asyncio
import redis.asyncio as redis
from ai_engine import analyze_root_cause

class TestAI(unittest.TestCase):
    def setUp(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.loop = asyncio.get_event_loop()

    def test_rca(self):
        async def test():
            await self.redis.hset("anomalies", "node1:1234567890", "MEMORY_EXCEEDED:CPU:5,IO:100")
            await self.redis.hset("node_status", "node1", "MEMORY_EXCEEDED:1000")
            await analyze_root_cause(self.redis)
            cause = await self.redis.hget("root_causes", "node1:1234567890")
            self.assertIsNotNone(cause)

        self.loop.run_until_complete(test())

if __name__ == "__main__":
    unittest.main()