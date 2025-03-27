import asyncio
import tensorflow as tf
import numpy as np
import logging

async def analyze_root_cause(redis_client):
    """AI-driven root cause analysis using TensorFlow Lite."""
    interpreter = tf.lite.Interpreter(model_path="models/rca_model.tflite")
    interpreter.allocate_tensors()
    input_details = interpreter.get_input_details()
    output_details = interpreter.get_output_details()

    causes_map = {
        0: "Software bug (segfault); update app",
        1: "Memory leak; check allocations",
        2: "I/O bottleneck; optimize storage",
        3: "CPU overload; rebalance tasks"
    }

    while True:
        anomalies = await redis_client.hgetall("anomalies")
        if not anomalies:
            await asyncio.sleep(5)
            continue

        features, keys = [], []
        for key, value in anomalies.items():
            node_id, ts = key.decode().split(":")
            event, details = value.decode().split(":", 1) if ":" in value.decode() else (value.decode(), "")
            cpu = float(details.split(",")[0].split(":")[1]) if "CPU" in details else 0
            io = int(details.split(",")[1].split(":")[1]) if "IO" in details else 0
            mem = float((await redis_client.hget("node_status", node_id)).decode().split(":")[1])
            features.append([mem, cpu, io])
            keys.append(key.decode())

        if features:
            input_data = np.array(features, dtype=np.float32)
            interpreter.set_tensor(input_details[0]["index"], input_data)
            interpreter.invoke()
            predictions = interpreter.get_tensor(output_details[0]["index"])
            for key, pred in zip(keys, predictions.argmax(axis=1)):
                cause = causes_map.get(pred, "Unknown; manual review needed")
                if "STACK" in (await redis_client.hget("anomalies", key)).decode():
                    cause = "Crash detected; inspect stack trace"
                await redis_client.hset("root_causes", key, cause)
        await asyncio.sleep(5)