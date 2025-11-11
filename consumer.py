import json
import os
import numpy as np
import tritonclient.grpc as grpcclient
from kafka import KafkaConsumer, KafkaProducer


class DataConsumer:
    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.input_topic = os.getenv("INPUT_TOPIC", "input_data")
        self.output_topic = os.getenv("OUTPUT_TOPIC", "output_data")
        self.triton_url = os.getenv("TRITON_URL", "localhost:8006")
        self.model_name = os.getenv("MODEL_NAME", "codegen")
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=[self.bootstrap_servers],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.triton_client = grpcclient.InferenceServerClient(url=self.triton_url)

    def process_with_triton(self, text: str) -> str:
        input_tokens = [ord(char) for char in text[:2500]]
        if len(input_tokens) < 2500:
            input_tokens.extend([0] * (2500 - len(input_tokens)))
        attention_mask = [0 if token == 0 else 1 for token in input_tokens]
        inference_inputs = [
            grpcclient.InferInput("input_ids", [1, 2500], "INT32"),
            grpcclient.InferInput("attention_mask", [1, 2500], "INT32"),
        ]
        inference_inputs[0].set_data_from_numpy(
            np.array([input_tokens], dtype=np.int32)
        )
        inference_inputs[1].set_data_from_numpy(
            np.array([attention_mask], dtype=np.int32)
        )
        result = self.triton_client.infer(
            self.model_name,
            inference_inputs,
            outputs=[grpcclient.InferRequestedOutput("output_0")],
        )
        output_array = result.as_numpy("output_0")
        codegen_tokens = [
            int(token) for token in output_array[0] if 0 < int(token) < 0x10FFFF
        ]
        codegen = "".join(chr(token) for token in codegen_tokens).strip()
        return codegen

    def process_messages(self) -> None:
        print("Starting to process messages... - consumer.py:56")
        for message in self.consumer:
            try:
                data = message.value
                text = data.get("text", "")
                metadata = data.get("metadata", {})
                print(f"Processing message from {metadata.get('source', 'unknown')}")
                codegen = self.process_with_triton(text)
                result = {
                    "original_text": text[:100] + "..." if len(text) > 100 else text,
                    "codegen_text": codegen,
                    "original_length": len(text),
                    "codegen_length": len(codegen),
                    "metadata": metadata,
                }
                self.producer.send(self.output_topic, value=result)
                print(
                    f"Processed and sent result for {metadata.get('source', 'unknown')}"
                )
            except Exception as e:
                print(f"Error processing message: {e} - consumer.py:78")


if __name__ == "__main__":
    consumer = DataConsumer()
    consumer.process_messages()
