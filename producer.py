import json
import time
import os
from typing import Dict, List, Union
from kafka import KafkaProducer


class DataProducer:
    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.input_topic = os.getenv("INPUT_TOPIC", "input_data")
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
        )

    def generate_sample_data(self) -> List[Dict[str, Union[str, Dict[str, str]]]]:
        return [
            {
                "text": "Write a Python function to calculate the factorial of a number using recursion",
                "metadata": {"source": "code_prompt1", "length": "72"},
            },
            {
                "text": "Create a JavaScript function that validates an email address using regular expressions",
                "metadata": {"source": "code_prompt2", "length": "85"},
            },
            {
                "text": "Implement a Java class for a simple bank account with deposit, withdraw and getBalance methods",
                "metadata": {"source": "code_prompt3", "length": "92"},
            },
            {
                "text": "Write a Python function to sort a list of dictionaries by a specific key",
                "metadata": {"source": "code_prompt4", "length": "67"},
            },
            {
                "text": "Create a React component that displays a counter with increment and decrement buttons",
                "metadata": {"source": "code_prompt5", "length": "82"},
            },
        ]

    def send_data(self) -> None:
        samples = self.generate_sample_data()
        for i, sample in enumerate(samples):
            try:
                future = self.producer.send(self.input_topic, value=sample)
                future.get(timeout=10)
                print(f"Sent sample {i + 1}: {sample['metadata']} - producer.py:48")
                time.sleep(2)
            except Exception as e:
                print(f"Failed to send sample {i + 1}: {e} - producer.py:51")
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    producer = DataProducer()
    producer.send_data()
