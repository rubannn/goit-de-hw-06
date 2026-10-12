import json
from typing import Any, List, Tuple

from kafka.serializer import Deserializer, Serializer


class JSONSerializer(Serializer):
    def serialize(self, topic: str, headers: List[Tuple[str, bytes]], data: Any) -> bytes:
        return json.dumps(data).encode("utf-8")


class JSONDeserializer(Deserializer):
    def deserialize(self, topic: str, headers: List[Tuple[str, bytes]], data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class StringSerializer(Serializer):
    def serialize(self, topic: str, headers: List[Tuple[str, bytes]], data: Any) -> bytes:
        return str(data).encode("utf-8")
