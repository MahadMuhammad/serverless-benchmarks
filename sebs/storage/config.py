from typing import cast, List

from dataclasses import dataclass, field

from sebs.cache import Cache
from sebs.faas.config import Resources


class MinioResources(Resources):
    def __init__(self):
        super().__init__(name="minio")

    @staticmethod
    def initialize(res: Resources, dct: dict):
        ret = cast(MinioResources, res)
        super(MinioResources, MinioResources).initialize(ret, dct)
        return ret

    def serialize(self) -> dict:
        return super().serialize()

    @staticmethod
    def deserialize(config: dict) -> "Resources":  # type: ignore

        ret = MinioResources()
        MinioResources.initialize(ret, {})
        return ret

    def update_cache(self, cache: Cache):
        super().update_cache(cache)


@dataclass
class MinioConfig:
    address: str = ""
    mapped_port: int = -1
    access_key: str = ""
    secret_key: str = ""
    instance_id: str = ""
    output_buckets: List[str] = field(default_factory=list)
    input_buckets: List[str] = field(default_factory=lambda: [])
    version: str = ""
    data_volume: str = ""
    type: str = "minio"

    def update_cache(self, path: List[str], cache: Cache):

        for key in MinioConfig.__dataclass_fields__.keys():
            if key == "resources":
                continue
            cache.update_config(val=getattr(self, key), keys=[*path, key])
        # self.resources.update_cache(cache)

    @staticmethod
    def deserialize(data: dict) -> "MinioConfig":
        keys = list(MinioConfig.__dataclass_fields__.keys())
        data = {k: v for k, v in data.items() if k in keys}

        cfg = MinioConfig(**data)

        return cfg

    def serialize(self) -> dict:
        return self.__dict__


@dataclass
class ScyllaDBConfig:
    address: str = ""
    mapped_port: int = -1
    access_key: str = "None"
    secret_key: str = "None"
    instance_id: str = ""
    region: str = "None"
    cpus: int = -1
    memory: int = -1
    version: str = ""
    data_volume: str = ""
    type: str = "nosql"

    def update_cache(self, path: List[str], cache: Cache):

        for key in ScyllaDBConfig.__dataclass_fields__.keys():
            cache.update_config(val=getattr(self, key), keys=[*path, key])

    @staticmethod
    def deserialize(data: dict) -> "ScyllaDBConfig":
        keys = list(ScyllaDBConfig.__dataclass_fields__.keys())
        data = {k: v for k, v in data.items() if k in keys}

        cfg = ScyllaDBConfig(**data)

        return cfg

    def serialize(self) -> dict:
        return self.__dict__
