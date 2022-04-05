from abc import ABC
from abc import abstractmethod
from typing import Iterator, Optional, List, Callable
from enum import Enum
import json


class State(ABC):

    def __init__(self, name: str):
        self.name = name

    @staticmethod
    def deserialize(name: str, payload: dict) -> "State":
        cls = _STATE_TYPES[payload["type"]]
        return cls.deserialize(name, payload)


class Task(State):

    def __init__(self,
                 name: str,
                 func_name: str,
                 next: Optional[str]):
        self.name = name
        self.func_name = func_name
        self.next = next

    @classmethod
    def deserialize(cls, name: str, payload: dict) -> "Task":
        return cls(
            name=name,
            func_name=payload["func_name"],
            next=payload.get("next")
        )


class Switch(State):

    class Case:
        def __init__(self,
                     var: str,
                     op: str,
                     val: str,
                     next: str):
            self.var = var
            self.op = op
            self.val = val
            self.next = next

        @staticmethod
        def deserialize(payload: dict) -> "Case":
            return Switch.Case(
                var=payload["var"],
                op=payload["op"],
                val=payload.get("val"),
                next=payload.get("next")
            )

    def __init__(self, name: str, cases: List[Case], default: Optional[str]):
        self.name = name
        self.cases = cases
        self.default = default

    @classmethod
    def deserialize(cls, name: str, payload: dict) -> "Switch":
        cases = [Switch.Case.deserialize(c) for c in payload["cases"]]

        return cls(
            name=name,
            cases=cases,
            default=payload["default"]
        )


_STATE_TYPES = {
    "task": Task,
    "switch": Switch
}


class Generator(ABC):

    def __init__(self, export_func: Callable[[dict], str] = json.dumps):
        self._export_func = export_func

    def parse(self, path: str):
        with open(path) as f:
            definition = json.load(f)

        self.states = {n: State.deserialize(n, s)
                       for n, s in definition["states"].items()}
        self.root = self.states[definition["root"]]

    def generate(self) -> str:
        states = self.states.values()
        payloads = [self.encode_state(s) for s in states]
        definition = self.postprocess(states, payloads)

        return self._export_func(definition)

    def postprocess(self, states: List[State], payloads: List[dict]) -> dict:
        return {s.name: p for (s, p) in zip(states, payloads)}

    def encode_state(self, state: State) -> dict:
        if isinstance(state, Task):
            return self.encode_task(state)
        elif isinstance(state, Switch):
            return self.encode_switch(state)

    @abstractmethod
    def encode_task(self, state: Task) -> dict:
        pass

    @abstractmethod
    def encode_switch(self, state: Switch) -> dict:
        pass