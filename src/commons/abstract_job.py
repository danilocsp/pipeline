from abc import ABC, abstractmethod

# classes abstratas python
class AbstractJob (ABC):

    def __init__(self, fr_path: str, to_path: str):
        self.fr_path = fr_path
        self.to_path = to_path

    @abstractmethod
    def process(self) -> None:
        pass
