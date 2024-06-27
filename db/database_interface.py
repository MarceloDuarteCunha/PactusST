from abc import ABC, abstractmethod

class DatabaseInterface(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def get_credential():
        pass

    @abstractmethod
    def get_client(self):
        pass

    @abstractmethod
    def data_load(self):
        pass
