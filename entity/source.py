from abc import ABC, abstractmethod
from typing import Dict,List
class Source(ABC):
    @abstractmethod
    def pulling_data(self, *args, **kwargs) -> List[Dict[str,str]]:
        raise NotImplementedError

    def make_to_dynamodb(self) -> List[Dict[str, Dict]]:
        raise NotImplementedError
