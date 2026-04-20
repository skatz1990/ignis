from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from ignis.parser.models import Application


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class Finding:
    rule: str
    severity: Severity
    stage_id: int
    stage_name: str
    message: str
    recommendation: str


class Rule(ABC):
    @abstractmethod
    def analyze(self, app: Application) -> list[Finding]: ...
