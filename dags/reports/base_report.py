from abc import ABC, abstractmethod
from datetime import date


class BaseReport(ABC):
    """
    Base class for all reports.

    Every report receives a report date and implements
    its own generation logic.
    """

    def __init__(self, report_date: date):
        self.report_date = report_date

    @abstractmethod
    def generate(self) -> None:
        """Generate the report."""
        raise NotImplementedError

    def run(self) -> None:
        print(
            f"Starting {self.__class__.__name__} "
            f"for {self.report_date}"
        )

        self.generate()

        print(
            f"Completed {self.__class__.__name__} "
            f"for {self.report_date}"
        )

