from datetime import date
from reports.base_report import BaseReport


class InventoryReport(BaseReport):

    def generate(self) -> None:
        print(f"Generating inventory report for {self.report_date}")

        # Your actual report logic goes here.
        #
        # 1. Query database
        # 2. Transform data
        # 3. Generate CSV/PDF/etc.
        # 4. Save it
        # 5. Email it

