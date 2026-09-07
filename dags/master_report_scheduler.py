from datetime import datetime
from airflow.sdk import DAG, task, task_group
from reports.customers.customers_report import CustomersReport
from reports.inventory.inventory_report import InventoryReport
from reports.sales.sales_report import SalesReport
from reports.common.business_calendar import BusinessCalendar

with DAG(
    dag_id="master_report_scheduler",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["reports"],
) as dag:

    @task
    def check_business_day():

        report_date = datetime.now().date()
        calendar = BusinessCalendar()

        if not calendar.is_business_day(report_date):
            print(
                f"{report_date} is not a business day. "
                "Reports will not run."
            )
            return False

        print(
            f"{report_date} is a business day. "
            "Reports will run."
        )

        return True

    @task_group
    def run_customers_report():
        report = CustomersReport(
            report_date=datetime.now().date()
        )

        report.run()

    @task_group
    def run_inventory_report():
        report = InventoryReport(
            report_date=datetime.now().date()
        )

        report.run()

    @task_group
    def run_sales_report():
        report = SalesReport(
            report_date=datetime.now().date()
        )

        report.run()

    business_day = check_business_day()
    customers = run_customers_report()
    inventory = run_inventory_report()
    sales = run_sales_report()

    business_day >> [customers, inventory, sales]