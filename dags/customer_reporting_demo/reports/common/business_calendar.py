from datetime import date
import holidays


class BusinessCalendar:
    """
    Determines whether a date is a business day.

    Business days are Monday-Friday excluding holidays.
    """

    def __init__(self, country="US"):
        self.holidays = holidays.country_holidays(country)

    def is_business_day(self, check_date: date) -> bool:
        # Monday = 0, Sunday = 6
        if check_date.weekday() >= 5:
            return False

        if check_date in self.holidays:
            return False

        return True

