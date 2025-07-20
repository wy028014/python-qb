from datetime import datetime, timedelta
from random import uniform
from time import sleep
from typing import List


class DateTime:
    def __init__(self) -> None:
        pass

    def get_now(self, format: str = "%Y-%m-%d %H:%M:%S.%f") -> str:
        """获取当前日期和时间

        :param format: 返回值的格式, defaults to "%Y-%m-%d %H:%M:%S.%f"
        :type format: str, optional
        :return: 当前日期和时间
        :rtype: str
        """
        return datetime.now().strftime(format)

    def get_today(self, format: str = "%Y-%m-%d") -> str:
        """获取当前日期

        :param format: 返回值的格式, defaults to "%Y-%m-%d"
        :type format: str, optional
        :return: 当前日期
        :rtype: str
        """
        return datetime.now().strftime(format)

    def get_yesterday(self, date: str = None, format: str = "%Y-%m-%d") -> str:
        """获取某天的前一天日期

        :param date: 某天的日期
        :type date: str
        :param format: 返回值的格式, defaults to "%Y-%m-%d"
        :type format: str, optional
        :return: 某天的前一天日期
        :rtype: str
        """
        date = datetime.now() if date is None else datetime.strptime(date, format)
        return (date - timedelta(days=1)).strftime(format)

    def get_tomorrow(self, date: str = None, format: str = "%Y-%m-%d") -> str:
        """获取某天的后一天日期

        :param date: 某天的日期
        :type date: str
        :param format: 返回值的格式, defaults to "%Y-%m-%d"
        :type format: str, optional
        :return: 某天的后一天日期
        :rtype: str
        """
        date = datetime.now() if date is None else datetime.strptime(date, format)
        tomorrow =  (date + timedelta(days=1)).strftime(format)
        print(type (tomorrow), tomorrow)
        return tomorrow

    def format_mysql_date(self, date: str, format: str = "%Y%m%d") -> str:
        """格式化mysql数据中日期字符串

        :param date: mysql数据中日期字符串
        :type date: str
        :param format: 返回值的格式, defaults to "%Y%m%d"
        :type format: str, optional
        :return: 以format格式返回mysql数据中日期字符串
        :rtype: str
        """
        return datetime.strptime(date, format).strftime("%Y-%m-%d")

    def format_mysql_time(self, time: str, format: str = "%H%M") -> str:
        """格式化mysql数据中时间字符串

        :param time: mysql数据中时间字符串
        :type time: str
        :param format: 返回值的格式, defaults to "%H%M"
        :type format: str, optional
        :return: 以format格式返回mysql数据中时间字符串
        :rtype: str
        """
        return datetime.strptime(time, format).strftime("%H:%M:%S")

    def format_mysql_datetime(
        self, dt: str, format: str = "%Y/%m/%d %H:%M:%S.%f"
    ) -> str:
        """格式化mysql数据中日期时间字符串

        :param dt: mysql数据中日期时间字符串
        :type dt: str
        :param format: 返回值的格式, defaults to "%Y/%m/%d %H:%M:%S.%f"
        :type format: _type_, optional
        :return: 以format格式返回mysql数据中日期时间字符串
        :rtype: str
        """
        return datetime.strptime(dt, format).strftime("%Y-%m-%d %H:%M:%S")

    def get_dates_between(
        self, date_start: str, date_end: str, format: str = "%Y-%m-%d"
    ) -> List[str]:
        """获取两个日期之间的所有日期

        :param start_date: 开始日期
        :type start_date: str
        :param end_date: 结束日期,
        :type end_date: str
        :param format: 返回值的格式, defaults to "%Y-%m-%d"
        :type format: str, optional
        :return: 两个日期之间的所有日期
        :rtype: List[str]
        """
        date_start = datetime.strptime(date_start, format)
        date_end = datetime.strptime(date_end, format) if date_end else datetime.today()
        dates = []
        while date_start <= date_end:
            dates.append(date_start.strftime(f"%Y-%m-%d"))
            date_start += timedelta(days=1)
        return dates

    def sleep_random(self, start: int, end: int) -> None:
        delay: float = uniform(start, end)
        sleep(delay / 1000.0)
