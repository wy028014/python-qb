from datetime import datetime


class Parse:
    def __init__(self) -> None:
        pass

    def get_age(self, id_num: str) -> int:
        """根据二代身份证号码获取年龄

        :param id_num: 二代身份证号码
        :type id_num: str
        :return: 年龄
        :rtype: str
        """
        birth_year: int = int(id_num[6:10])
        birth_month: int = int(id_num[10:12])
        birth_day: int = int(id_num[12:14])
        birth_date = datetime(birth_year, birth_month, birth_day).date()
        today = datetime.now().date()
        age: int = today.year - birth_date.year
        if (today.month, today.day) < (birth_date.month, birth_date.day):
            age -= 1
        return age

    def get_gender(self, id_num: str) -> str:
        """根据二代身份证号码获取性别

        :param id_num: 二代身份证号码
        :type id_num: str
        :return: "男" | "女"
        :rtype: str
        """
        return "男" if int(id_num[-2]) % 2 else "女"
