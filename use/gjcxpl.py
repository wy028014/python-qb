from json import loads
from requests import Response, post
from utils.utils_xlsx import Xlsx
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from os import path
from tqdm import tqdm


class GJCXPL:
    def __init__(self) -> None:
        self.db: MariaDB = MariaDB(host=239, database="情报", print=False)
        self.db.connect()
        self.dt: DateTime = DateTime()
        self.xlsx: Xlsx = Xlsx()

    def getGJCXData(self, data: object) -> list:
        """获取数字证书的数据信息

        :param data: 轨迹查询的请求参数
        :type data: object
        :return: 数据列表
        :rtype: list
        """
        url: str = f"http://10.3.32.60:2326/certificate/cyber/gjcxpl"
        headers: dict[str, str] = {
            f"accept": f"*/*",
            f"Content-Type": f"application/json",
        }
        response: Response = post(
            url, json=data, headers=headers, timeout=60 * 60)
        return loads(response.text)["data"]["data"]

    def getTXTData(self, date_start: str = f"2023-01-01", date_end: str = f"2025-05-28") -> list:
        data_list = []
        current_dir = path.dirname(path.abspath(__file__))
        file_path = path.join(current_dir, f"gjcxpl.txt")
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in tqdm(file, desc=f"数量", unit=f"条"):
                if line.strip():
                    data_list.append(
                        {"date_start": date_start, "date_end": date_end, "id_no": line.strip()})
        return data_list

    def run(self) -> None:
        TXTData = self.getTXTData()
        gjrows = self.getGJCXData(data=TXTData)
        print(len(gjrows))
        self.xlsx.write_xlsx(
            f".\轨迹批量\轨迹批量_{self.dt.get_now(format=f'%Y%m%d%H%M%S')}_{len(gjrows)}",
            rows=gjrows,
            head=[
                "姓名",
                "证件类型",
                "证件编号",
                "乘车日期",
                "乘车时间",
                "车次",
                "发站",
                "到站",
                "车厢号",
                "席别",
                "座位号",
                "票价",
            ]
        )
