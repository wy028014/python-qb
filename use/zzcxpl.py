from json import loads
from requests import Response, post
from utils.utils_xlsx import Xlsx
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from os import path
from tqdm import tqdm


class ZZCXPL:
    def __init__(self) -> None:
        self.db: MariaDB = MariaDB(host=239, database="情报", print=False)
        self.db.connect()
        self.dt: DateTime = DateTime()
        self.xlsx: Xlsx = Xlsx()

    def getZZCXData(self, data: object) -> list:
        """获取数字证书的数据信息

        :param data: 站站查询的请求参数
        :type data: object
        :return: 数据列表
        :rtype: list
        """
        url: str = f"http://10.3.32.60:2326/certificate/cyber/zzcx"
        headers: dict[str, str] = {
            f"accept": f"*/*",
            f"Content-Type": f"application/json",
        }
        response: Response = post(
            url, json=data, headers=headers, timeout=60 * 60)
        return loads(response.text)["data"]["data"]

    def getTXTData(self) -> list:
        data_list = []
        current_dir = path.dirname(path.abspath(__file__))
        file_path = path.join(current_dir, f"zzcxpl.txt")
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if line.strip():
                    lineList: list = line.strip().split(',')
                    if len(lineList) == 3:
                        data_list.append(
                            {"train_date": lineList[0], "board_train_code": lineList[1], "fromStation": lineList[2]})
                    elif len(lineList) == 4:
                        data_list.append(
                            {"train_date": lineList[0], "board_train_code": lineList[1], "fromStation": lineList[2], "toStation": lineList[3]})
        return data_list

    def run(self) -> None:
        rows = []
        postData_list = self.getTXTData()
        for postData in tqdm(postData_list, desc=f"站站查询数量", unit=f"条"):
            zzrows = self.getZZCXData(data=postData)
            if len(rows) != 0:
                rows.append([None] * 26)
            for zzrow in zzrows:
                rows.append((
                    zzrow["车次"],  # 车次
                    zzrow["乘车日期"],  # 乘车日期
                    zzrow["乘车时间"],  # 乘车时间
                    zzrow["发站"],  # 发站
                    zzrow["到站"],  # 到站
                    zzrow["车厢号"],  # 车厢号
                    zzrow["座位号"],  # 座位号
                    zzrow["席别"],  # 席别
                    zzrow["姓名"],  # 姓名
                    zzrow["证件编号"],  # 证件编号
                    zzrow["证件类型"],  # 证件类型
                    zzrow["售票处"],  # 售票处
                    zzrow["窗口"],  # 窗口
                    zzrow["操作员"],  # 操作员
                    zzrow["售票时间"],  # 售票时间
                    zzrow["票号"],  # 票号
                    zzrow["票价"],  # 票价
                    zzrow["票种"],  # 票种
                ))
        self.xlsx.write_xlsx(
            f".\站站批量\站站批量_{self.dt.get_now(format=f'%Y%m%d%H%M%S')}_{len(rows)}",
            rows=rows,
        )
