from json import loads
from requests import Response, post
from utils.utils_xlsx import Xlsx
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from os import path
from tqdm import tqdm

class TXRPL:
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

    def getTXTData(self, date_start: str = f"2021-01-01", date_end: str = f"2025-12-31") -> list:
        data_list = []
        current_dir = path.dirname(path.abspath(__file__))
        file_path = path.join(current_dir, f"txrpl.txt")
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if line.strip():
                    data_list.append(
                        {"date_start": date_start, "date_end": date_end, "id_no": line.strip()})
        return data_list

    def run(self) -> None:
        rows_txr: list = []
        TXTData = self.getTXTData()
        gjrows = self.getGJCXData(data=TXTData)
        for gjrow in tqdm(gjrows, desc=f"轨迹数量", unit=f"条"):
            zzrows = self.getZZCXData({
                "train_date": gjrow["乘车日期"],
                "board_train_code": gjrow["车次"],
                "fromStation": gjrow["发站"],
            })
            print(f'站站轨迹 {len(zzrows)} 条')
            spsj = None
            for zzrow in zzrows:
                if zzrow["证件编号"] == gjrow["证件编号"]:
                    spsj = zzrow["售票时间"]
                    if len(rows_txr) != 0:
                        rows_txr.append([None] * 26)
                    rows_txr.append((
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
                    break
            for zzrow in zzrows:
                if zzrow["售票时间"] == spsj:
                    if zzrow["证件编号"] != gjrow["证件编号"]:
                        rows_txr.append((
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
                # sql_query: str = f"""
                # SELECT id
                # FROM `乘车数据_数据表`
                # WHERE `乘车日期` = '{zzrow['乘车日期']}'
                # AND `车次` = '{zzrow['车次']}'
                # AND `发站` = '{zzrow['发站']}'
                # AND `到站` = '{zzrow['到站']}'
                # AND `证件编号` = '{zzrow['证件编号']}'
                # AND `车厢号` = '{zzrow['车厢号']}'
                # AND `座位号` = '{zzrow['座位号']}'
                # AND `售票处` IS NULL
                # """
                # flag_query, res_query = self.db.query(sql=sql_query)
                # if not flag_query:
                #     return
                # sql_update: str = (
                #     f"""
                #     UPDATE `乘车数据_数据表`
                #     SET
                #     `票价` = '{zzrow['票价']}',
                #     `售票处` = '{zzrow['售票处']}',
                #     `窗口` = '{zzrow['窗口']}',
                #     `操作员编号` = '{zzrow['操作员']}',
                #     `售票时间` = '{zzrow['售票时间'].replace('/', '-').replace('.000', '')}',
                #     `更新时间` = '{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}'
                #     WHERE id = {res_query[0][0]};
                #     """
                #     if len(res_query) > 0
                #     else f"""
                #         INSERT INTO `乘车数据_数据表`
                #         (`车次`,`乘车日期`,`乘车时间`,`发站`,`到站`,`车厢号`,`座位号`,`席别`,`姓名`,`证件编号`,`证件类型`,`售票处`,`窗口`,`操作员编号`,`售票时间`,`票号`,`票价`,`票种`,`出行状态`,`MongoDB`,`创建时间`,`更新时间`,`删除时间`)
                #         VALUES ('{zzrow['车次']}','{zzrow['乘车日期']}','{zzrow['乘车时间']}','{zzrow['发站']}','{zzrow['到站']}','{zzrow['车厢号']}','{zzrow['座位号']}','{zzrow['席别']}','{zzrow['姓名']}','{zzrow['证件编号']}','{zzrow['证件类型']}','{zzrow['售票处']}','{zzrow['窗口']}','{zzrow['操作员']}','{zzrow['售票时间'].replace('/', '-').replace('.000', '')}','{zzrow['票号']}','{zzrow['票价']}','{zzrow['票种']}','出行', 0,'{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}','{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}',NULL);
                #         """
                # )
                # flag_update = self.db.update(sql=sql_update)
                # if not flag_update:
                #     return                
        self.xlsx.write_xlsx(
            f".\同行人批量\同行人批量_{self.dt.get_now(format=f'%Y%m%d%H%M%S')}_{len(rows_txr)}",
            rows=rows_txr,
        )
