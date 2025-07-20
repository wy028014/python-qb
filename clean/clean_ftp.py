from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime as DT
from typing import Any, List, Tuple
from ftp.ftp_htwa import Ftp
from tqdm import tqdm
from datetime import date, time, datetime
from os import path

class CleanFTP:
    def __init__(self, print: bool = True) -> None:
        self.dt: DateTime = DT()
        self.print: bool = print
        self.ftp: Ftp = Ftp()
        self.mariadb: MariaDB = MariaDB(239, "情报", print=self.print)
        pass

    def checkUpdateData(self) -> list or bool:
        result_list: List[str] = []
        flag_ftp: bool = self.ftp.connect()
        flag_mariadb: bool = self.mariadb.connect()
        if flag_ftp and flag_mariadb:
            file_list: list or bool = self.ftp.get_file_list()
            if file_list:
                for file_name in file_list:
                    sql_updated: str = f"""
                    SELECT `id`
                    FROM `情报`.`ftp数据_更新表`
                    WHERE `更新文件名` = '{file_name}';
                    """
                    flag_query, result = self.mariadb.query(sql_updated)
                    if flag_query and len(result) == 0:
                        result_list.append(file_name)
        if len(result_list) != 0:
            print(f"{self.dt.get_now()} | 数据需要更新")
            return result_list
        else:
            print(f"{self.dt.get_now()} | 数据已是最新")
            return False

    def ftpINTOmariadb(self) -> None:
        file_list: list or bool = self.checkUpdateData()
        if file_list:
            for file_name in file_list:
                flag_down: bool = self.ftp.download(file_name)
                if flag_down:
                        file_path = path.join(f"C:\Personal\Projects\FTP数据", file_name)
                        flag_insert: bool = True
                        with open(file_path, 'r', encoding="utf-8") as file:
                            data_list: list = []
                            for line in file:
                                line = line.strip()
                                if line:
                                    elements: list = line.split(',')
                                    elements[3] = f"{elements[3][:4]}-{elements[3][4:6]}-{elements[3][6:8]}"
                                    elements[4] = f"{elements[4][:2]}:{elements[4][2:4]}:00"
                                    elements[15] = elements[15].replace('/', '-')[:19]
                                    elements[16] = int(elements[16][:3]) + int(elements[16][3:]) / 10.0
                                    data_list.append(elements)
                            batch_size = 1000
                            sql_insert: str = """
                                INSERT IGNORE INTO `情报`.`ftp数据_数据表` (
                                `姓名`, `证件类型`, `证件编号`,
                                `乘车日期`, `乘车时间`,  `车次`, `发站`, `到站`, `车厢号`, `座位号`, `席别`, 
                                `票号`, `票种`, `售票处`, `窗口`, `操作员编号`, `售票时间`, `票价`,  `出行状态`, `MongoDB`
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, '购票', 0)
                            """
                            total_batches = (len(data_list) + batch_size - 1) // batch_size
                            for i in tqdm(range(0, len(data_list), batch_size), total=total_batches, desc=f"插入数据 {len(data_list)} 条", unit=f"千条"):
                                batch_data = data_list[i: i+batch_size]
                                flag_insert = self.mariadb.insert(sql=sql_insert, data=batch_data)
                                if not flag_insert:
                                    continue
                        if flag_insert:
                            sql_updated: str = f"""
                            INSERT INTO `情报`.`ftp数据_更新表` (`更新文件名`, `更新数量`, `创建时间`, `更新时间`, `删除时间`)
                            VALUES ('{file_name}', {len(data_list)}, '{self.dt.get_now()}', '{self.dt.get_now()}', NULL)
                            """
                            self.mariadb.update(sql_updated)
