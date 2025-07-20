from ftplib import FTP
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
import os


class CleanMySQL:
    def __init__(self, print: bool = True) -> None:
        self.dt = DateTime()
        self.ftp = FTP(host="10.3.16.197", user="htwa", passwd="htwadata")
        self.mysql = MariaDB(238, "smzz", print)
        self.dir = r".\\data\\"
        self.update_list: list = []
        pass

    def getFile(self, ftp: FTP, file_name: str) -> bool:
        try:
            print(os.path.join(self.dir, file_name))
            with open(os.path.join(self.dir, file_name), "wb") as f:
                ftp.retrbinary("RETR " + file_name, f.write)
            return True
        except Exception as e:
            print("Error:", str(e))
            return False

    def checkUpdateData(self) -> bool:
        self.ftp.cwd("haerbin")
        file_list: list[str] = self.ftp.nlst()
        print(f"{self.dt.get_now()} | 共有 txt 文件: {len(file_list)} 个")
        if self.mysql.connect():
            for file_name in file_list:
                sql_query = f"""
SELECT *
FROM `updated_list`
WHERE updated_list_name = '{file_name}'
                """
                flag, result = self.mysql.query(sql_query)
                if len(result) == 0:
                    self.update_list.append(file_name)
        print(f"{self.dt.get_now()} | 需要更新的文件: {len(self.update_list)} 个")
        if len(self.update_list) == 0:
            return False
        return True

    def ftpINTOmysql(self) -> None:
        flag_update: bool = self.checkUpdateData()
        if flag_update:
            for file_name in self.update_list:
                flag: bool = self.getFile(self.ftp, file_name)
                if flag:
                    print(f"{self.dt.get_now()} | 下载成功: {file_name}")
                    lines_tuples: list = []
                    with open(os.path.join(self.dir, file_name), "rb") as fp:
                        lines: list[str] = fp.read().decode(
                            "utf-8").splitlines()
                        for line in lines:
                            line: list[str] = line.split(",")
                            line.append("#".join(line[2:8] + line[13:14]))
                            if len(line) == 2:
                                continue
                            lines_tuples.append(tuple(line[0:17]))

                    for i in range(0, len(lines_tuples), 500):
                        tmp_tuples: list = lines_tuples[i: i + 500]
                        sql_insert: str = f"""
                        INSERT INTO smz
                        (name, card_type, id_no,
                        train_date, start_time, train_no, start_station, end_station, carriage_no, seat_no, seat_type,
                        ticket_no, ticket_type, ticket_no_, ticket_price, sale_time, unique_string)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE updated_times = updated_times + 1
                        """
                        self.mysql.insert(sql_insert, tmp_tuples)
                    print(f"{self.dt.get_now()} | 插入成功: {file_name}")
                    sql_update: str = f"""
INSERT IGNORE INTO `updated_list` SET updated_list_name = {file_name}
                    """
                    self.mysql.update(sql_update)
            self.mysql.close()
            self.ftp.quit()
