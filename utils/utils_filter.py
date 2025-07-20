from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from utils.utils_parse import Parse
from tqdm import tqdm


class Filter:
    def __init__(self) -> None:
        self.dt: DateTime = DateTime()
        self.mariadb: MariaDB = MariaDB(239, f"情报", print=False)
        self.mariadb.connect()
        pass

    def filter_adjacent(self, rows: list) -> list:
        """筛选相邻人数据
        :param rows: 需要筛选相邻人的数据
        :type rows: list
        :return: 相邻人数据
        :rtype: list
        """
        rows_adjacent: list = []
        results_list: list = []
        for row in tqdm(rows, desc=f"筛选相邻人数据", unit=f"条"):
            row: list = list(row)
            sql_query = f"""
                SELECT `车次`, DATE_FORMAT(`乘车日期`, '%Y-%m-%d'), DATE_FORMAT(`乘车时间`, '%H:%i:%s'),
                `发站`, `到站`,
                `车厢号`, `座位号`, `席别`,
                `姓名`, `证件编号`, `证件类型`,
                `售票处`, `窗口`, `操作员编号`, DATE_FORMAT(`售票时间`, '%Y-%m-%d %H:%i:%s'),
                `票号`, `票价`, `票种`
                FROM `情报`.`ftp_data`
                WHERE `乘车日期` = '{row[1]}'
                AND `车次` = '{row[0]}'
                AND ABS(TIMESTAMPDIFF(MINUTE, `售票时间`, '{row[14]}')) < 10
                """
            flag_query, results = self.mariadb.query(sql=sql_query)
            if flag_query and len(results) > 0 and results not in results_list:
                if len(rows_adjacent) != 0:
                    rows_adjacent.append(("",) * 18)
                rows_adjacent = rows_adjacent + results
                results_list.append(results)
            else:
                rows_adjacent = rows_adjacent + row
        print(
            f"{self.dt.get_now()} | 筛选完毕, 共有 相邻人数据 {len(rows_adjacent)} 条:"
        )
        return rows_adjacent

    def filter_companion(self, rows: list) -> list:
        """筛选同行人数据
        :param rows: 需要筛选同行人的数据
        :type rows: list
        :return: 同行人数据
        :rtype: list
        """
        rows_companion: list = []
        results_list: list = []
        for row in tqdm(rows, desc=f"筛选同行人数据", unit=f"条"):
            row: list = list(row)
            if row in rows_companion:
                continue
            sql_query: str = f"""
                SELECT `车次`, DATE_FORMAT(`乘车日期`, '%Y-%m-%d'), DATE_FORMAT(`乘车时间`, '%H:%i:%s'),
                `发站`, `到站`,
                `车厢号`, `座位号`, `席别`,
                `姓名`, `证件编号`, `证件类型`,
                `售票处`, `窗口`, `操作员编号`, DATE_FORMAT(`售票时间`, '%Y-%m-%d %H:%i:%s'),
                `票号`, `票价`, `票种`
                FROM `情报`.`ftp_data`
                WHERE `乘车日期` = '{row[1]}'
                AND `车次` = '{row[0]}'
                AND `发站` = '{row[3]}'
                AND `到站` = '{row[4]}'
                AND `售票时间` = '{row[14]}'
                """
            flag_query, results = self.mariadb.query(sql=sql_query)
            if flag_query and len(results) > 0 and results not in results_list:
                if len(rows_companion) != 0:
                    rows_companion.append(("",) * 18)
                rows_companion = rows_companion + results
                results_list.append(results)
        print(
            f"{self.dt.get_now()} | 筛选完毕, 共有 同行人数据 {len(rows_companion)} 条:"
        )
        return rows_companion

    def filter_companion_minor(self, rows: list) -> list:
        """筛选未成年同行人数据
        :param rows: 需要筛选同行人的未成年数据
        :type rows: list
        :return: 未成年的同行人数据
        :rtype: list
        """
        parse = Parse()
        rows_companion: list = []
        results_list: list = []
        for row in tqdm(rows, desc=f"筛选未成年同行人数据", unit=f"条"):
            flag_father_tegether = False
            row: list = list(row)
            if row in rows_companion:
                continue
            sql_query: str = f"""
                SELECT
                `车次`,
                DATE_FORMAT( `乘车日期`, '%Y-%m-%d' ),
                DATE_FORMAT( `乘车时间`, '%H:%i:%s' ),
                `发站`,
                `到站`,
                `车厢号`,
                `座位号`,
                `席别`,
                `姓名`,
                `证件编号`,
                `证件类型`,
                `售票处`,
                `窗口`,
                `操作员编号`,
                DATE_FORMAT( `售票时间`, '%Y-%m-%d %H:%i:%s' ),
                `票号`,
                `票价`,
                `票种` 
                FROM
                `情报`.`ftp_data` 
                WHERE
                `乘车日期` = '{row[1]}' 
                AND `车次` = '{row[0]}' 
                AND `发站` = '{row[3]}' 
                AND `到站` = '{row[4]}' 
                AND `售票时间` = '{row[14]}' UNION
                SELECT
                `车次`,
                DATE_FORMAT( `乘车日期`, '%Y-%m-%d' ),
                DATE_FORMAT( `乘车时间`, '%H:%i:%s' ),
                `发站`,
                `到站`,
                `车厢号`,
                `座位号`,
                `席别`,
                `姓名`,
                `证件编号`,
                `证件类型`,
                `售票处`,
                `窗口`,
                `操作员编号`,
                DATE_FORMAT( `售票时间`, '%Y-%m-%d %H:%i:%s' ),
                `票号`,
                `票价`,
                `票种` 
                FROM
                `情报`.`ca_data` 
                WHERE
                `乘车日期` = '{row[1]}' 
                AND `车次` = '{row[0]}' 
                AND `发站` = '{row[3]}' 
                AND `到站` = '{row[4]}' 
                AND `售票时间` = '{row[14]}'
                """
            flag_query, results = self.mariadb.query(sql=sql_query)
            if flag_query and len(results) > 0 and results not in results_list:
                for result in results:
                    result = list(result)
                    if result[9] != row[9] and result[8][:1] == row[8][:1] and int(result[9][-2]) % 2 and parse.get_age(result[9]) - parse.get_age(row[9]) >= 22:
                        flag_father_tegether = True
                        break
                    if result[9] != row[9] and result[9][:6] == row[9][:6] and parse.get_age(result[9]) - parse.get_age(row[9]) >= 22:
                        flag_father_tegether = True
                        break
                if not flag_father_tegether:
                    if len(rows_companion) != 0:
                        rows_companion.append(("",) * 18)
                    rows_companion = rows_companion + results
                results_list.append(results)
        print(
            f"{self.dt.get_now()} | 筛选完毕, 共有 同行人数据 {len(rows_companion)} 条:"
        )
        return rows_companion

    def filter_minor(self, rows: list, age1: int = 6, age2: int = 18) -> list:
        """筛选age1 <= 年龄 <= age2 的数据
        :param rows: 需要筛选age1 <= 年龄 <= age2的数据
        :type rows: list
        :param age1: 年龄限制, defaults to 6
        :type age1: int
        :param age2: 年龄限制, defaults to 18
        :type age2: int
        :return: 筛选age1 <= 年龄 <= age2的数据
        :rtype: list
        """
        parse = Parse()
        rows_minor: list = []
        for row in tqdm(rows, desc=f"筛选{age1} <= 年龄 <= {age2}数据", unit=f"条"):
            row: list = list(row)
            if age1 <= parse.get_age(row[9]) and parse.get_age(row[9]) <= age2:
                rows_minor.append(row)
        print(
            f"{self.dt.get_now()} | 筛选完毕, 共有 {age1} <= 年龄 <= {age2} 数据 {len(rows_minor)} 条:"
        )
        return rows_minor
