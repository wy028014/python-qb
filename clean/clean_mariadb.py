from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from typing import Any, List, Tuple
from tqdm import tqdm


class CleanMariaDB:
    def __init__(self, print: bool = True) -> None:
        self.dt: DateTime = DateTime()
        self.print: bool = print
        pass

    def checkUpdateData(self) -> int or bool:
        mariadb: MariaDB = MariaDB(239, "情报", self.print)
        result: List[Any] = None
        if mariadb.connect():
            sql_max_id: str = f"""
            SELECT MAX(`id`)
            FROM `情报`.`ftp数据_数据表`;
            """
            flag_query_max_id, result_max_id = mariadb.query(sql_max_id)
            if flag_query_max_id:
                max_id: int = result_max_id[0][0] if isinstance(result_max_id, list) else 0
            sql_last_id: str = f"""
            SELECT `更新id`
            FROM `情报`.`乘车数据_更新表`
            ORDER BY `创建时间` DESC
            LIMIT 1;
            """
            flag_query_last_id, result_last_id = mariadb.query(sql_last_id)
            if flag_query_last_id:
                last_id: int = 0 if len(result_last_id) == 0 else result_last_id[0][0] if isinstance(result_last_id, list) else 0
        if max_id != last_id:
            print(f"{self.dt.get_now()} | 数据需要更新")
            return last_id if max_id > last_id else max_id
        else:
            print(f"{self.dt.get_now()} | 数据已是最新")
            return False

    def mysqlINTOmariadb(self) -> None:
        last_id = self.checkUpdateData()
        if isinstance(last_id, int):
            mariadb: MariaDB = MariaDB(239, "情报", self.print)
            mariadb.connect()
            sql_query: str = f"""
            SELECT DISTINCT
                `姓名`, `证件类型`, `证件编号`,
                `乘车日期`, `乘车时间`,  `车次`, `发站`, `到站`, `车厢号`, `座位号`, `席别`, 
                `票号`, `票种`, `售票处`, `窗口`, `操作员编号`, `售票时间`, `票价`
            FROM `情报`.`ftp数据_数据表`
            WHERE
                id > {last_id}
            AND
                `证件类型` <> 'null'
            ORDER BY id ASC
            LIMIT 100000;
            """
            flag_query, result = mariadb.query(sql_query)
            if flag_query:
                batch_size = 1000
                sql_insert: str = """
                INSERT IGNORE INTO `情报`.`乘车数据_数据表` (
                `姓名`, `证件类型`, `证件编号`,
                `乘车日期`, `乘车时间`,  `车次`, `发站`, `到站`, `车厢号`, `座位号`, `席别`, 
                `票号`, `票种`, `售票处`, `窗口`, `操作员编号`, `售票时间`, `票价`,  `出行状态`, `MongoDB`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '购票', 0)
                """
                total_batches = (len(result) + batch_size - 1) // batch_size
                for i in tqdm(range(0, len(result), batch_size), total=total_batches, desc=f"插入数据 {len(result)} 条", unit=f"千条"):
                    batch_data = result[i: i+batch_size]
                    flag_insert = mariadb.insert(sql=sql_insert, data=batch_data)
                    if not flag_insert:
                        continue
                sql_query = f"""
                SELECT id
                FROM `情报`.`ftp数据_数据表`
                WHERE
                    `姓名` = ({result[-1][0]})
                AND `证件类型` = ({result[-1][1]})
                AND `证件编号` = ({result[-1][2]})
                AND `乘车日期` = ({result[-1][3]})
                AND `乘车时间` = ({result[-1][4]})
                AND  `车次` = ({result[-1][5]})
                AND `发站` = ({result[-1][6]})
                AND `到站` = ({result[-1][7]})
                AND `车厢号` = ({result[-1][8]})
                AND `座位号` = ({result[-1][9]})
                AND `席别` = ({result[-1][10]})
                AND `票号` = ({result[-1][11]})
                AND `票种` = ({result[-1][12]})
                AND `售票处` = ({result[-1][13]})
                AND `窗口` = ({result[-1][14]})
                AND `操作员编号` = ({result[-1][15]})
                AND `售票时间` = ({result[-1][16]})
                AND `票价` = ({result[-1][17]})                
                ORDER BY id ASC
                LIMIT 100000;
                """
                flag_query, new_id = mariadb.query(sql=sql_query)
                if flag_query:
                    sql_query = f"""
                    INSERT INTO `情报`.`乘车数据_更新表` (`更新id`, `创建时间`, `更新时间`, `删除时间`)
                    VALUES ({result[-1][0]}, '{self.dt.get_now()}', '{self.dt.get_now()}', NULL)
                    """
                    mariadb.update(sql_query)
