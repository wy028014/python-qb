from database.db_mariadb import MariaDB
from utils.utils_filter import Filter
from utils.utils_xlsx import Xlsx
from utils.utils_datetime import DateTime


def fuc_minor(print_key: bool = True) -> None:
    """未成年女性和同行人信息"""
    age1: int = 12
    age2: int = 14
    dt: DateTime = DateTime()
    mariadb: MariaDB = MariaDB(host=239, database=f"情报", print=print_key)
    flag_connect: bool = mariadb.connect()
    if flag_connect:
        filter: Filter = Filter()
        rows_minor: list = []
        xlsx: Xlsx = Xlsx()
        sql_query: str = f"""
        SELECT DISTINCT
        `车次`,
        `乘车日期`,
        `乘车时间`,
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
        `售票时间`,
        `票号`,
        `票价`,
        `票种` 
        FROM
        `情报`.`ftp_data` 
        WHERE
        `乘车日期` = CURRENT_DATE 
        AND `证件类型` = '二代身份证' 
        AND MOD ( MID( `证件编号`, 17, 1 ), 2 ) = 0 
        AND (
            `车次` IN ( SELECT `车次` FROM `情报`.`基础信息_车次表` WHERE `部门id` = 215 ) 
            OR `发站` IN ( SELECT `车站名称` FROM `情报`.`基础信息_车站表` WHERE `车站类型` LIKE '%客运%' ) 
        ) UNION
        SELECT DISTINCT
        `车次`,
        `乘车日期`,
        `乘车时间`,
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
        `售票时间`,
        `票号`,
        `票价`,
        `票种` 
        FROM
        `情报`.`ca_data` 
        WHERE
        `乘车日期` = CURRENT_DATE 
        AND `证件类型` = '二代身份证' 
        AND MOD ( MID( `证件编号`, 17, 1 ), 2 ) = 0 
        AND (
            `车次` IN ( SELECT `车次` FROM `情报`.`基础信息_车次表` WHERE `部门id` = 215 ) 
        OR `发站` IN ( SELECT `车站名称` FROM `情报`.`基础信息_车站表` WHERE `车站类型` LIKE '%客运%' ) 
        )
        """
        flag_query, results = mariadb.query(sql=sql_query)
        if flag_query:
            rows_minor: list = (
                rows_minor + filter.filter_minor(rows=results, age1=age1, age2=age2)
                if len(results) > 0
                else rows_minor
            )
            rows_minor: list = filter.filter_companion_minor(rows=rows_minor)
            rows_leng: int = 0
            for row in rows_minor:
                if row[0] != "":
                    rows_leng += 1
            xlsx.write_xlsx(
                f"C:\\Personal\\Outputs\\情报\\未成年\\{age1}至{age2}周岁女性_{dt.get_now(f'%Y%m%d')}_{rows_leng}",
                rows=rows_minor,
            )
        else:
            pass
        mariadb.close()
    else:
        pass
