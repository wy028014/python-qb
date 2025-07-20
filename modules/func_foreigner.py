from database.db_mariadb import MariaDB
from utils.utils_filter import Filter
from utils.utils_xlsx import Xlsx
from utils.utils_datetime import DateTime


def func_foreigner(print_key: bool = True) -> None:
    """外籍旅客和同行人信息"""
    dt: DateTime = DateTime()
    mariadb: MariaDB = MariaDB(host=239, database=f"情报", print=print_key)
    flag_connect: bool = mariadb.connect()
    if flag_connect:
        filter: Filter = Filter()
        rows_foreigner: list = []
        xlsx: Xlsx = Xlsx()
        today: str = dt.get_today()
        yesterday: str = dt.get_yesterday(date=today)
        sql_query: str = f"""
        SELECT
        `车次`, DATE_FORMAT(`乘车日期`, '%Y-%m-%d'), DATE_FORMAT(`乘车时间`, '%H:%i:%s'), `发站`, `到站`, `车厢号`, `座位号`, `席别`,
        `姓名`, `证件编号`, `证件类型`,
        `售票处`, `窗口`, `操作员编号`, DATE_FORMAT(`售票时间`, '%Y-%m-%d %H:%i:%s'), `票号`, `票价`, `票种`
        FROM `情报`.`ftp_data`
        WHERE `证件类型` IN ('内港', '港内', '台内', '外入', '外国人护照', '外留', '护照', '领馆')
        AND (
            `车次` IN (
                SELECT `车次` FROM `情报`.`基础信息_车次表` WHERE `部门id` = 215
            )
        OR `发站` IN (
                SELECT `车站名称` FROM `情报`.`基础信息_车站表` WHERE `车站类型` LIKE '%客运%'
            )
        )
        AND (
            `乘车日期` = '{today}'
            OR (
                `乘车日期` = '{yesterday}'
                AND `售票时间` >= '{yesterday} 08:00:00'
            )
        )
        """
        flag_query, results = mariadb.query(sql=sql_query)
        if flag_query:
            rows_foreigner: list = (
                rows_foreigner + filter.filter_companion(rows=results)
                if len(results) > 0
                else rows_foreigner
            )
            rows_leng: int = 0
            for row in rows_foreigner:
                if row[0] != "":
                    rows_leng += 1
            xlsx.write_xlsx(
                f"C:\\Personal\\Outputs\\情报\\外籍\\外籍_{dt.get_now(format=f'%Y%m%d')}_{rows_leng}",
                rows=rows_foreigner,
            )
        else:
            pass
        mariadb.close()
    else:
        pass
