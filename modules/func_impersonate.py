from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from re import sub

def fuc_impersonate(print_key: bool = True) -> None:
    """冒用他人信息"""

    def normalize_station_name(station_name: str) -> str:
        """去掉车站名称中的空格和东南西北"""
        station_name = station_name.strip()
        station_name = sub(r"[东南西北]", "", station_name)
        return station_name

    mariadb: MariaDB = MariaDB(host=239, database=f"情报", print=print_key)
    dt: DateTime = DateTime()
    flag_connect: bool = mariadb.connect()
    if flag_connect:
        # 获取所有不重复的证件编号，按 id 增序排列
        sql_distinct_id_no: str = """
        SELECT DISTINCT `证件编号`
        FROM `情报`.`乘车数据_数据表_数字证书`
        ORDER BY `id`
        """
        flag_id_no, results_id_no = mariadb.query(sql=sql_distinct_id_no)
        if flag_id_no:
            # 构造所有证件编号的列表
            id_no_list = [row[0] for row in results_id_no]
            # 获取上次查询的身份证号码和日期
            sql_check_last: str = """
            SELECT `证件编号`, `更新时间`
            FROM `业务数据_冒用表`
            ORDER BY `更新时间` DESC
            LIMIT 1
            """
            flag_last, result_last = mariadb.query(sql=sql_check_last)
            last_id_no = result_last[0][0] if flag_last and result_last else None
            start_index = id_no_list.index(last_id_no) + 1 if last_id_no in id_no_list else 0
            for id_no in id_no_list[start_index:]:
                # 查询乘车轨迹数据
                sql_query_gj: str = f"""
                SELECT
                DATE_FORMAT(`乘车日期`, '%Y-%m-%d') AS `乘车日期`,
                DATE_FORMAT(`乘车时间`, '%H:%i:%s') AS `乘车时间`,
                `发站`, `到站`, `车次`
                FROM `乘车数据_数据表_数字证书`
                WHERE `证件编号` = '{id_no}'
                AND `乘车日期` >= '2024-10-01'
                ORDER BY `乘车日期`, `乘车时间`
                """
                flag_query_gj, result_gj = mariadb.query(sql=sql_query_gj)
                if flag_query_gj and result_gj:
                    count = 0
                    for i in range(1, len(result_gj)):
                        if normalize_station_name(result_gj[i - 1][3]) != normalize_station_name(result_gj[i][2]) and result_gj[i][4].startswith("G"):  # 到站和发站不一致
                            count += 1
                    # 检查业务数据_冒用表中是否已经存在该证件编号
                    sql_check: str = f"""
                    SELECT COUNT(*)
                    FROM `业务数据_冒用表`
                    WHERE `证件编号` = '{id_no}'
                    """
                    flag_check, result_check = mariadb.query(sql=sql_check)
                    if flag_check and result_check and result_check[0][0] > 0:
                        # 如果存在，则执行更新操作
                        sql_update: str = f"""
                        UPDATE `业务数据_冒用表`
                        SET 
                            `近三个月乘车轨迹空间不闭合次数` = {count},
                            `近三个月乘车轨迹次数` = {len(result_gj)},
                            `更新时间` = '{dt.get_now()}'
                        WHERE `证件编号` = '{id_no}'
                        """
                        flag_update = mariadb.update(sql_update)
                        if flag_update:
                            print(f"{dt.get_now()} | 身份证 {id_no} 更新成功: 总次数 {len(result_gj)}, 不连贯次数 {count}")
                        else:
                            print(f"{dt.get_now()} | 身份证 {id_no} 更新失败")
                    else:
                        # 如果不存在，则执行插入操作
                        sql_insert: str = f"""
                        INSERT INTO `业务数据_冒用表`
                        (`证件编号`, `近三个月乘车轨迹空间不闭合次数`, `近三个月乘车轨迹次数`, `创建时间`, `更新时间`)
                        VALUES
                        ('{id_no}', {count}, {len(result_gj)}, '{dt.get_now()}', '{dt.get_now()}')
                        """
                        flag_insert = mariadb.update(sql_insert)
                        if flag_insert:
                            print(f"{dt.get_now()} | 身份证 {id_no} 插入成功: 总次数 {len(result_gj)}, 不连贯次数 {count}")
                        else:
                            print(f"{dt.get_now()} | 身份证 {id_no} 插入失败")
                else:
                    print(f"{dt.get_now()} | 身份证 {id_no} 没有找到对应的乘车数据")
            mariadb.close()