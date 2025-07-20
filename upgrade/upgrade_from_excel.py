from os import listdir, path, rename
from tqdm import tqdm
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from utils.utils_xlsx import Xlsx


class UpgradeFromExcel:
    def __init__(self) -> None:
        self.db: MariaDB = MariaDB(host=239, database="情报", print=False)
        self.db.connect()
        self.dt: DateTime = DateTime()
        self.xlsx: Xlsx = Xlsx()

    def clean(self):
        for i in range(1, 3):
            if i == 1:
                self.dir_path: str = f".\网逃导出数据"
            elif i == 2:
                self.dir_path: str = f".\实名制导出数据"
            for filename in tqdm(listdir(self.dir_path), desc=f"读取{self.dir_path}下的xlsx文件", unit=f"个"):
                if filename.endswith('.xlsx') or filename.endswith('.xls'):
                    if '已导入' in filename:
                        continue
                    filepath = path.join(self.dir_path, filename)
                    data = self.xlsx.read_xlsx(
                        filename=filepath, sheet_index=0, readHead=False)
                    if '在逃导出数据' in filename:
                        for row in data:
                            sql_query: str = f"""
                            SELECT `id`
                            FROM `情报`.`网逃数据_在逃表`
                            WHERE `人员编号` = '{row['人员编号']}'
                            """
                            flag_query, res_query = self.db.query(sql=sql_query)
                            if flag_query:
                                if len(res_query) > 0:
                                    pass
                                else:
                                    sql_update: str = f"""
                                    INSERT INTO `情报`.`网逃数据_在逃表`
                                    (`人员编号`, `姓名`, `性别`, `证件编号`, `户籍地区划`, `户籍地详址`, `案件类别`, `案件分类`, `立案单位`, `简要案情`, `案件数量`, `主办单位分类`, `主办单位（区划）`, `逃跑方向`, `主办单位详称`, `入部登记库日期`)
                                    VALUES ('{row['人员编号']}','{row['姓名']}','{row['性别']}','{row['证件编号']}','{row['户籍地区划']}','{row['户籍地详址']}','{row['案件类别']}','{row['案件分类']}','{row['立案单位']}','{row['简要案情'].replace("'", '"')}','{row['案件数量']}','{row['主办单位分类']}','{row['主办单位（区划）']}','{row['逃跑方向']}','{row['主办单位详称']}','{row['入部登记库日期'].replace('年', '-').replace('月', '-').replace('日', '-')}');
                                    """
                                    flag_update = self.db.update(sql=sql_update)
                                    if not flag_update:
                                        return
                    elif '站站查询' in filename or '全列查询' in filename:
                        for row in data:
                            sql_query: str = f"""
                            SELECT *
                            FROM `情报`.`ca_data`
                            WHERE `乘车日期` = '{row['乘车日期']}'
                            AND `车次` = '{row['车次']}'
                            AND `发站` = '{row['发站']}'
                            AND `到站` = '{row['到站']}'
                            AND `证件编号` = '{row['证件编号']}'
                            AND `车厢号` = '{row['车厢号']}'
                            AND `座位号` = '{row['座位号']}'
                            AND `售票处` IS NULL
                            """
                            flag_query, res_query = self.db.query(sql=sql_query)
                            if flag_query:
                                if len(res_query) > 0:
                                    sql_update: str = f"""
                                    UPDATE `ca_data`
                                    SET
                                    `票价` = '{row['票价']}',
                                    `售票处` = '{row['售票处']}',
                                    `窗口` = '{row['窗口']}',
                                    `操作员编号` = '{row['操作员']}',
                                    `售票时间` = '{row['售票时间'].replace('/', '-').replace('.000', '')}',
                                    `更新时间` = '{self.dt.get_now(format="%Y-%m-%d %H:%M:%S")}'
                                    WHERE `乘车日期` = '{row['乘车日期']}'
                                    AND `车次` = '{row['车次']}'
                                    AND `发站` = '{row['发站']}'
                                    AND `到站` = '{row['到站']}'
                                    AND `证件编号` = '{row['证件编号']}'
                                    AND `车厢号` = '{row['车厢号']}'
                                    AND `座位号` = '{row['座位号']}';
                                    """
                                    flag_update = self.db.update(sql=sql_update)
                                    if not flag_update:
                                        return
                                else:
                                    sql_update: str = f"""
                                    INSERT INTO `ca_data`
                                    (`车次`,`乘车日期`,`乘车时间`,`发站`,`到站`,`车厢号`,`座位号`,`席别`,`姓名`,`证件编号`,`证件类型`,`售票处`,`窗口`,`操作员编号`,`售票时间`,`票号`,`票价`,`票种`)
                                    VALUES ('{row['车次']}','{row['乘车日期']}','{row['乘车时间']}','{row['发站']}','{row['到站']}','{row['车厢号']}','{row['座位号']}','{row['席别']}','{row['姓名']}','{row['证件编号']}','{row['证件类型']}','{row['售票处']}','{row['窗口']}','{row['操作员']}','{row['售票时间'].replace('/', '-').replace('.000', '')}','{row['票号']}','{row['票价']}','{row['票种']}')
                                    ON DUPLICATE KEY UPDATE `更新时间` = CURRENT_TIMESTAMP();;
                                    """
                                    flag_update = self.db.update(sql=sql_update)
                                    if not flag_update:
                                        return
                    # rename(filepath, filepath.replace('.xls', '_已导入.xls'))
    def close(self) -> None:
        self.db.close()
