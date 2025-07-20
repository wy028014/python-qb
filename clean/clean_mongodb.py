from database.db_mongodb import MongoDB
from database.db_mariadb import MariaDB
from utils.utils_datetime import DateTime
from utils.utils_parse import Parse


class CleanMongoDB:
    def __init__(self, print: bool = True) -> None:
        self.dt: DateTime = DateTime()
        self.mariadb: MariaDB = MariaDB(host=239, database="情报", print=print)
        self.mongodb: MariaDB = MongoDB(print=print)
        self.parse: Parse = Parse()
        self.mariadb.connect()
        self.mongodb.connect()
        pass

    def mariadbINTOmongodb(self) -> None:
        sql_query: str = f"""
        SELECT DISTINCT `姓名`, `证件编号`, `证件类型`
        FROM `情报`.`乘车数据_数据表`
        WHERE `MongoDB` = 0;
        """
        flag_query, results = self.mariadb.query(sql_query)
        if flag_query:
            for row in results:
                sql_query: str = f"""
                SELECT id
                FROM `情报`.`乘车数据_数据表`
                WHERE
                `姓名` = '{row[0]}' AND
                `证件编号` = '{row[1]}' AND
                `证件类型` = '{row[2]}' AND
                `MongoDB` = 0 AND
                `删除时间` IS NULL;
                """
                flag_query, ids = self.mariadb.query(sql_query)
                if flag_query:
                    gender: str = (
                        self.parse.get_gender(row[1]) if row[2] == "二代身份证" else ""
                    )
                    # 构建要插入或更新的数据信息
                    document_insert_or_update: dict = {
                        "性别": gender,
                        "证件": [
                            {"姓名": row[0], "证件类型": row[2], "证件编号": row[1]}
                        ],
                        "乘车轨迹": list(ids),
                    }
                    # 检查MongoDB中是否已存在相同的文档
                    flag_exist, document_exist_list = self.mongodb.find(
                        {
                            "证件": {
                                "$elemMatch": {
                                    "姓名": row[0],
                                    "证件类型": row[2],
                                    "证件编号": row[1],
                                }
                            }
                        }
                    )
                    flag_mongodb: bool = False
                    if flag_exist:
                        if len(document_exist_list) > 0:
                            new_ids: list[int] = [
                                id[0]
                                for id in ids
                                if id[0] not in document_exist_list[0]["乘车轨迹"]
                            ]
                            # 如果存在, 且乘车轨迹中存在新的id, 则更新乘车轨迹
                            if len(new_ids) > 0:
                                flag_mongodb = self.mongodb.update(
                                    {"_id": document_exist_list[0]["_id"]},
                                    {"$push": {"乘车轨迹": {"$each": new_ids}}},
                                )
                        else:
                            # 如果不存在, 则插入新的文档
                            flag_mongodb = self.mongodb.insert(
                                [document_insert_or_update]
                            )
                    if flag_mongodb:
                        # 更新数据库中的MongoDB字段
                        sql_update: str = f"""
                        UPDATE `情报`.`乘车数据_数据表`
                        SET `MongoDB` = 1
                        WHERE `id` IN ({','.join(map(str, [item[0] for item in ids]))});
                        """
                        self.mariadb.update(sql_update)

    def close(self) -> None:
        self.mariadb.close()
        self.mongodb.close()
