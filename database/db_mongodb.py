from pymongo import collection, database, MongoClient
from typing import Any, Dict, List, Mapping, Tuple
from utils.utils_datetime import DateTime as DT
from utils.utils_socket import Socket as SK


class MongoDB:
    def __init__(
        self,
        host: str = f"10.3.32.239",
        database: str = f"情报",
        table: str = f"基础信息_人员信息表",
        print: bool = True,
    ) -> None:
        """初始化

        :param host: 数据库地址, defaults to f"10.3.32.239"
        :type host: str, optional
        :param database: 数据库名称, defaults to f"情报"
        :type database: str, optional
        :param table: 集合名称, defaults to f"基础信息_人员信息表"
        :type table: str, optional
        """
        sk: SK = SK()
        ips: list[str] = sk.get_local_ips()
        self.host: str = "127.0.0.1" if host in ips else host
        self.port: int = 27017
        self.database_name: str = database
        self.collection_name: str = table
        self.client: MongoClient = None
        self.database = None
        self.collection = None
        self.bulk_operations: list = []
        self.dt = DT()
        self.print: bool = print

    def connect(self) -> bool:
        """连接数据库

        :return: 连接数据库是否成功
        :rtype: bool
        """
        try:
            self.client: MongoClient = MongoClient(self.host, self.port)
            self.database: database = self.client[self.database_name]
            self.collection: collection = self.database[self.collection_name]
            (
                print(f"{self.dt.get_now()} | MongoDB数据库 {self.host} 已连接")
                if self.print
                else None
            )
            return True
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | MongoDB数据库 {self.host} 连接失败: {e}")
                if self.print
                else None
            )
            return False

    def insert(self, documents: List[Dict]) -> bool:
        """插入文档

        :param documents: 要批量插入的数据
        :type documents: list
        :return: 插入数据是否成功
        :rtype: bool
        """
        try:
            self.collection.insert_many(documents)
            (
                print(
                    f"{self.dt.get_now()} | MongoDB数据库插入成功, 影响文档数: {len(documents)}"
                )
                if self.print
                else None
            )
            return True
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | MongoDB数据库插入失败, 错误信息: {e}")
                if self.print
                else None
            )
            return False

    def find(self, query: Mapping[str, Any]) -> Tuple[bool, List[Any]]:
        """根据查询条件查找文档

        :param query: 查询条件
        :type query: Mapping[str, Any]
        :return: 查询结果的迭代器
        :rtype: Iterator[dict]
        """
        try:
            result: list = list(self.collection.find(query))
            (
                print(
                    f"{self.dt.get_now()} | MongoDB数据库查询成功, 返回结果 {len(result)} 条"
                )
                if self.print
                else None
            )
            return True, result
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | MongoDB数据库查询失败, 错误信息: {e}")
                if self.print
                else None
            )
            return False, []

    def update(self, query: Mapping[str, Any], update: Any) -> bool:
        """更新语句

        :param query: 要更新的条件
        :type query: Mapping[str, Any]
        :param update: 要更新的数据
        :type update: Any
        :return: 更新是否成功
        :rtype: bool
        """
        try:
            self.collection.update_one(query, update)
            (
                print(f"{self.dt.get_now()} | MongoDB数据库更新成功")
                if self.print
                else None
            )
            return True
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | MongoDB数据库更新失败, 错误信息: {e}")
                if self.print
                else None
            )
            return False

    def delete(self, query: Mapping[str, Any]) -> bool:
        """删除语句

        :param query: 要删除的条件
        :type query: Mapping[str, Any]
        :return: 删除是否成功
        :rtype: bool
        """
        try:
            self.collection.delete_many(query)
            (
                print(f"{self.dt.get_now()} | MongoDB数据库删除成功")
                if self.print
                else None
            )
            return True
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | MongoDB数据库删除失败, 错误信息: {e}")
                if self.print
                else None
            )
            return False

    def bulk(self, bulk_operations: List[Dict]) -> bool:
        """批量操作语句

        :param bulk_operations: 要批量操作的语句
        :type bulk_operations: list
        :return: 查询是否成功
        :rtype: bool
        """
        try:
            self.collection.bulk_write(bulk_operations)
            (
                print(f"{self.dt.get_now()} | MongoDB数据库批量操作成功")
                if self.print
                else None
            )
            return True
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | MongoDB数据库批量操作失败, 错误信息: {e}")
                if self.print
                else None
            )
            return False

    def close(self) -> bool:
        """关闭数据库连接

        :return: 关闭数据库连接是否成功
        :rtype: bool
        """
        try:
            if self.client:
                self.client.close()
            (
                print(f"{self.dt.get_now()} | 关闭 {self.host} 成功")
                if self.print
                else None
            )
            return True
        except Exception as e:
            (
                print(f"{self.dt.get_now()} | 关闭 {self.host} 失败, 错误信息: {e}")
                if self.print
                else None
            )
            return False
