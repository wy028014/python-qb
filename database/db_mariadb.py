from typing import Any, List, Tuple
from mysql import connector
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.pooling import PooledMySQLConnection
from utils.utils_datetime import DateTime as DT
from utils.utils_socket import Socket as SK


class MariaDB:
    def __init__(self, host: int, database: str, print: bool = True) -> None:
        """初始化

        :param host: 数据库代码
        :type host: int
        :param database: 数据库名称
        :type database: str
        """
        sk: SK = SK()
        ips: list[str] = sk.get_local_ips()
        target_ip: str = f"10.3.32.{host}" if 238 <= host <= 239 else "127.0.0.1"
        self.host: str = '127.0.0.1' if target_ip in ips else target_ip
        self.port: int = 3306 if host != 3307 else 3307
        self.username: str = "wangye" if 238 <= host <= 239 else "root"
        self.password: str = "Wy028014." if 238 <= host <= 239 else "root"
        self.database: str = database
        self.connection = None
        self.cursor = None
        self.dt = DT()
        self.print: bool = print

    def connect(self) -> bool:
        """连接数据库

        :return: 连接数据库是否成功
        :rtype: bool
        """
        try:
            self.connection: PooledMySQLConnection | MySQLConnectionAbstract = connector.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                charset="utf8mb4",  # 指定字符集为utf8mb4
                collation="utf8mb4_general_ci",  # 指定校对规则为utf8mb4_general_ci
            )
            self.cursor: MySQLCursorAbstract | Any = self.connection.cursor()
            print(
                f"{self.dt.get_now()} | 连接 {self.host}:{self.port} 成功") if self.print else None
            return True
        except Exception as e:
            print(
                f"{self.dt.get_now()} | 连接 {self.host}:{self.port} 失败，错误信息: {e}"
            ) if self.print else None
            return False

    def insert(self, sql: str, data: list) -> bool:
        """插入数据

        :param sql: 要执行的 sql 语句
        :type sql: str
        :param data: 要批量插入的数据
        :type data: list
        :return: 插入数据是否成功
        :rtype: bool
        """
        try:
            self.cursor.executemany(sql, data)
            self.connection.commit()
            print(
                f"{self.dt.get_now()} | 插入成功, 影响行数: {self.cursor.rowcount}") if self.print else None
            return True
        except Exception as e:
            self.connection.rollback()
            print(f"{self.dt.get_now()} | 插入失败, 错误信息: {e}") if self.print else None
            return False

    def query(self, sql: str) -> Tuple[bool, List[Any]]:
        """查询语句

        :param sql: 要执行的 sql 语句
        :type sql: str
        :return: 查询是否成功, 返回结果
        :rtype: Tuple[bool, List[Any]]
        """
        try:
            self.cursor.execute(sql)
            result: list = self.cursor.fetchall()
            print(
                f"{self.dt.get_now()} | 查询成功, 返回结果 {len(result)} 条") if self.print else None
            return True, result
        except Exception as e:
            print(f"{self.dt.get_now()} | 查询失败, 错误信息: {e}") if self.print else None
            return False, []

    def update(self, sql: str) -> bool:
        """更新语句

        :param sql: 要执行的 sql 语句
        :type sql: str
        :return: 更新是否成功
        :rtype: bool
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            print(f"{self.dt.get_now()} | 更新成功") if self.print else None
            return True
        except Exception as e:
            print(f"{self.dt.get_now()} | 更新失败, 错误信息: {e}") if self.print else None
            return False

    def close(self) -> bool:
        """关闭数据库连接

        :return: 关闭数据库连接是否成功
        :rtype: bool
        """
        try:
            if self.connection:
                self.connection.close()
            if self.cursor:
                self.cursor.close()
            print(f"{self.dt.get_now()} | 关闭 {self.host} 成功") if self.print else None
            return True
        except Exception as e:
            print(
                f"{self.dt.get_now()} | 关闭 {self.host} 失败, 错误信息: {e}") if self.print else None
            return False
