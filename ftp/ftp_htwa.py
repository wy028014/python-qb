from ftplib import FTP, error_perm
from utils.utils_datetime import DateTime as DT
from os import path

class Ftp:
    def __init__(self, print: bool = True) -> None:
        """初始化

        :param print: 是否打印
        :type print: bool
        """
        self.host: str = '10.3.16.197'
        self.user: str = 'htwa'
        self.passwd: str = 'htwa@123'
        self.path: str = 'haerbin'
        self.dt = DT()
        self.print: bool = print

    def connect(self) -> bool:
        """连接ftp

        :return: 连接ftp是否成功
        :rtype: bool
        """
        try:
            self.ftp = FTP(host=self.host, timeout=30)
            self.ftp.login(user=self.user, passwd=self.passwd)
            self.ftp.cwd(self.path)
            print(
                f"{self.dt.get_now()} | ftp 连接成功") if self.print else None
            return True
        except Exception as e:
            print(
                f"{self.dt.get_now()} | ftp 连接失败，错误信息: {e}"
            ) if self.print else None
            return False

    def get_file_list(self) -> list or bool:
        if self.connect():
            return [f for f in self.ftp.nlst() if f.endswith('.txt')]
        else:
            return False
            

    def download(self, file_name) -> bool:
        try:
            local_path = path.join(f"C:\Personal\Projects\FTP数据", file_name)
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f"RETR {file_name}", f.write)
            print(f"{self.dt.get_now()} | 下载成功: {file_name}")
            return True
        except error_perm as e:
            print(f"{self.dt.get_now()} | 下载失败（权限问题）: {file_name} - {e}")
        except IOError as e:
            print(f"{self.dt.get_now()} | 本地写入失败: {file_name} - {e}")
        return False

