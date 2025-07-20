from clean.clean_ftp import CleanFTP
from clean.clean_mariadb import CleanMariaDB
from clean.clean_mongodb import CleanMongoDB
from ftp.ftp_htwa import Ftp
from modules.func_foreigner import func_foreigner
from modules.func_minor import fuc_minor
from modules.func_impersonate import fuc_impersonate
from utils.utils_datetime import DateTime
from upgrade.upgrade_from_waca import UpgradeFromWaca
from upgrade.upgrade_from_excel import UpgradeFromExcel
from use.gjcxpl import GJCXPL
from use.zzcxpl import ZZCXPL
from use.txrpl import TXRPL
from os import path,walk

if __name__ == "__main__":
    dt: DateTime = DateTime()

    while True:
        print(
            f"""
{dt.get_now()}
输入要执行功能的编号:
0. 退出程序
1.  MySQL 数据库清理入 MariaDB 乘车数据库
2.  MariaDB 数据库清理入 MongoDB 乘车数据库
3.  数字证书 清理入 MariaDB 乘车数据库
4.  Excel 清理入 MariaDB 网逃数据库
5.  外籍旅客 筛查
6.  未成年人 筛查
7.  冒用证件 筛查
11. 轨迹查询 批量
12. 站站查询 批量
13. 同行人查询 批量
"""
        )
        user_input: str = input()
        if user_input.isdigit():
            num: int = int(user_input)
            if num == 0:
                break
            # elif num == 1:
            #     cleanMariaDB: CleanMariaDB = CleanMariaDB(print=False)
            #     cleanMariaDB.mysqlINTOmariadb()
            # elif num == 2:
            #     cleanMongoDB: CleanMongoDB = CleanMongoDB(print=True)
            #     cleanMongoDB.mariadbINTOmongodb()
            # elif num == 3:
            #     up = UpgradeFromWaca()
            #     up.useZZ(
            #         date_start="2024-11-22", date_end="2024-11-22", train_code="K546"
            #     )
            elif num == 1:
                # ftp: Ftp = Ftp()
                # flag_ftp: bool = ftp.connect()
                # print(f"flag_ftp:", flag_ftp)
                cleanFtp: CleanFTP = CleanFTP(print=False)
                cleanFtp.ftpINTOmariadb()
            elif num == 2:
                cleanMariaDB: CleanMariaDB = CleanMariaDB(print=False)
                cleanMariaDB.mysqlINTOmariadb()
            elif num == 3:
                cleanMongoDB: CleanMongoDB = CleanMongoDB(print=True)
                cleanMongoDB.mariadbINTOmongodb()
            elif num == 4:
                up = UpgradeFromExcel()
                up.clean()
            elif num == 5:
                func_foreigner(print_key=True)
            elif num == 6:
                fuc_minor(print_key=False)
            elif num == 7:
                fuc_impersonate(print_key=True)
            elif num == 11:
                gjcxpl = GJCXPL()
                gjcxpl.run()
            elif num == 12:
                zzcxpl = ZZCXPL()
                zzcxpl.run()
            elif num == 13:
                txrpl = TXRPL()
                txrpl.run()
            elif num == 15:
                result = ('K7205', '2024-11-25', '20:52:00', '哈尔滨东', '绥化', '12', '0001', '硬座', '赵琪', '231202200904300166', '二代身份证', None, None, None, '2024-11-16 13:10:38', '9834626', '86.00', '成人票')
                row = ('K7205', '2024-11-25', '20:52:00', '哈尔滨东', '绥化', '12', '0001', '硬座', '赵琪', '231202200904300166', '二代身份证', None, None, None, '2024-11-16 13:10:38', '9834626', '86.00', '成人票')
                print(result[9], row[9], result[9] != row[9])
                print(result[8][:1], row[8][:1], result[8][:1] == row[8][:1])
                print(int(result[9][-2]) % 2)                
                print(parse.get_age(result[9]), parse.get_age(row[9]), parse.get_age(result[9]) - parse.get_age(row[9]) >= 22)
            else:
                print(f"输入超纲, 请输入正确范围的编号")
        else:
            print(f"输入有误, 请输入正确的编号")
